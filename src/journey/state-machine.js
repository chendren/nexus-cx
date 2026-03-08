/**
 * @module nexus-cx/journey/state-machine
 * @description Finite state machine engine for tracking customer journeys across channels.
 *
 * Each customer can have multiple concurrent active journeys (e.g., a billing dispute
 * and a service upgrade running simultaneously). Journey types are defined in
 * {@link module:nexus-cx/journey/definitions} with explicit state graphs and transition rules.
 *
 * Key capabilities:
 *   - **State transitions**: triggered by classified interaction events, validated against
 *     the journey definition's allowed transitions from the current state
 *   - **Cross-channel continuity**: journeys persist across voice, chat, web, email, SMS, mobile
 *   - **Journey intelligence**: stuck-state detection, common path analysis, cross-channel
 *     handoff tracking, and top transition trigger identification
 *   - **Bounded history**: per-journey history capped at 50 entries; global history capped at
 *     10K in memory (older entries persist in SQLite)
 *
 * Supports 13 journey types across commercial CX and US Treasury domains.
 *
 * Persistence: uses {@link module:nexus-cx/store/persisted-store|PersistedStore} for journey
 * records and write-through to `journey_transitions` table for audit trail.
 *
 * @requires uuid
 * @requires ../store/memory-store
 * @requires ../store/persisted-store
 * @requires ./definitions
 * @requires ../config
 *
 * @see {@link module:nexus-cx/journey/definitions} Journey type definitions
 * @see {@link module:nexus-cx/pipeline} Pipeline orchestrator (consumer)
 */
const { v4: uuidv4 } = require('uuid');
const { MemoryStore } = require('../store/memory-store');
const { PersistedStore } = require('../store/persisted-store');
const { getJourneyDefinition, findMatchingJourneys } = require('./definitions');
const config = require('../config');

/**
 * Journey state machine engine. Manages the lifecycle of customer journeys
 * from creation through state transitions to completion, with persistence,
 * SLA tracking, and cross-channel handoff detection.
 */
class JourneyEngine {
  constructor() {
    // MemoryStore schema mirrors the DynamoDB table design for journeys
    const memConfig = {
      partitionKey: 'customer_id',
      sortKey: 'journey_key',
      gsis: [
        { name: 'by-type-state', partitionKey: 'journey_type', sortKey: 'current_state' },
        { name: 'by-status', partitionKey: 'status', sortKey: 'updated_at' }
      ]
    };

    this.store = config.PERSIST
      ? new PersistedStore('journeys', memConfig, {
          upsertSQL: `INSERT OR REPLACE INTO journeys
                       (customer_id, journey_key, journey_id, journey_type, status, data, created_at, updated_at)
                       VALUES (@customer_id, @journey_key, @journey_id, @journey_type, @status, @data, @created_at, @updated_at)`,
          deleteSQL: null,
          selectAllSQL: 'SELECT data FROM journeys',
          serialize: (item) => ({
            customer_id: item.customer_id,
            journey_key: item.journey_key,
            journey_id: item.journey_id,
            journey_type: item.journey_type,
            status: item.status,
            data: JSON.stringify(item),
            created_at: item.started_at || new Date().toISOString(),
            updated_at: item.updated_at || new Date().toISOString()
          }),
          deserialize: (row) => JSON.parse(row.data)
        })
      : new MemoryStore('journeys', memConfig);

    /** @type {Array<Object>} In-memory transition history (bounded by JOURNEY_HISTORY_MAX) */
    this.history = [];
    /** @type {import('better-sqlite3').Statement|null} Lazily prepared transition INSERT statement */
    this._transitionStmt = null;
  }

  /**
   * Hydrate journey state and transition history from SQLite. Called once at startup.
   * Transitions are loaded newest-first from the DB, then reversed to chronological order.
   */
  hydrate() {
    if (this.store.hydrate) this.store.hydrate();

    // Load recent transitions from SQLite
    if (config.PERSIST) {
      try {
        const { getDatabase } = require('../store/database');
        const db = getDatabase();
        const rows = db.prepare(
          'SELECT data FROM journey_transitions ORDER BY id DESC LIMIT ?'
        ).all(config.JOURNEY_HISTORY_MAX);

        for (let i = rows.length - 1; i >= 0; i--) {
          try {
            this.history.push(JSON.parse(rows[i].data));
          } catch {}
        }
      } catch {}
    }
  }

  /**
   * Persist a transition to SQLite. Best-effort: failures are silently ignored.
   * @param {Object} transition - Transition record
   * @private
   */
  _persistTransition(transition) {
    if (!config.PERSIST) return;
    try {
      if (!this._transitionStmt) {
        const { getDatabase } = require('../store/database');
        this._transitionStmt = getDatabase().prepare(
          `INSERT INTO journey_transitions
           (journey_id, customer_id, journey_type, from_state, to_state, trigger_intent, channel, data, timestamp)
           VALUES (@journey_id, @customer_id, @journey_type, @from_state, @to_state, @trigger_intent, @channel, @data, @timestamp)`
        );
      }
      this._transitionStmt.run({
        journey_id: transition.journey_id,
        customer_id: transition.customer_id,
        journey_type: transition.journey_type,
        from_state: transition.from_state || '',
        to_state: transition.to_state,
        trigger_intent: transition.trigger,
        channel: transition.channel,
        data: JSON.stringify(transition),
        timestamp: transition.timestamp
      });
    } catch {}
  }

  /**
   * Process a classified event through the journey engine. Two-phase, multi-intent aware:
   *
   * Phase 1: Try transitions on existing active journeys with all detected intents.
   * Phase 2: For intents that didn't trigger transitions, create new journeys if applicable.
   *
   * @param {Object} event - Classified event with event.classification populated
   * @returns {{journeys: Array, transitions: Array, intelligence: Object}} Processing result
   */
  processEvent(event) {
    if (!event.classification) return { journeys: [], transitions: [], intelligence: this.getJourneyIntelligence() };

    const customerId = event.identity?.unified_customer_id || event.customer_id;
    const channel = event.channel;
    const transitions = [];

    // Collect all intent keys to process — multi-intent aware.
    // detectedIntents contains all intents above the confidence threshold.
    const intentKeys = [];
    if (event.classification.detectedIntents && event.classification.detectedIntents.length > 0) {
      for (const di of event.classification.detectedIntents) {
        intentKeys.push(di.fullKey);
      }
    } else {
      intentKeys.push(event.classification.primary.fullKey);
    }

    const activeJourneys = this._getStoredJourneys(customerId, { activeOnly: true });

    // Phase 1: Try transitions on existing active journeys with ALL detected intents
    for (const intentKey of intentKeys) {
      for (const journey of activeJourneys) {
        const transition = this._tryTransition(journey, intentKey, channel, event);
        if (transition) transitions.push(transition);
      }
    }

    // Phase 2: For intents that didn't trigger transitions, try creating new journeys
    const transitionedIntents = new Set(transitions.map(t => t.trigger));
    const createdJourneyTypes = new Set(); // Prevent duplicate journey types within same event
    for (const intentKey of intentKeys) {
      if (transitionedIntents.has(intentKey)) continue;

      const matchingDefs = findMatchingJourneys(intentKey);
      for (const match of matchingDefs) {
        const existing = activeJourneys.find(journey => journey.journey_type === match.type);
        if (!existing && !createdJourneyTypes.has(match.type)) {
          createdJourneyTypes.add(match.type);
          const newJourney = this._createJourney(customerId, match.type, channel, event);
          const newTransition = {
            journey_id: newJourney.journey_id,
            customer_id: customerId,
            journey_type: match.type,
            from_state: null,
            to_state: newJourney.current_state,
            trigger: intentKey,
            channel,
            timestamp: new Date().toISOString(),
            routing_priority: event.routing?.priority || null,
            sentiment: event.analysis?.sentiment || null,
            isNew: true,
            isTerminal: false
          };
          transitions.push(newTransition);
          this.history.push(newTransition);
          this._persistTransition(newTransition);
        }
      }
    }

    return {
      journeys: this.getActiveJourneys(customerId),
      transitions,
      intelligence: this.getJourneyIntelligence({ customerId })
    };
  }

  /**
   * Create a new journey from a definition. Initializes at the definition's initial state.
   * @param {string} customerId - Canonical customer ID
   * @param {string} journeyType - Journey type key
   * @param {string} channel - Originating channel
   * @param {Object} triggerEvent - Event that initiated this journey
   * @returns {Object} Newly created journey record
   * @throws {Error} If journey type is not defined
   * @private
   */
  _createJourney(customerId, journeyType, channel, triggerEvent) {
    const def = getJourneyDefinition(journeyType);
    if (!def) throw new Error('Unknown journey type: ' + journeyType);

    const journeyId = uuidv4();
    const now = new Date().toISOString();
    const timeout = this._getTimeoutConfig(def, def.initialState);

    const journey = {
      customer_id: customerId,
      journey_key: journeyType + '#' + journeyId,
      journey_id: journeyId,
      journey_type: journeyType,
      journey_label: def.label,
      current_state: def.initialState,
      current_state_label: def.states[def.initialState].label,
      current_state_entered_at: now,
      status: 'active',
      channels: [channel],
      cross_channel_handoffs: 0,
      started_at: now,
      updated_at: now,
      last_transition_at: now,
      state_history: [{
        state: def.initialState,
        entered_at: now,
        channel,
        trigger: triggerEvent.classification?.primary?.fullKey || 'system',
        sentiment: triggerEvent.analysis?.sentiment || null
      }],
      context: {
        trigger_event_id: triggerEvent.event_id,
        initial_channel: channel,
        latest_channel: channel,
        latest_intent: triggerEvent.classification?.primary?.fullKey || null,
        latest_sentiment: triggerEvent.analysis?.sentiment || null,
        customer_effort_score: triggerEvent.analysis?.customerEffortScore || null,
        routing_priority: triggerEvent.routing?.priority || null,
        total_transitions: 0
      },
      sla: timeout ? {
        target_state: timeout.state,
        after_minutes: timeout.afterMinutes,
        due_at: addMinutes(now, timeout.afterMinutes)
      } : null
    };

    this.store.put(journey);
    return journey;
  }

  /**
   * Attempt to transition a journey based on an intent key. Checks the current
   * state's transition rules, updates state, detects cross-channel handoffs,
   * and marks terminal states as completed.
   * @param {Object} journey - Active journey
   * @param {string} intentKey - Intent to match against transition rules
   * @param {string} channel - Current channel
   * @param {Object} event - Full event for context
   * @returns {Object|null} Transition record, or null if no transition matched
   * @private
   */
  _tryTransition(journey, intentKey, channel, event) {
    const def = getJourneyDefinition(journey.journey_type);
    if (!def) return null;

    const stateConfig = def.states[journey.current_state];
    if (!stateConfig || !stateConfig.transitions) return null;

    const nextState = stateConfig.transitions[intentKey];
    if (!nextState) return null;

    const fromState = journey.current_state;
    const now = new Date().toISOString();
    const previousChannel = journey.context?.latest_channel || journey.channels[journey.channels.length - 1];
    const handoffOccurred = previousChannel && previousChannel !== channel;

    journey.current_state = nextState;
    journey.current_state_label = def.states[nextState]?.label || nextState;
    journey.current_state_entered_at = now;
    journey.updated_at = now;
    journey.last_transition_at = now;
    journey.context.total_transitions++;
    journey.context.latest_channel = channel;
    journey.context.latest_intent = intentKey;
    journey.context.latest_sentiment = event.analysis?.sentiment || journey.context.latest_sentiment || null;
    journey.context.customer_effort_score = event.analysis?.customerEffortScore || journey.context.customer_effort_score || null;
    journey.context.routing_priority = event.routing?.priority || journey.context.routing_priority || null;

    if (!journey.channels.includes(channel)) {
      journey.channels.push(channel);
    }
    if (handoffOccurred) {
      journey.cross_channel_handoffs = (journey.cross_channel_handoffs || 0) + 1;
    }

    journey.state_history.push({
      state: nextState,
      entered_at: now,
      channel,
      trigger: intentKey,
      sentiment: event.analysis?.sentiment || null
    });

    const timeout = this._getTimeoutConfig(def, nextState);
    journey.sla = timeout ? {
      target_state: timeout.state,
      after_minutes: timeout.afterMinutes,
      due_at: addMinutes(now, timeout.afterMinutes)
    } : null;

    const isTerminal = def.terminalStates.includes(nextState);
    if (isTerminal) {
      journey.status = 'completed';
      journey.completed_at = now;
    }

    this.store.put(journey);

    const transition = {
      journey_id: journey.journey_id,
      customer_id: journey.customer_id,
      journey_type: journey.journey_type,
      from_state: fromState,
      to_state: nextState,
      trigger: intentKey,
      channel,
      timestamp: now,
      routing_priority: event.routing?.priority || null,
      sentiment: event.analysis?.sentiment || null,
      handoffOccurred,
      isTerminal,
      isNew: false
    };

    this.history.push(transition);
    this._persistTransition(transition);
    return transition;
  }

  /**
   * Retrieve journeys, optionally filtered to active-only.
   * @param {string|null} customerId - Customer ID (null for all)
   * @param {Object} [options]
   * @param {boolean} [options.activeOnly=false] - Filter to active journeys only
   * @returns {Array<Object>} Journey records
   * @private
   */
  _getStoredJourneys(customerId, { activeOnly = false } = {}) {
    const journeys = customerId ? this.store.query(customerId) : this.store.scan();
    if (activeOnly) return journeys.filter(journey => journey.status === 'active');
    return journeys;
  }

  _getTimeoutConfig(definition, state) {
    return definition?.states?.[state]?.transitions?._timeout || null;
  }

  /**
   * Enrich a journey with computed runtime fields: time-in-state, SLA status,
   * and stuck detection. These fields are computed on read, not persisted.
   * @param {Object} journey - Raw journey record
   * @param {number} [nowMs=Date.now()] - Current timestamp (injectable for testing)
   * @returns {Object} Enriched journey
   * @private
   */
  _enrichJourney(journey, nowMs = Date.now()) {
    const definition = getJourneyDefinition(journey.journey_type);
    const timeout = this._getTimeoutConfig(definition, journey.current_state);
    const enteredAt = journey.current_state_entered_at || journey.updated_at || journey.started_at;
    const stateStartMs = enteredAt ? new Date(enteredAt).getTime() : nowMs;
    const minutesInState = Math.max(0, Math.round((nowMs - stateStartMs) / 60000));

    let slaStatus = journey.status === 'completed' ? 'closed' : 'compliant';
    let isStuck = false;

    if (journey.status === 'active' && timeout) {
      const atRiskMinutes = Math.round(timeout.afterMinutes * 0.8);
      if (minutesInState >= timeout.afterMinutes) {
        slaStatus = 'breached';
        isStuck = true;
      } else if (minutesInState >= atRiskMinutes) {
        slaStatus = 'at_risk';
      }
    } else if (journey.status === 'active' && minutesInState >= 1440) {
      slaStatus = 'at_risk';
    }

    return {
      ...journey,
      minutes_in_state: minutesInState,
      timeout_after_minutes: timeout?.afterMinutes || null,
      timeout_target_state: timeout?.state || null,
      sla_status: slaStatus,
      is_stuck: isStuck
    };
  }

  getActiveJourneys(customerId) {
    return this._getStoredJourneys(customerId, { activeOnly: true }).map(journey => this._enrichJourney(journey));
  }

  getAllJourneys(customerId) {
    return this._getStoredJourneys(customerId).map(journey => this._enrichJourney(journey));
  }

  getJourney(customerId, journeyId) {
    const all = this._getStoredJourneys(customerId);
    const found = all.find(journey => journey.journey_id === journeyId);
    return found ? this._enrichJourney(found) : null;
  }

  getJourneysByType(journeyType) {
    return this.store.queryGSI('by-type-state', journeyType).map(journey => this._enrichJourney(journey));
  }

  getRecentTransitions(limit = 50) {
    return this.history.slice(-limit);
  }

  /**
   * Compute journey intelligence: active/completed counts, stuck journeys,
   * common paths, top triggers, cross-channel handoffs, and per-type summaries.
   * @param {Object} [options]
   * @param {string|null} [options.customerId=null] - Scope to customer
   * @param {number} [options.limit=10] - Max items in ranked lists
   * @param {number} [options.nowMs=Date.now()] - Current timestamp for SLA
   * @returns {Object} Journey intelligence report
   */
  getJourneyIntelligence({ customerId = null, limit = 10, nowMs = Date.now() } = {}) {
    const all = this._getStoredJourneys(customerId).map(journey => this._enrichJourney(journey, nowMs));
    const active = all.filter(journey => journey.status === 'active');
    const completed = all.filter(journey => journey.status === 'completed');

    const activeByType = {};
    const activeByState = {};
    const pathCounts = {};
    const triggerCounts = {};
    const typeSummary = {};
    let crossChannelHandoffs = 0;

    for (const journey of all) {
      const path = journey.state_history.map(entry => entry.state).join(' -> ');
      pathCounts[path] = (pathCounts[path] || 0) + 1;
      crossChannelHandoffs += journey.cross_channel_handoffs || 0;

      for (const entry of journey.state_history) {
        if (!entry.trigger || entry.trigger === 'system') continue;
        triggerCounts[entry.trigger] = (triggerCounts[entry.trigger] || 0) + 1;
      }

      if (!typeSummary[journey.journey_type]) {
        typeSummary[journey.journey_type] = {
          journey_type: journey.journey_type,
          label: journey.journey_label,
          total: 0,
          active: 0,
          completed: 0,
          totalTransitions: 0,
          totalChannels: 0,
          totalMinutesInState: 0
        };
      }

      const summary = typeSummary[journey.journey_type];
      summary.total++;
      summary.totalTransitions += journey.context?.total_transitions || 0;
      summary.totalChannels += (journey.channels || []).length;
      summary.totalMinutesInState += journey.minutes_in_state || 0;
      if (journey.status === 'active') summary.active++;
      if (journey.status === 'completed') summary.completed++;
    }

    for (const journey of active) {
      activeByType[journey.journey_type] = (activeByType[journey.journey_type] || 0) + 1;
      const stateKey = journey.journey_type + ':' + journey.current_state;
      activeByState[stateKey] = (activeByState[stateKey] || 0) + 1;
    }

    const stuckJourneys = active
      .filter(journey => journey.is_stuck || journey.sla_status === 'at_risk' || journey.sla_status === 'breached')
      .sort((a, b) => b.minutes_in_state - a.minutes_in_state)
      .slice(0, limit)
      .map(journey => ({
        customer_id: journey.customer_id,
        journey_id: journey.journey_id,
        journey_type: journey.journey_type,
        current_state: journey.current_state,
        minutes_in_state: journey.minutes_in_state,
        sla_status: journey.sla_status,
        cross_channel_handoffs: journey.cross_channel_handoffs
      }));

    const commonPaths = Object.entries(pathCounts)
      .sort((a, b) => b[1] - a[1])
      .slice(0, limit)
      .map(([path, count]) => ({ path, count }));

    const topTriggers = Object.entries(triggerCounts)
      .sort((a, b) => b[1] - a[1])
      .slice(0, limit)
      .map(([trigger, count]) => ({ trigger, count }));

    const journeyTypeSummary = Object.values(typeSummary)
      .map(summary => ({
        journey_type: summary.journey_type,
        label: summary.label,
        total: summary.total,
        active: summary.active,
        completed: summary.completed,
        avg_transitions: summary.total ? round(summary.totalTransitions / summary.total) : 0,
        avg_channels: summary.total ? round(summary.totalChannels / summary.total) : 0,
        avg_minutes_in_state: summary.total ? round(summary.totalMinutesInState / summary.total) : 0
      }))
      .sort((a, b) => b.total - a.total);

    return {
      scope: customerId || 'all',
      activeJourneyCount: active.length,
      completedJourneyCount: completed.length,
      stuckJourneyCount: stuckJourneys.length,
      crossChannelHandoffs,
      activeByType,
      activeByState,
      stuckJourneys,
      commonPaths,
      topTriggers,
      journeyTypeSummary,
      recentTransitions: this.getRecentTransitions(limit)
    };
  }

  /** Get journey engine statistics for the status endpoint. @returns {Object} */
  getStats() {
    const intelligence = this.getJourneyIntelligence();

    return {
      total: this.store.count(),
      active: intelligence.activeJourneyCount,
      completed: intelligence.completedJourneyCount,
      byType: intelligence.activeByType,
      byState: intelligence.activeByState,
      crossChannelJourneys: this.store.scan().filter(journey => (journey.channels || []).length > 1).length,
      crossChannelHandoffs: intelligence.crossChannelHandoffs,
      stuckJourneyCount: intelligence.stuckJourneyCount,
      totalTransitions: this.history.length
    };
  }
}

/**
 * Add minutes to an ISO timestamp.
 * @param {string} isoTimestamp - ISO 8601 timestamp
 * @param {number} minutes - Minutes to add
 * @returns {string} Resulting ISO timestamp
 */
function addMinutes(isoTimestamp, minutes) {
  const date = new Date(isoTimestamp);
  date.setMinutes(date.getMinutes() + minutes);
  return date.toISOString();
}

function round(value) {
  return Math.round(value * 100) / 100;
}

// ── Singleton ─────────────────────────────────────────────────────────
/** @type {JourneyEngine|null} */
let instance = null;
/** Get or create the singleton JourneyEngine. @returns {JourneyEngine} */
function getJourneyEngine() {
  if (!instance) instance = new JourneyEngine();
  return instance;
}

module.exports = { JourneyEngine, getJourneyEngine };
