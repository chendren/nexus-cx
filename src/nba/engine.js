/**
 * @module nexus-cx/nba/engine
 * @description Next-Best-Action (NBA) recommendation engine for real-time action orchestration.
 *
 * Implements a three-tier evaluation approach:
 *   1. **Deterministic rules**: business rules that always fire when conditions match
 *      (e.g., high churn risk → retention offer). Auditable and explainable.
 *   2. **Scored recommendations**: simulated ML-based scoring using customer context,
 *      journey state, and interaction history to rank candidate actions
 *   3. **Channel-specific routing**: directs actions to the appropriate delivery channel
 *      (voice prompt, chat message, email, SMS) with priority-based ordering
 *
 * Action types: retention_offer, escalation, self_service_nudge, proactive_outreach,
 * cross_sell, survey_trigger, knowledge_article, callback_schedule
 *
 * Persistence: action log is write-through to `nba_actions` SQLite table when
 * `PERSIST=true`. Statistics (action counts, conversion rates) are recalculated
 * from the hydrated action log on startup.
 *
 * @requires ./rules
 * @requires uuid
 * @requires ../config
 * @requires ../logger
 *
 * @see {@link module:nexus-cx/nba/rules} Deterministic business rules
 * @see {@link module:nexus-cx/pipeline} Pipeline orchestrator (consumer)
 */
const { RULES } = require('./rules');
const { v4: uuidv4 } = require('uuid');
const config = require('../config');
const { createLogger } = require('../logger');

const log = createLogger('nba');

/**
 * Next-Best-Action engine combining deterministic business rules with simulated
 * ML scoring and channel-aware routing.
 *
 * Evaluation flow: deterministic rules -> ML scoring -> deduplication -> top-3 ranking -> channel routing.
 *
 * @class
 */
class NBAEngine {
  constructor() {
    /** @type {Object[]} Rolling log of all emitted actions (bounded by NBA_ACTION_LOG_MAX) */
    this.actionLog = [];

    /** @type {Object} Cumulative statistics for monitoring and dashboard display */
    this.stats = {
      totalEvaluations: 0,
      actionsTriggered: 0,
      deflectionsTriggered: 0,
      byActionType: {},
      byChannel: {},
      byUrgency: {},
      priorityDistribution: {}
    };

    /** @type {?Object} Cached prepared statement for SQLite action persistence */
    this._actionStmt = null;
  }

  /**
   * Restore action log and statistics from the SQLite `nba_actions` table.
   * Rows are read in descending order but pushed in ascending order so the
   * in-memory log preserves chronological ordering.
   *
   * @returns {void}
   */
  hydrate() {
    if (!config.PERSIST) return;
    try {
      const { getDatabase } = require('../store/database');
      const db = getDatabase();
      const rows = db.prepare(
        'SELECT data FROM nba_actions ORDER BY id DESC LIMIT ?'
      ).all(config.NBA_ACTION_LOG_MAX);

      // Reverse iteration restores chronological order (rows come back newest-first)
      for (let i = rows.length - 1; i >= 0; i--) {
        try {
          const action = JSON.parse(rows[i].data);
          this.actionLog.push(action);
          this.stats.actionsTriggered++;
          if (action.type) {
            this.stats.byActionType[action.type] = (this.stats.byActionType[action.type] || 0) + 1;
          }
        } catch {}
      }
      log.info({ loaded: this.actionLog.length }, 'NBA action log hydrated');
    } catch {}
  }

  /**
   * Write a single action record to the `nba_actions` SQLite table.
   * Uses a lazily-initialized prepared statement for performance.
   *
   * @param {Object} action - The fully-built action object to persist
   * @returns {void}
   * @private
   */
  _persistAction(action) {
    if (!config.PERSIST) return;
    try {
      if (!this._actionStmt) {
        const { getDatabase } = require('../store/database');
        this._actionStmt = getDatabase().prepare(
          `INSERT INTO nba_actions (action_id, action_type, urgency, data, timestamp)
           VALUES (@action_id, @action_type, @urgency, @data, @timestamp)`
        );
      }
      this._actionStmt.run({
        action_id: action.action_id || '',
        action_type: action.type || '',
        urgency: action.urgency || '',
        data: JSON.stringify(action),
        timestamp: action.timestamp || new Date().toISOString()
      });
    } catch {}
  }

  /**
   * Core evaluation entry point. Runs the full three-tier action pipeline:
   *   1. Evaluate all deterministic rules against the enriched context
   *   2. Generate ML-scored recommendations based on customer signals
   *   3. Deduplicate, rank by priority, take top 3, and route to delivery channel
   *
   * Each emitted action is logged, persisted, and counted in statistics.
   *
   * @param {Object} ctx - Evaluation context
   * @param {Object} ctx.event - The enriched event being processed
   * @param {Object} [ctx.classification] - Classifier output (domain, intent, confidence)
   * @param {Object} [ctx.profile] - Unified customer profile from identity resolution
   * @param {Object} [ctx.journey] - Best-matching active journey from the state machine
   * @param {Object} [ctx.analysis] - Deterministic or LLM-based interaction analysis
   * @returns {{ actions: Object[], totalRulesMatched: number, channel: string, routing: Object, ruleErrors: Object[], decisionSummary: Object }}
   */
  evaluate(ctx) {
    this.stats.totalEvaluations++;

    const channel = ctx.event?.channel || 'web';
    const routing = this._buildRoutingContext(ctx);
    const enrichedContext = {
      ...ctx,
      analysis: ctx.analysis || ctx.event?.analysis || null,
      routing
    };

    const matchedActions = [];
    const ruleErrors = [];

    for (const rule of RULES) {
      try {
        if (rule.condition(enrichedContext)) {
          matchedActions.push({
            rule_id: rule.id,
            rule_name: rule.name,
            priority: rule.priority,
            ...rule.action,
            source: 'deterministic'
          });
        }
      } catch (error) {
        ruleErrors.push({ rule_id: rule.id, error: error.message });
        log.warn({ ruleId: rule.id, err: error }, 'Rule evaluation failed');
      }
    }

    const mlScores = this._simulateMLScoring(enrichedContext);
    for (const score of mlScores) {
      matchedActions.push({
        ...score,
        source: 'ml_scored'
      });
    }

    const rankedActions = this._deduplicateActions(
      matchedActions.sort((a, b) => (b.priority || 0) - (a.priority || 0))
    ).slice(0, 3);

    const routedActions = rankedActions.map(action => ({
      action_id: uuidv4(),
      ...action,
      delivery: this._routeToChannel(action, channel, routing),
      decision_factors: this._buildDecisionFactors(enrichedContext, action),
      timestamp: new Date().toISOString()
    }));

    for (const action of routedActions) {
      this.actionLog.push(action);
      this._persistAction(action);
      this.stats.actionsTriggered++;
      this.stats.byActionType[action.type] = (this.stats.byActionType[action.type] || 0) + 1;
      this.stats.byChannel[channel] = (this.stats.byChannel[channel] || 0) + 1;
      this.stats.byUrgency[action.urgency] = (this.stats.byUrgency[action.urgency] || 0) + 1;
      if (['self_service_redirect', 'digital_deflection'].includes(action.type)) {
        this.stats.deflectionsTriggered++;
      }
    }

    this.stats.priorityDistribution[routing.priority] = (this.stats.priorityDistribution[routing.priority] || 0) + 1;

    return {
      actions: routedActions,
      totalRulesMatched: matchedActions.length,
      channel,
      routing,
      ruleErrors,
      decisionSummary: {
        topActionType: routedActions[0]?.type || null,
        shouldDeflect: routing.should_deflect,
        contactValue: routing.contact_value,
        slaStatus: routing.sla_status
      }
    };
  }

  /**
   * Compute the full routing context including priority score, SLA status,
   * estimated wait time, skill match score, contact value, and deflection decision.
   *
   * Priority is computed additively from channel baseline, customer tier, urgency,
   * domain, sentiment, wait time, skill availability, and journey state, then
   * clamped to [0, 10].
   *
   * @param {Object} ctx - Evaluation context containing event, profile, classification, analysis, journey
   * @returns {{ priority: number, available_agents: number, estimated_wait_time: number, skill_match_score: number, contact_value: number, sla_status: string, should_deflect: boolean, recommended_queue: string, channel: string }}
   * @private
   */
  _buildRoutingContext(ctx) {
    const event = ctx.event || {};
    const channel = event.channel || 'web';
    const classification = ctx.classification || event.classification || {};
    const analysis = ctx.analysis || event.analysis || {};
    const profile = ctx.profile || {};
    const journey = ctx.journey || {};
    const metadata = event.payload?.metadata || {};
    const configuredWait = Number(event.context?.queue_wait_minutes || 0);

    let priority = basePriorityByChannel(channel);

    const tier = profile.attributes?.customer_tier || metadata.customer_tier || 'standard';
    if (tier === 'premium' || tier === 'enterprise') priority += 3;
    else if (tier === 'gold') priority += 2;

    const urgency = analysis.urgency || 'medium';
    if (urgency === 'critical') priority += 3;
    else if (urgency === 'high') priority += 2;
    else if (urgency === 'medium') priority += 1;

    const domain = classification?.primary?.domain;
    if (['billing', 'technical_support', 'account'].includes(domain)) priority += 1;
    if (classification?.primary?.fullKey === 'account.cancellation') priority += 2;

    const sentiment = analysis.sentiment || 'neutral';
    if (sentiment === 'angry') priority += 3;
    else if (sentiment === 'frustrated' || sentiment === 'negative') priority += 2;
    else if (sentiment === 'positive') priority -= 1;

    const estimatedWaitTime = configuredWait > 0 ? configuredWait : this._estimateWaitTime(channel, priority);
    if (estimatedWaitTime > 10) priority += 1;
    if (estimatedWaitTime > 20) priority += 1;

    const skillMatchScore = this._estimateSkillMatch(ctx);
    if (skillMatchScore < 50) priority += 2;
    else if (skillMatchScore < 75) priority += 1;

    if (journey?.status === 'active' && (journey?.cross_channel_handoffs || 0) > 0) {
      priority += 1;
    }

    priority = clamp(priority, 0, 10);

    const availableAgents = this._estimateAvailableAgents(channel, priority);
    const contactValue = this._estimateContactValue(ctx, priority);
    const slaStatus = this._computeSlaStatus(priority, estimatedWaitTime);
    const shouldDeflect = this._shouldDeflect(ctx, { priority, contactValue, sentiment, urgency });

    return {
      priority,
      available_agents: availableAgents,
      estimated_wait_time: estimatedWaitTime,
      skill_match_score: skillMatchScore,
      contact_value: contactValue,
      sla_status: slaStatus,
      should_deflect: shouldDeflect,
      recommended_queue: this._selectQueue(domain, priority, tier),
      channel
    };
  }

  /**
   * Simulate ML model scoring for candidate actions. In production this would
   * call a SageMaker real-time inference endpoint. The simulation generates
   * scored actions based on heuristic signals:
   *   - Churn propensity: elevated when interaction_count > 5
   *   - Upsell propensity: triggered when domain is 'sales'
   *   - Friction recovery: triggered for angry sentiment + high priority
   *
   * @param {Object} ctx - Enriched evaluation context with routing already computed
   * @returns {Object[]} Array of ML-scored action candidates with priority, type, and model attribution
   * @private
   */
  _simulateMLScoring(ctx) {
    const scores = [];

    if (ctx.profile && ctx.profile.interaction_count > 5) {
      const churnScore = Math.min(0.95, (ctx.profile.interaction_count - 3) * 0.1);
      if (churnScore > 0.5) {
        scores.push({
          type: 'proactive_retention',
          urgency: churnScore > 0.75 ? 'high' : 'medium',
          priority: Math.round(churnScore * 55),
          content: 'ML model predicts elevated churn risk (' + Math.round(churnScore * 100) + '%)',
          params: { churn_score: round(churnScore), contact_value: ctx.routing.contact_value },
          ml_model: 'churn_propensity_v2'
        });
      }
    }

    if (ctx.classification?.primary?.domain === 'sales') {
      scores.push({
        type: 'upsell_recommendation',
        urgency: 'low',
        priority: 25 + Math.round(ctx.routing.contact_value / 10),
        content: 'Customer is showing purchase intent — recommend premium features',
        params: {
          conversion_probability: 0.35,
          recommended_queue: ctx.routing.recommended_queue
        },
        ml_model: 'upsell_propensity_v2'
      });
    }

    if (ctx.analysis?.sentiment === 'angry' && ctx.routing.priority >= 8) {
      scores.push({
        type: 'manager_callback',
        urgency: 'high',
        priority: 78,
        content: 'High-risk interaction detected — schedule rapid manager callback',
        params: { callback_within_minutes: 15 },
        ml_model: 'friction_recovery_v1'
      });
    }

    return scores;
  }

  /**
   * Remove duplicate actions by composite key (type + rule_id/ml_model/content).
   * Preserves the first occurrence, which is the highest-priority due to pre-sorting.
   *
   * @param {Object[]} actions - Pre-sorted array of candidate actions (highest priority first)
   * @returns {Object[]} Deduplicated action array preserving sort order
   * @private
   */
  _deduplicateActions(actions) {
    const seen = new Set();
    const deduped = [];

    for (const action of actions) {
      const key = action.type + ':' + (action.rule_id || action.ml_model || action.content);
      if (seen.has(key)) continue;
      seen.add(key);
      deduped.push(action);
    }

    return deduped;
  }

  /**
   * Estimate queue wait time in minutes based on channel baseline and priority.
   * Higher priority reduces wait time (up to 2 minutes off baseline). Minimum is 1 minute.
   * In production, this would query Amazon Connect real-time queue metrics.
   *
   * @param {string} channel - Contact channel (voice, chat, web, etc.)
   * @param {number} priority - Computed routing priority [0-10]
   * @returns {number} Estimated wait time in minutes (rounded to 2 decimal places)
   * @private
   */
  _estimateWaitTime(channel, priority) {
    const baseline = {
      voice: 8,
      chat: 4,
      web: 2,
      mobile: 3,
      email: 25,
      sms: 5,
      whatsapp: 6
    };

    const rawWait = (baseline[channel] || 4) - Math.min(priority, 5) * 0.4;
    return round(Math.max(1, rawWait));
  }

  /**
   * Estimate how well available agents match the required skill set.
   * Score starts at 85 and is penalized for complex skill requirements,
   * technical domains, critical urgency, and multi-channel customers.
   * Clamped to [35, 98].
   *
   * @param {Object} ctx - Evaluation context
   * @returns {number} Skill match score as a percentage [35-98]
   * @private
   */
  _estimateSkillMatch(ctx) {
    const requiredSkills = Array.isArray(ctx.event?.context?.required_skills)
      ? ctx.event.context.required_skills.length
      : 0;
    const domain = ctx.classification?.primary?.domain;

    let score = 85;
    if (requiredSkills >= 3) score -= 20;
    else if (requiredSkills === 2) score -= 10;

    if (domain === 'technical_support') score -= 10;
    if (ctx.analysis?.urgency === 'critical') score -= 10;
    if (ctx.profile?.channels_seen?.length >= 3) score -= 5;

    return clamp(score, 35, 98);
  }

  /**
   * Estimate the number of available agents for a given channel and priority.
   * Higher priority contacts consume more agent capacity, reducing availability.
   * In production, this would query Amazon Connect agent availability APIs.
   *
   * @param {string} channel - Contact channel
   * @param {number} priority - Routing priority [0-10]
   * @returns {number} Estimated available agent count (minimum 1)
   * @private
   */
  _estimateAvailableAgents(channel, priority) {
    const baseline = {
      voice: 6,
      chat: 10,
      web: 20,
      mobile: 18,
      email: 14,
      sms: 12,
      whatsapp: 8
    };

    const available = (baseline[channel] || 8) - Math.max(0, priority - 5);
    return Math.max(1, available);
  }

  /**
   * Estimate the monetary value of the contact for prioritization and reporting.
   * Base value scales with priority; bonuses added for premium tiers, sales domain,
   * cancellation intent, churn prevention journeys, and angry sentiment.
   * Clamped to [1, 100].
   *
   * @param {Object} ctx - Evaluation context
   * @param {number} priority - Computed routing priority
   * @returns {number} Contact value score [1-100]
   * @private
   */
  _estimateContactValue(ctx, priority) {
    let value = 35 + priority * 4;
    const tier = ctx.profile?.attributes?.customer_tier || ctx.event?.payload?.metadata?.customer_tier || 'standard';

    if (tier === 'premium' || tier === 'enterprise') value += 20;
    else if (tier === 'gold') value += 10;

    if (ctx.classification?.primary?.domain === 'sales') value += 15;
    if (ctx.classification?.primary?.fullKey === 'account.cancellation') value += 20;
    if (ctx.journey?.journey_type === 'churn_prevention') value += 15;
    if (ctx.analysis?.sentiment === 'angry') value += 10;

    return clamp(value, 1, 100);
  }

  /**
   * Determine SLA compliance status based on priority and estimated wait time.
   * High-priority contacts have tighter SLA thresholds.
   *
   * @param {number} priority - Routing priority [0-10]
   * @param {number} estimatedWaitTime - Estimated wait time in minutes
   * @returns {'compliant'|'at_risk'|'breached'} SLA status
   * @private
   */
  _computeSlaStatus(priority, estimatedWaitTime) {
    if (priority >= 8 && estimatedWaitTime > 6) return 'breached';
    if (priority >= 7 && estimatedWaitTime > 4) return 'at_risk';
    if (priority >= 5 && estimatedWaitTime > 8) return 'at_risk';
    return 'compliant';
  }

  /**
   * Determine whether this contact should be deflected to self-service.
   * Deflection is only recommended when ALL of these conditions are met:
   *   - Channel supports digital deflection (web, mobile, chat, sms, whatsapp)
   *   - Intent is in the deflectable set (password_reset, billing_inquiry, connectivity, app_error)
   *   - Priority < 7 and contact value < 80
   *   - Urgency is not high/critical
   *   - Sentiment is not negative/frustrated/angry
   *   - Customer is not on a churn prevention journey
   *
   * @param {Object} ctx - Evaluation context
   * @param {Object} signals - Pre-computed routing signals
   * @param {number} signals.priority - Routing priority
   * @param {number} signals.contactValue - Estimated contact value
   * @param {string} signals.sentiment - Customer sentiment
   * @param {string} signals.urgency - Interaction urgency
   * @returns {boolean} True if the contact should be deflected to self-service
   * @private
   */
  _shouldDeflect(ctx, { priority, contactValue, sentiment, urgency }) {
    const intent = ctx.classification?.primary?.fullKey;
    const channel = ctx.event?.channel;
    const allowedChannels = ['web', 'mobile', 'chat', 'sms', 'whatsapp'];
    const deflectableIntents = new Set([
      'account.password_reset',
      'billing.billing_inquiry',
      'technical_support.connectivity',
      'technical_support.app_error'
    ]);

    if (!allowedChannels.includes(channel)) return false;
    if (!deflectableIntents.has(intent)) return false;
    if (priority >= 7 || contactValue >= 80) return false;
    if (urgency === 'high' || urgency === 'critical') return false;
    if (['negative', 'frustrated', 'angry'].includes(sentiment)) return false;
    if (ctx.journey?.journey_type === 'churn_prevention') return false;

    return true;
  }

  /**
   * Select the recommended queue based on customer tier, priority, and domain.
   * Premium/enterprise customers always go to the premium queue regardless of domain.
   * In production, maps to Amazon Connect routing profiles.
   *
   * @param {string} domain - Classification domain (billing, technical_support, sales, etc.)
   * @param {number} priority - Routing priority [0-10]
   * @param {string} tier - Customer tier (standard, gold, premium, enterprise)
   * @returns {string} Queue identifier
   * @private
   */
  _selectQueue(domain, priority, tier) {
    if (tier === 'premium' || tier === 'enterprise') return 'premium-support';
    if (priority >= 8) return 'priority-escalations';
    if (domain === 'technical_support') return 'technical-support';
    if (domain === 'billing') return 'billing-care';
    if (domain === 'sales') return 'sales-specialists';
    return 'general-support';
  }

  /**
   * Build an explainability payload capturing the key factors that influenced
   * this action decision. Used for audit trails and agent-facing rationale display.
   *
   * @param {Object} ctx - Enriched evaluation context
   * @param {Object} action - The action being explained
   * @returns {{ intent: ?string, journey_type: ?string, sentiment: ?string, urgency: ?string, routing_priority: number, sla_status: string, contact_value: number, source: string }}
   * @private
   */
  _buildDecisionFactors(ctx, action) {
    return {
      intent: ctx.classification?.primary?.fullKey || null,
      journey_type: ctx.journey?.journey_type || null,
      sentiment: ctx.analysis?.sentiment || null,
      urgency: ctx.analysis?.urgency || null,
      routing_priority: ctx.routing.priority,
      sla_status: ctx.routing.sla_status,
      contact_value: ctx.routing.contact_value,
      source: action.source
    };
  }

  /**
   * Route an action to the appropriate channel-specific delivery mechanism.
   * Maps each channel to its AWS service and format:
   *   - voice -> Amazon Q in Connect agent assist panel
   *   - chat -> Agent CCP suggested response card
   *   - web -> WebSocket push notification
   *   - mobile -> Push notification
   *   - email -> Amazon SES outbound
   *   - sms -> Amazon SNS SMS
   *   - whatsapp -> WhatsApp Business API
   *
   * @param {Object} action - The action to route
   * @param {string} channel - Target delivery channel
   * @param {Object} routing - Computed routing context (includes queue and priority)
   * @returns {{ mechanism: string, description: string, format: string, queue: string, priority: number }}
   * @private
   */
  _routeToChannel(action, channel, routing) {
    const deliveryMap = {
      voice: {
        mechanism: 'agent_assist',
        description: 'Push to Amazon Q in Connect agent assist panel',
        format: 'suggested_response'
      },
      chat: {
        mechanism: 'chat_suggestion',
        description: 'Display as suggested response in agent CCP',
        format: 'rich_card'
      },
      web: {
        mechanism: 'websocket_push',
        description: 'Real-time push via WebSocket to web app',
        format: 'in_app_notification'
      },
      mobile: {
        mechanism: 'push_notification',
        description: 'Mobile push notification or in-app message',
        format: 'push'
      },
      email: {
        mechanism: 'ses_outbound',
        description: 'Send via Amazon SES or Connect outbound',
        format: 'email_template'
      },
      sms: {
        mechanism: 'sns_sms',
        description: 'Send via Amazon SNS SMS',
        format: 'short_text'
      },
      whatsapp: {
        mechanism: 'whatsapp_api',
        description: 'Send via WhatsApp Business API',
        format: 'rich_message'
      }
    };

    const base = deliveryMap[channel] || deliveryMap.web;

    return {
      ...base,
      queue: routing.recommended_queue,
      priority: routing.priority
    };
  }

  /**
   * Retrieve the most recent actions from the action log.
   *
   * @param {number} [limit=50] - Maximum number of actions to return
   * @returns {Object[]} Most recent actions in chronological order
   */
  getActionLog(limit = 50) {
    return this.actionLog.slice(-limit);
  }

  /**
   * Return a shallow copy of cumulative evaluation statistics.
   *
   * @returns {{ totalEvaluations: number, actionsTriggered: number, deflectionsTriggered: number, byActionType: Object, byChannel: Object, byUrgency: Object, priorityDistribution: Object }}
   */
  getStats() {
    return { ...this.stats };
  }
}

/**
 * Return the baseline routing priority for a given channel.
 * Voice contacts start highest (4) because they represent the most
 * expensive agent resource. Email starts lowest (1) due to async nature.
 *
 * @param {string} channel - Contact channel identifier
 * @returns {number} Baseline priority [1-4]
 */
function basePriorityByChannel(channel) {
  const priorities = {
    voice: 4,
    chat: 3,
    web: 2,
    mobile: 2,
    email: 1,
    sms: 2,
    whatsapp: 2
  };
  return priorities[channel] || 2;
}

/**
 * Clamp a numeric value to the inclusive range [min, max].
 *
 * @param {number} value - Value to clamp
 * @param {number} min - Lower bound
 * @param {number} max - Upper bound
 * @returns {number} Clamped value
 */
function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

/**
 * Round a number to 2 decimal places.
 *
 * @param {number} value - Value to round
 * @returns {number} Rounded value
 */
function round(value) {
  return Math.round(value * 100) / 100;
}

/**
 * Module-level singleton accessor for the NBAEngine.
 * Creates a new instance on first call; returns the same instance thereafter.
 *
 * @returns {NBAEngine} The singleton engine instance
 */
let instance = null;
function getNBAEngine() {
  if (!instance) instance = new NBAEngine();
  return instance;
}

module.exports = { NBAEngine, getNBAEngine };
