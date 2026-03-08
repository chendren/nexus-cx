/**
 * @module nexus-cx/analytics/metrics
 * @description Real-time analytics engine for aggregating metrics across all platform layers.
 *
 * Simulates Apache Flink stream aggregations and Amazon Timestream time-series storage
 * using in-memory data structures with periodic SQLite snapshots.
 *
 * Capabilities:
 *   - **Counters**: monotonic event counters (total events, classifications, journeys, actions)
 *   - **Gauges**: point-in-time measurements (active journeys, memory usage, queue depth)
 *   - **Time series**: rolling window of timestamped metric entries for trend visualization
 *   - **Sliding windows**: configurable time-bucketed aggregations (1m, 5m, 15m, 1h, 24h)
 *   - **KPI computation**: SLA compliance, FCR rate, channel distribution, CSAT averages
 *   - **Dashboard projection**: pre-computed view combining counters, gauges, KPIs, and summaries
 *
 * Persistence: counter snapshots written to `metrics_snapshots` every 60s and event records
 * to `events_archive` when `PERSIST=true`. Last snapshot hydrated on startup to restore counters.
 *
 * Memory monitoring: records `process.memoryUsage()` every 30s as gauges; logs warning
 * if heap exceeds 500MB.
 *
 * @requires ../config
 * @requires ../logger
 *
 * @see {@link module:nexus-cx/pipeline} Pipeline orchestrator (producer)
 * @see {@link module:nexus-cx/store/database} SQLite persistence for snapshots
 */
const config = require('../config');
const { createLogger } = require('../logger');

const log = createLogger('metrics');

/**
 * Real-time metrics aggregation engine. Collects counters, gauges, time-series
 * entries, and sliding window counts across all platform layers. Provides
 * pre-computed KPI snapshots and dashboard projections.
 *
 * In production, this would be backed by Amazon Timestream for time-series
 * storage and Apache Flink for stream aggregations. The in-memory implementation
 * preserves the same query interface.
 *
 * @class
 */
class MetricsEngine {
  constructor() {
    /** @type {Object[]} Rolling time-series buffer of metric entries (bounded by METRICS_SERIES_SIZE) */
    this.timeSeries = [];

    /** @type {Object} Nested monotonic counters keyed by dot-separated paths (e.g., 'events_by_channel.voice') */
    this.counters = {};

    /** @type {Object} Point-in-time gauge values (e.g., memory_rss_mb, last_priority) */
    this.gauges = {};

    /** @type {Map<string, Object>} Named sliding windows with timestamp arrays for rate computation */
    this.windows = new Map();

    /** @type {number} Maximum entries retained in the time-series buffer */
    this.maxSeriesLength = config.METRICS_SERIES_SIZE;

    /** @type {?Object} Cached prepared statement for snapshot persistence */
    this._snapshotStmt = null;

    /** @type {?Object} Cached prepared statement for event archival */
    this._eventStmt = null;

    /** @type {?Object} Interval handle for periodic snapshot writes */
    this._snapshotInterval = null;

    this._initWindows();
  }

  /**
   * Restore counters and gauges from the most recent SQLite snapshot, then
   * start periodic snapshot writes (every 60s) and memory monitoring (every 30s).
   * No-op when PERSIST is disabled.
   *
   * @returns {void}
   */
  hydrate() {
    if (!config.PERSIST) return;
    try {
      const { getDatabase } = require('../store/database');
      const db = getDatabase();
      const row = db.prepare(
        'SELECT counters, gauges FROM metrics_snapshots ORDER BY id DESC LIMIT 1'
      ).get();

      if (row) {
        this.counters = JSON.parse(row.counters);
        this.gauges = JSON.parse(row.gauges);
        log.info('Metrics counters hydrated from snapshot');
      }
    } catch {}

    // Start periodic snapshots and memory monitor
    this._snapshotInterval = setInterval(() => this._saveSnapshot(), 60000);
    this.startMemoryMonitor();
  }

  /**
   * Persist current counters, gauges, and computed KPIs to the `metrics_snapshots`
   * table. Called every 60s by the snapshot interval and once on destroy().
   *
   * @returns {void}
   * @private
   */
  _saveSnapshot() {
    if (!config.PERSIST) return;
    try {
      if (!this._snapshotStmt) {
        const { getDatabase } = require('../store/database');
        this._snapshotStmt = getDatabase().prepare(
          `INSERT INTO metrics_snapshots (counters, gauges, kpis, timestamp)
           VALUES (@counters, @gauges, @kpis, @timestamp)`
        );
      }
      this._snapshotStmt.run({
        counters: JSON.stringify(this.counters),
        gauges: JSON.stringify(this.gauges),
        kpis: JSON.stringify(this.getKPIs()),
        timestamp: new Date().toISOString()
      });
    } catch {}
  }

  /**
   * Archive a fully-enriched event to the `events_archive` table for historical
   * analysis and CSV/JSON export. Content is truncated to 500 chars. Uses INSERT
   * OR IGNORE to handle duplicate event_ids idempotently.
   *
   * @param {Object} event - The enriched event to archive
   * @returns {void}
   */
  archiveEvent(event) {
    if (!config.PERSIST) return;
    try {
      if (!this._eventStmt) {
        const { getDatabase } = require('../store/database');
        this._eventStmt = getDatabase().prepare(
          `INSERT OR IGNORE INTO events_archive
           (event_id, customer_id, channel, event_type, classification_domain, classification_intent, data, timestamp)
           VALUES (@event_id, @customer_id, @channel, @event_type, @classification_domain, @classification_intent, @data, @timestamp)`
        );
      }
      this._eventStmt.run({
        event_id: event.event_id,
        customer_id: event.identity?.unified_customer_id || event.customer_id,
        channel: event.channel,
        event_type: event.event_type,
        classification_domain: event.classification?.primary?.domain || null,
        classification_intent: event.classification?.primary?.fullKey || null,
        data: JSON.stringify({
          content: (event.payload?.content || '').substring(0, 500),
          classification: event.classification?.primary || null,
          analysis: event.analysis ? {
            sentiment: event.analysis.sentiment,
            urgency: event.analysis.urgency
          } : null,
          routing: event.routing ? {
            priority: event.routing.priority,
            sla_status: event.routing.sla_status
          } : null
        }),
        timestamp: event.timestamp
      });
    } catch {}
  }

  /**
   * Start a 30-second interval that records process memory usage as gauges.
   * Logs a warning when heap usage exceeds 500MB. In production, these metrics
   * would be emitted to CloudWatch via the embedded metrics format.
   *
   * @returns {void}
   */
  startMemoryMonitor() {
    this._memoryInterval = setInterval(() => {
      const mem = process.memoryUsage();
      this.setGauge('memory_rss_mb', Math.round(mem.rss / 1024 / 1024));
      this.setGauge('memory_heap_used_mb', Math.round(mem.heapUsed / 1024 / 1024));
      this.setGauge('memory_heap_total_mb', Math.round(mem.heapTotal / 1024 / 1024));
      if (mem.heapUsed > 500 * 1024 * 1024) {
        log.warn({ heapMB: Math.round(mem.heapUsed / 1024 / 1024) }, 'High memory usage');
      }
    }, 30000);
  }

  /**
   * Gracefully shut down the metrics engine. Clears all intervals and writes
   * a final snapshot to ensure no counter data is lost.
   *
   * @returns {void}
   */
  destroy() {
    if (this._snapshotInterval) {
      clearInterval(this._snapshotInterval);
      this._snapshotInterval = null;
    }
    if (this._memoryInterval) {
      clearInterval(this._memoryInterval);
      this._memoryInterval = null;
    }
    this._saveSnapshot();
  }

  /**
   * Initialize the named sliding windows used for rate calculations.
   * Each window maintains a timestamp array that is pruned on every insertion.
   *
   * Windows: events_1m, events_5m, classifications_1m, nba_actions_5m, journey_transitions_5m
   *
   * @returns {void}
   * @private
   */
  _initWindows() {
    const windowDefs = [
      { name: 'events_1m', metric: 'event_count', windowMs: 60000 },
      { name: 'events_5m', metric: 'event_count', windowMs: 300000 },
      { name: 'classifications_1m', metric: 'classification_count', windowMs: 60000 },
      { name: 'nba_actions_5m', metric: 'nba_action_count', windowMs: 300000 },
      { name: 'journey_transitions_5m', metric: 'journey_transition_count', windowMs: 300000 }
    ];

    for (const def of windowDefs) {
      this.windows.set(def.name, {
        ...def,
        entries: []
      });
    }
  }

  /**
   * Record a fully-enriched event into the metrics engine. Extracts a normalized
   * entry from the event's routing, classification, analysis, and identity data,
   * then updates counters, sliding windows, and gauges.
   *
   * The time-series buffer is bounded by `maxSeriesLength`; oldest entries are
   * evicted when the buffer overflows.
   *
   * @param {Object} event - The enriched event from the pipeline
   * @returns {void}
   */
  recordEvent(event) {
    const now = Date.now();
    const routing = event.routing || event.nba?.routing || {};
    const entry = {
      timestamp: now,
      channel: event.channel,
      event_type: event.event_type,
      has_classification: !!event.classification,
      classification_domain: event.classification?.primary?.domain || null,
      classification_intent: event.classification?.primary?.fullKey || null,
      customer_id: event.identity?.unified_customer_id || event.customer_id,
      sentiment: event.analysis?.sentiment || event.payload?.metadata?.sentiment_hint || 'unknown',
      urgency: event.analysis?.urgency || 'unknown',
      priority: routing.priority || 0,
      sla_status: routing.sla_status || 'unknown',
      estimated_wait_time: Number(routing.estimated_wait_time || 0),
      skill_match_score: Number(routing.skill_match_score || 0),
      contact_value: Number(routing.contact_value || 0),
      deflected: routing.should_deflect === true || hasDeflectionAction(event.nba?.actions),
      journey_type: event.journey?.journeys?.[0]?.journey_type || null
    };

    this.timeSeries.push(entry);
    if (this.timeSeries.length > this.maxSeriesLength) {
      this.timeSeries = this.timeSeries.slice(-this.maxSeriesLength);
    }

    this._increment('total_events');
    this._increment('events_by_channel.' + event.channel);
    this._increment('events_by_type.' + event.event_type);

    if (event.classification) {
      this._increment('total_classifications');
      this._increment('classifications_by_domain.' + event.classification.primary.domain);
      this._incrementInMap('classifications_by_intent', event.classification.primary.fullKey);
    }

    if (entry.sentiment !== 'unknown') this._increment('sentiment_distribution.' + entry.sentiment);
    if (entry.urgency !== 'unknown') this._increment('urgency_distribution.' + entry.urgency);
    if (entry.sla_status !== 'unknown') this._increment('sla_status.' + entry.sla_status);
    if (entry.priority >= 8) this._increment('high_priority_contacts');
    if (entry.deflected) this._increment('deflections');

    this._addToWindow('events_1m', now);
    this._addToWindow('events_5m', now);
    if (event.classification) this._addToWindow('classifications_1m', now);

    this.setGauge('last_priority', entry.priority);
    this.setGauge('last_contact_value', entry.contact_value);
    this.setGauge('last_sla_status', entry.sla_status);
  }

  /**
   * Record a journey state transition in counters and the journey sliding window.
   * Tracks new journey creation, completion, and cross-channel handoffs separately.
   *
   * @param {Object} transition - Transition record from the journey engine
   * @param {string} transition.journey_type - Journey type identifier
   * @param {boolean} [transition.isNew] - Whether this created a new journey
   * @param {boolean} [transition.isTerminal] - Whether this completed a journey
   * @param {boolean} [transition.handoffOccurred] - Whether a cross-channel handoff occurred
   * @returns {void}
   */
  recordJourneyTransition(transition) {
    this._increment('total_journey_transitions');
    this._increment('transitions_by_type.' + transition.journey_type);
    if (transition.isNew) this._increment('new_journeys_created');
    if (transition.isTerminal) this._increment('journeys_completed');
    if (transition.handoffOccurred) this._increment('cross_channel_handoffs');
    this._addToWindow('journey_transitions_5m', Date.now());
  }

  /**
   * Record an NBA action in counters and the NBA sliding window.
   * Self-service redirects and digital deflections are counted separately
   * for deflection rate reporting.
   *
   * @param {Object} action - Action record from the NBA engine
   * @param {string} action.type - Action type identifier
   * @param {string} action.urgency - Action urgency level
   * @returns {void}
   */
  recordNBAAction(action) {
    this._increment('total_nba_actions');
    this._increment('nba_by_type.' + action.type);
    this._increment('nba_by_urgency.' + action.urgency);
    if (['self_service_redirect', 'digital_deflection'].includes(action.type)) {
      this._increment('deflection_actions');
    }
    this._addToWindow('nba_actions_5m', Date.now());
  }

  /**
   * Increment a nested counter by dot-separated key path. Creates intermediate
   * objects as needed. For example, 'events_by_channel.voice' navigates to
   * `this.counters.events_by_channel` and increments `.voice`.
   *
   * @param {string} key - Dot-separated counter path
   * @returns {void}
   * @private
   */
  _increment(key) {
    const parts = key.split('.');
    let obj = this.counters;
    for (let i = 0; i < parts.length - 1; i++) {
      if (!obj[parts[i]]) obj[parts[i]] = {};
      obj = obj[parts[i]];
    }
    const lastKey = parts[parts.length - 1];
    obj[lastKey] = (obj[lastKey] || 0) + 1;
  }

  /**
   * Increment a counter within a flat map bucket (single level of nesting).
   * Used for intent distributions where the key itself contains dots.
   *
   * @param {string} bucket - Top-level counter group name
   * @param {string} key - Key within the bucket to increment
   * @returns {void}
   * @private
   */
  _incrementInMap(bucket, key) {
    if (!this.counters[bucket]) this.counters[bucket] = {};
    this.counters[bucket][key] = (this.counters[bucket][key] || 0) + 1;
  }

  /**
   * Add a timestamp to a named sliding window and prune entries outside the window.
   *
   * @param {string} windowName - Name of the sliding window (e.g., 'events_1m')
   * @param {number} timestamp - Unix timestamp in milliseconds
   * @returns {void}
   * @private
   */
  _addToWindow(windowName, timestamp) {
    const win = this.windows.get(windowName);
    if (!win) return;
    win.entries.push(timestamp);
    const cutoff = timestamp - win.windowMs;
    win.entries = win.entries.filter(time => time > cutoff);
  }

  /**
   * Get the current count of entries within a named sliding window,
   * pruning expired entries relative to the current time.
   *
   * @param {string} windowName - Name of the sliding window
   * @returns {number} Number of entries within the active window
   */
  getWindowCount(windowName) {
    const win = this.windows.get(windowName);
    if (!win) return 0;
    const now = Date.now();
    const cutoff = now - win.windowMs;
    return win.entries.filter(time => time > cutoff).length;
  }

  /**
   * Set a point-in-time gauge value. Gauges represent current state (not cumulative)
   * and are overwritten on each call.
   *
   * @param {string} key - Gauge identifier
   * @param {number|string} value - Current gauge value
   * @returns {void}
   */
  setGauge(key, value) {
    this.gauges[key] = value;
  }

  /**
   * Compute current KPI snapshot from the in-memory time-series buffer.
   * Includes SLA compliance rate, average wait time, deflection rate,
   * average contact value, sentiment/urgency distributions, and high-priority count.
   *
   * @returns {{ total_contacts: number, contacts_by_channel: Object, sla_compliance_rate: number, avg_wait_time: number, deflection_rate: number, avg_contact_value: number, sentiment_distribution: Object, urgency_distribution: Object, high_priority_contacts: number }}
   */
  getKPIs() {
    const contacts = this.timeSeries;
    const totalContacts = contacts.length;
    const slaKnown = contacts.filter(entry => entry.sla_status !== 'unknown');
    const slaCompliant = slaKnown.filter(entry => entry.sla_status === 'compliant').length;
    const waitTimes = contacts.map(entry => entry.estimated_wait_time).filter(value => Number.isFinite(value));
    const contactValues = contacts.map(entry => entry.contact_value).filter(value => Number.isFinite(value));
    const deflectedCount = contacts.filter(entry => entry.deflected).length;

    return {
      total_contacts: totalContacts,
      contacts_by_channel: this._groupBy(contacts, 'channel'),
      sla_compliance_rate: percent(slaCompliant, slaKnown.length),
      avg_wait_time: average(waitTimes),
      deflection_rate: percent(deflectedCount, totalContacts),
      avg_contact_value: average(contactValues),
      sentiment_distribution: this._groupBy(contacts, 'sentiment'),
      urgency_distribution: this._groupBy(contacts, 'urgency'),
      high_priority_contacts: contacts.filter(entry => entry.priority >= 8).length
    };
  }

  /**
   * Build an operational summary from the most recent 100 time-series entries.
   * Includes unique active customers, SLA distribution, top intents, top channels,
   * deflection count, and cross-channel handoff count.
   *
   * @returns {{ recent_active_customers: number, sla_status: Object, top_intents: Array<{key: string, count: number}>, top_channels: Array<{key: string, count: number}>, deflections: number, cross_channel_handoffs: number }}
   */
  getOperationalSummary() {
    const recent = this.timeSeries.slice(-100);
    const activeCustomers = new Set(recent.map(entry => entry.customer_id));
    const flattenedIntents = this.counters.classifications_by_intent || {};

    return {
      recent_active_customers: activeCustomers.size,
      sla_status: this.counters.sla_status || {},
      top_intents: topCounts(flattenedIntents, 5),
      top_channels: topCounts(this.counters.events_by_channel || {}, 5),
      deflections: this.counters.deflections || 0,
      cross_channel_handoffs: this.counters.cross_channel_handoffs || 0
    };
  }

  /**
   * Build the full dashboard projection combining counters, gauges, sliding window
   * rates, KPIs, operational summary, channel/classification/intent distributions,
   * and the 20 most recent events. This is the primary payload for the WebSocket
   * dashboard broadcast and the GET /api/dashboard endpoint.
   *
   * @returns {Object} Complete dashboard state
   */
  getDashboard() {
    return {
      timestamp: new Date().toISOString(),
      counters: { ...this.counters },
      gauges: { ...this.gauges },
      windows: {
        events_per_minute: this.getWindowCount('events_1m'),
        events_per_5min: this.getWindowCount('events_5m'),
        classifications_per_minute: this.getWindowCount('classifications_1m'),
        nba_actions_per_5min: this.getWindowCount('nba_actions_5m'),
        journey_transitions_per_5min: this.getWindowCount('journey_transitions_5m')
      },
      kpis: this.getKPIs(),
      operationalSummary: this.getOperationalSummary(),
      channelDistribution: this.counters.events_by_channel || {},
      classificationDistribution: this.counters.classifications_by_domain || {},
      intentDistribution: this.counters.classifications_by_intent || {},
      recentEvents: this.timeSeries.slice(-20).reverse()
    };
  }

  /**
   * Query time-series entries within a time range and aggregate them into
   * fixed-width buckets. Each bucket contains event count, channel breakdown,
   * domain breakdown, and average priority/contact value.
   *
   * Defaults to the last hour with 1-minute buckets if no parameters are provided.
   *
   * @param {Object} [options] - Query options
   * @param {number} [options.fromMs] - Start of range (Unix ms). Defaults to 1 hour ago.
   * @param {number} [options.toMs] - End of range (Unix ms). Defaults to now.
   * @param {number} [options.bucketMs=60000] - Bucket width in milliseconds
   * @returns {Array<{ timestamp: string, count: number, byChannel: Object, byDomain: Object, avgPriority: number, avgContactValue: number }>}
   */
  getTimeSeries({ fromMs, toMs, bucketMs = 60000 } = {}) {
    const from = fromMs || Date.now() - 3600000;
    const to = toMs || Date.now();
    const filtered = this.timeSeries.filter(entry => entry.timestamp >= from && entry.timestamp <= to);

    const buckets = new Map();
    for (const entry of filtered) {
      const bucketKey = Math.floor(entry.timestamp / bucketMs) * bucketMs;
      if (!buckets.has(bucketKey)) buckets.set(bucketKey, []);
      buckets.get(bucketKey).push(entry);
    }

    return [...buckets.entries()]
      .sort((a, b) => a[0] - b[0])
      .map(([timestamp, entries]) => ({
        timestamp: new Date(timestamp).toISOString(),
        count: entries.length,
        byChannel: this._groupBy(entries, 'channel'),
        byDomain: this._groupBy(entries, 'classification_domain'),
        avgPriority: average(entries.map(entry => entry.priority)),
        avgContactValue: average(entries.map(entry => entry.contact_value))
      }));
  }

  /**
   * Group an array of objects by a key and return counts per group.
   * Missing or falsy values are counted under 'unknown'.
   *
   * @param {Object[]} arr - Array of metric entries
   * @param {string} key - Property name to group by
   * @returns {Object.<string, number>} Map of group values to counts
   * @private
   */
  _groupBy(arr, key) {
    const groups = {};
    for (const item of arr) {
      const value = item[key] || 'unknown';
      groups[value] = (groups[value] || 0) + 1;
    }
    return groups;
  }
}

/**
 * Compute the arithmetic mean of an array of numeric values.
 * Non-finite values are filtered out. Returns 0 for empty arrays.
 *
 * @param {number[]} values - Array of numbers
 * @returns {number} Arithmetic mean rounded to 2 decimal places
 */
function average(values) {
  const filtered = values.filter(value => Number.isFinite(value));
  if (filtered.length === 0) return 0;
  return round(filtered.reduce((sum, value) => sum + value, 0) / filtered.length);
}

/**
 * Compute a percentage, returning 0 when the denominator is zero.
 *
 * @param {number} value - Numerator
 * @param {number} total - Denominator
 * @returns {number} Percentage rounded to 2 decimal places
 */
function percent(value, total) {
  if (!total) return 0;
  return round((value / total) * 100);
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
 * Check whether an array of NBA actions contains a deflection action.
 * Used to determine if an event was deflected to self-service.
 *
 * @param {Object[]} actions - Array of NBA action objects
 * @returns {boolean} True if any action is a self_service_redirect or digital_deflection
 */
function hasDeflectionAction(actions) {
  return Array.isArray(actions) && actions.some(action =>
    ['self_service_redirect', 'digital_deflection'].includes(action.type)
  );
}

/**
 * Extract the top N entries from a count map, sorted by count descending.
 *
 * @param {Object.<string, number>} map - Map of keys to counts
 * @param {number} limit - Maximum number of entries to return
 * @returns {Array<{ key: string, count: number }>} Top entries sorted by count
 */
function topCounts(map, limit) {
  return Object.entries(map)
    .sort((a, b) => b[1] - a[1])
    .slice(0, limit)
    .map(([key, count]) => ({ key, count }));
}

/**
 * Module-level singleton accessor for the MetricsEngine.
 * Creates a new instance on first call; returns the same instance thereafter.
 *
 * @returns {MetricsEngine} The singleton metrics engine instance
 */
let instance = null;
function getMetrics() {
  if (!instance) instance = new MetricsEngine();
  return instance;
}

module.exports = { MetricsEngine, getMetrics };
