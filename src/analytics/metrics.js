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

class MetricsEngine {
  constructor() {
    this.timeSeries = [];
    this.counters = {};
    this.gauges = {};
    this.windows = new Map();
    this.maxSeriesLength = config.METRICS_SERIES_SIZE;
    this._snapshotStmt = null;
    this._eventStmt = null;
    this._snapshotInterval = null;
    this._initWindows();
  }

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

  recordJourneyTransition(transition) {
    this._increment('total_journey_transitions');
    this._increment('transitions_by_type.' + transition.journey_type);
    if (transition.isNew) this._increment('new_journeys_created');
    if (transition.isTerminal) this._increment('journeys_completed');
    if (transition.handoffOccurred) this._increment('cross_channel_handoffs');
    this._addToWindow('journey_transitions_5m', Date.now());
  }

  recordNBAAction(action) {
    this._increment('total_nba_actions');
    this._increment('nba_by_type.' + action.type);
    this._increment('nba_by_urgency.' + action.urgency);
    if (['self_service_redirect', 'digital_deflection'].includes(action.type)) {
      this._increment('deflection_actions');
    }
    this._addToWindow('nba_actions_5m', Date.now());
  }

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

  _incrementInMap(bucket, key) {
    if (!this.counters[bucket]) this.counters[bucket] = {};
    this.counters[bucket][key] = (this.counters[bucket][key] || 0) + 1;
  }

  _addToWindow(windowName, timestamp) {
    const win = this.windows.get(windowName);
    if (!win) return;
    win.entries.push(timestamp);
    const cutoff = timestamp - win.windowMs;
    win.entries = win.entries.filter(time => time > cutoff);
  }

  getWindowCount(windowName) {
    const win = this.windows.get(windowName);
    if (!win) return 0;
    const now = Date.now();
    const cutoff = now - win.windowMs;
    return win.entries.filter(time => time > cutoff).length;
  }

  setGauge(key, value) {
    this.gauges[key] = value;
  }

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

  _groupBy(arr, key) {
    const groups = {};
    for (const item of arr) {
      const value = item[key] || 'unknown';
      groups[value] = (groups[value] || 0) + 1;
    }
    return groups;
  }
}

function average(values) {
  const filtered = values.filter(value => Number.isFinite(value));
  if (filtered.length === 0) return 0;
  return round(filtered.reduce((sum, value) => sum + value, 0) / filtered.length);
}

function percent(value, total) {
  if (!total) return 0;
  return round((value / total) * 100);
}

function round(value) {
  return Math.round(value * 100) / 100;
}

function hasDeflectionAction(actions) {
  return Array.isArray(actions) && actions.some(action =>
    ['self_service_redirect', 'digital_deflection'].includes(action.type)
  );
}

function topCounts(map, limit) {
  return Object.entries(map)
    .sort((a, b) => b[1] - a[1])
    .slice(0, limit)
    .map(([key, count]) => ({ key, count }));
}

// Singleton
let instance = null;
function getMetrics() {
  if (!instance) instance = new MetricsEngine();
  return instance;
}

module.exports = { MetricsEngine, getMetrics };
