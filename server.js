/**
 * @module nexus-cx/server
 * @description HTTP API server and WebSocket gateway for the Nexus CX Intelligence Platform.
 *
 * Provides:
 *   - Express REST API (event ingestion, classification, dashboards, customer/journey/NBA data)
 *   - WebSocket server for real-time dashboard streaming (metrics every 2s, live events, LLM enrichment)
 *   - Middleware stack: helmet (security headers), express-rate-limit (200 req/min), request IDs, request logging
 *   - Deep health check at /api/health/ready (pipeline, classifier, Ollama, SQLite, memory)
 *   - Graceful shutdown (SIGTERM/SIGINT): stops HTTP, closes WebSockets, clears intervals, flushes SQLite
 *
 * Boot sequence (when PERSIST=true):
 *   1. Config → 2. Database init (migrations) → 3. Hydrate resolver → 4. Hydrate journey engine
 *   → 5. Hydrate NBA → 6. Hydrate metrics → 7. Build classifier → 8. Listen
 *
 * @requires express
 * @requires ws
 * @requires helmet
 * @requires express-rate-limit
 * @requires uuid
 * @requires ./src/config
 * @requires ./src/pipeline
 *
 * @see {@link module:nexus-cx/pipeline} Pipeline orchestrator
 * @see {@link module:nexus-cx/store/database} SQLite persistence layer
 */
const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const cors = require('cors');
const path = require('path');

const config = require('./src/config');
const { createLogger } = require('./src/logger');
const { createEvent, CHANNELS } = require('./src/events/schema');
const { getPipeline } = require('./src/pipeline');
const { getClassifier } = require('./src/classifier/classifier');
const { getResolver } = require('./src/identity/resolver');
const { getJourneyEngine } = require('./src/journey/state-machine');
const { getNBAEngine } = require('./src/nba/engine');
const { getMetrics } = require('./src/analytics/metrics');
const { getFabric } = require('./src/events/fabric');
const { TAXONOMY } = require('./src/classifier/taxonomy');
const { JOURNEY_DEFINITIONS } = require('./src/journey/definitions');
const { initDatabase, closeDatabase, getDatabaseStats } = require('./src/store/database');

const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const { v4: uuidv4 } = require('uuid');

const log = createLogger('server');
const PORT = config.PORT;

const app = express();

// ---------------------------------------------------------------
// Middleware Stack
// ---------------------------------------------------------------
// Order matters: security headers first, then rate limiting, then
// CORS, body parsing, request tracing, and finally static assets.
// This ensures all requests are protected before any route logic
// executes, and that request IDs are available for downstream logging.
// ---------------------------------------------------------------

// Security headers via Helmet. CSP is disabled to allow inline scripts
// in the dashboard UI without requiring a build step or nonce injection.
app.use(helmet({
  contentSecurityPolicy: false
}));

// Rate limiting scoped to /api/ routes only. Dashboard static assets
// and WebSocket upgrades are exempt to avoid degrading the UI experience.
app.use('/api/', rateLimit({
  windowMs: config.RATE_LIMIT_WINDOW_MS,
  max: config.RATE_LIMIT_MAX,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Rate limit exceeded', retryAfterMs: config.RATE_LIMIT_WINDOW_MS }
}));

app.use(cors());
app.use(express.json({ limit: '100kb' }));

// Request tracing middleware: assigns a UUID to every request and logs
// latency on completion. Health checks are excluded from logging to
// reduce noise from load balancer probes.
app.use((req, res, next) => {
  req.id = uuidv4();
  res.setHeader('X-Request-ID', req.id);
  const start = Date.now();
  res.on('finish', () => {
    const latency = Date.now() - start;
    if (req.path !== '/api/health' && latency > 0) {
      log.info({ requestId: req.id, method: req.method, path: req.path, status: res.statusCode, latencyMs: latency }, 'request');
    }
  });
  next();
});

// Serve the single-page ops dashboard from the ui/ directory
app.use(express.static(path.join(__dirname, 'ui')));

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// Pipeline and classifier are initialized asynchronously in boot().
// They remain null until boot() completes, which prevents route handlers
// from executing against uninitialized state.
let pipeline = null;
let classifier = null;
let metricsInterval = null;

// ===============================================================
// WebSocket -- real-time event streaming to dashboard clients
// ===============================================================
// The dashboard UI connects via WebSocket to receive live event
// updates, enrichment notifications, and periodic metrics snapshots.
// On initial connection, the current pipeline status is sent so the
// UI can render without waiting for the first event.
// ===============================================================

/** @type {Set<WebSocket>} Active WebSocket client connections */
const wsClients = new Set();

wss.on('connection', (ws) => {
  wsClients.add(ws);
  log.debug({ clients: wsClients.size }, 'WebSocket client connected');

  // Send current pipeline state immediately so the dashboard
  // can render the latest status without waiting for a new event
  if (pipeline) {
    ws.send(JSON.stringify({
      type: 'init',
      data: pipeline.getStatus()
    }));
  }

  ws.on('close', () => {
    wsClients.delete(ws);
  });
});

/**
 * Broadcasts a typed message to all connected WebSocket clients.
 * Silently skips clients whose connections are not in the OPEN state
 * (readyState === 1) to avoid write-after-close errors.
 *
 * @param {string} type - Message type identifier (e.g., 'event', 'metrics', 'llm_enrichment')
 * @param {Object} data - Payload to serialize and send
 */
function broadcast(type, data) {
  const msg = JSON.stringify({ type, data, timestamp: new Date().toISOString() });
  for (const ws of wsClients) {
    if (ws.readyState === 1) {
      ws.send(msg);
    }
  }
}

// Subscribe to Event Fabric's 'enriched' event to relay fully-processed
// events to the dashboard in real time. The payload is trimmed to only
// the fields the UI needs, keeping WebSocket frame sizes small.
const fabric = getFabric();
fabric.on('enriched', (event) => {
  broadcast('event', {
    event_id: event.event_id,
    unified_customer_id: event.identity?.unified_customer_id || event.customer_id,
    customer_id: event.customer_id,
    channel: event.channel,
    event_type: event.event_type,
    content: event.payload?.content?.substring(0, 200),
    classification: event.classification?.primary || null,
    analysis: event.analysis ? {
      sentiment: event.analysis.sentiment,
      urgency: event.analysis.urgency,
      summary: event.analysis.summary
    } : null,
    journey: event.journey?.transitions || [],
    routing: event.routing || null,
    nba: event.nba?.actions?.map(a => ({
      type: a.type,
      urgency: a.urgency,
      content: a.content
    })) || [],
    timestamp: event.timestamp
  });
});

// Relay LLM enrichment results (when Ollama is available) so the
// dashboard can display AI-generated analysis alongside deterministic results
fabric.on('llm_enrichment', (enrichment) => {
  broadcast('llm_enrichment', enrichment);
});

// Periodic metrics broadcast: every 2 seconds, push aggregated dashboard
// data including latency stats and circuit breaker state. This keeps the
// UI's KPI cards and charts current even when no new events are flowing.
metricsInterval = setInterval(() => {
  const dashboardData = getMetrics().getDashboard();
  if (pipeline) {
    dashboardData.latency = pipeline.getLatencyStats();
    const { getCircuitState } = require('./src/classifier/llm-client');
    dashboardData.circuitBreaker = getCircuitState();
  }
  broadcast('metrics', dashboardData);
}, 2000);

// ===============================================================
// REST API Endpoints
// ===============================================================
// Organized by domain: health, event ingestion, classification,
// analysis, customers, journeys, NBA, analytics/dashboard, export,
// streaming (SSE), and simulation helpers.
// ===============================================================

// --- Health & Readiness ----------------------------------------

/**
 * GET /api/health
 * Lightweight liveness probe. Returns immediately with uptime.
 * Suitable for load balancer health checks at high frequency.
 */
app.get('/api/health', (req, res) => {
  res.json({ status: 'healthy', uptime: process.uptime(), port: PORT });
});

/**
 * GET /api/health/ready
 * Deep readiness check. Validates pipeline, classifier, persistence,
 * memory headroom, and Ollama connectivity. Returns 503 with degraded/unhealthy
 * status if any critical subsystem is down. The Ollama check uses a 2-second
 * timeout to avoid blocking the probe when the model server is unreachable.
 */
app.get('/api/health/ready', async (req, res) => {
  const mem = process.memoryUsage();
  const checks = {
    pipeline: !!pipeline?.isRunning,
    classifier: !!(classifier && classifier.getStats().built),
    persistence: config.PERSIST ? !!getDatabaseStats() : true,
    memory: mem.heapUsed < 500 * 1024 * 1024
  };

  // Ollama check (non-blocking, 2s timeout)
  try {
    const { checkModels } = require('./src/classifier/llm-client');
    const models = await checkModels();
    checks.ollama = models.embeddings;
  } catch {
    checks.ollama = false;
  }

  const allPassing = Object.values(checks).every(Boolean);
  const status = allPassing ? 'ready' : (checks.pipeline ? 'degraded' : 'unhealthy');

  res.status(allPassing ? 200 : 503).json({
    status,
    checks,
    uptime: Math.round(process.uptime()),
    memoryMB: Math.round(mem.heapUsed / 1024 / 1024),
    database: config.PERSIST ? getDatabaseStats() : null
  });
});

// --- Event Ingestion -------------------------------------------

/**
 * POST /api/events
 * Ingests a single customer interaction event into the pipeline.
 * Validates input constraints (content length, customerId format,
 * metadata key count) before creating a canonical event and feeding
 * it through all five processing layers.
 *
 * @param {Object} req.body
 * @param {string} [req.body.customerId] - Customer identifier (auto-generated if omitted)
 * @param {string} [req.body.sessionId] - Session identifier for grouping interactions
 * @param {string} [req.body.channel='web'] - Channel: voice, chat, web, mobile, email, sms
 * @param {string} [req.body.eventType='interaction'] - Event type classification
 * @param {string} [req.body.content=''] - Interaction text content (max 10,000 chars)
 * @param {Object} [req.body.metadata={}] - Arbitrary metadata (max 20 keys)
 * @param {Object} [req.body.context={}] - Contextual data (e.g., page URL, referrer)
 * @returns {{ success: boolean, event: Object }}
 */
app.post('/api/events', async (req, res) => {
  try {
    const { customerId, sessionId, channel, eventType, content, metadata, context } = req.body;

    // Input validation -- reject early before pipeline processing
    if (content && typeof content !== 'string') {
      return res.status(422).json({ error: 'content must be a string' });
    }
    if (content && content.length > 10000) {
      return res.status(422).json({ error: 'content exceeds 10000 character limit' });
    }
    if (customerId && (typeof customerId !== 'string' || customerId.length > 128)) {
      return res.status(422).json({ error: 'customerId must be a string under 128 characters' });
    }
    if (metadata && typeof metadata === 'object' && Object.keys(metadata).length > 20) {
      return res.status(422).json({ error: 'metadata exceeds 20 key limit' });
    }

    const event = createEvent({
      customerId: customerId || 'cust-' + Math.random().toString(36).substring(2, 8),
      sessionId,
      channel: channel || 'web',
      eventType: eventType || 'interaction',
      payload: { content: content || '', metadata: metadata || {} },
      context: context || {}
    });
    await pipeline.ingest(event);
    res.json({ success: true, event });
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

/**
 * POST /api/events/batch
 * Ingests multiple events sequentially. Each event is processed
 * independently -- failures in one event do not abort the batch.
 * Returns per-event success/failure results for the caller to handle.
 *
 * @param {Object} req.body
 * @param {Array<Object>} req.body.events - Array of event payloads (same shape as POST /api/events)
 * @returns {{ processed: number, results: Array<{ success: boolean, event_id?: string, error?: string }> }}
 */
app.post('/api/events/batch', async (req, res) => {
  const { events } = req.body;
  if (!Array.isArray(events)) return res.status(400).json({ error: 'events must be an array' });

  const results = [];
  for (const e of events) {
    try {
      const event = createEvent({
        customerId: e.customerId,
        sessionId: e.sessionId,
        channel: e.channel || 'web',
        eventType: e.eventType || 'interaction',
        payload: { content: e.content || '', metadata: e.metadata || {} },
        context: e.context || {}
      });
      await pipeline.ingest(event);
      results.push({ success: true, event_id: event.event_id });
    } catch (err) {
      results.push({ success: false, error: err.message });
    }
  }
  res.json({ processed: results.length, results });
});

// --- Classification --------------------------------------------

/**
 * POST /api/classify
 * Classifies freeform text using the embedding-based (or TF-IDF fallback)
 * classifier. Returns primary intent, domain, confidence, and ranked alternatives.
 *
 * @param {Object} req.body
 * @param {string} req.body.text - Text to classify
 * @returns {Object} Classification result with primary and alternatives
 */
app.post('/api/classify', async (req, res) => {
  const { text } = req.body;
  if (!text) return res.status(400).json({ error: 'text is required' });
  const result = await classifier.classify(text);
  res.json(result);
});

/**
 * POST /api/classify/llm
 * Comparison endpoint that runs both embedding-based and LLM-based
 * classification in parallel, plus full interaction analysis.
 * Useful for evaluating classifier accuracy and LLM enrichment quality.
 *
 * @param {Object} req.body
 * @param {string} req.body.text - Text to classify and analyze
 * @returns {{ embedding: Object, llm: Object, analysis: Object }}
 */
app.post('/api/classify/llm', async (req, res) => {
  const { text } = req.body;
  if (!text) return res.status(400).json({ error: 'text is required' });
  const embedding = await classifier.classify(text);
  const llm = await classifier.classifyWithLLM(text);
  const analysis = await classifier.analyzeInteraction(text);
  res.json({ embedding, llm, analysis });
});

// --- LLM Analysis Results --------------------------------------

/**
 * GET /api/analysis
 * Returns recent LLM-enriched analysis results stored in the pipeline's
 * in-memory buffer. Includes classifier stats for monitoring model health.
 *
 * @param {number} [req.query.limit=20] - Maximum number of results to return
 */
app.get('/api/analysis', (req, res) => {
  const limit = parseInt(req.query.limit) || 20;
  const analyses = pipeline.getRecentAnalyses(limit);
  res.json({ count: analyses.length, analyses, classifierStats: classifier.getStats() });
});

/**
 * GET /api/analysis/:eventId
 * Retrieves the LLM analysis for a specific event by its UUID.
 */
app.get('/api/analysis/:eventId', (req, res) => {
  const analysis = pipeline.getAnalysis(req.params.eventId);
  if (!analysis) return res.status(404).json({ error: 'No analysis found for this event' });
  res.json(analysis);
});

/**
 * GET /api/taxonomy
 * Returns the full intent taxonomy tree: domains, intents, and exemplar phrases.
 * Useful for UI dropdowns and classifier evaluation tooling.
 */
app.get('/api/taxonomy', (req, res) => {
  res.json(TAXONOMY);
});

// --- Customer Profiles -----------------------------------------

/**
 * GET /api/customers
 * Lists all unified customer profiles. Each profile aggregates identity
 * data across channels via the identity resolver's alias stitching.
 */
app.get('/api/customers', (req, res) => {
  const profiles = getResolver().getAllProfiles();
  res.json({ count: profiles.length, profiles });
});

/**
 * GET /api/customers/:id
 * Returns a single customer's unified profile, active journeys,
 * and journey intelligence (common paths, stuck journeys, triggers).
 * Accepts either the canonical customer_id or any known alias.
 */
app.get('/api/customers/:id', (req, res) => {
  const profile = getResolver().getProfile(req.params.id);
  if (!profile) return res.status(404).json({ error: 'Customer not found' });

  const journeys = getJourneyEngine().getAllJourneys(profile.customer_id);
  const intelligence = getJourneyEngine().getJourneyIntelligence({ customerId: profile.customer_id });
  res.json({ profile, journeys, intelligence });
});

// --- Journeys --------------------------------------------------

/**
 * GET /api/journeys
 * Returns aggregate journey statistics and cross-customer intelligence.
 */
app.get('/api/journeys', (req, res) => {
  const stats = getJourneyEngine().getStats();
  res.json({
    ...stats,
    intelligence: getJourneyEngine().getJourneyIntelligence()
  });
});

/**
 * GET /api/journeys/definitions
 * Returns the full set of journey type definitions (states, transitions,
 * triggers, and completion criteria) used by the state machine engine.
 */
app.get('/api/journeys/definitions', (req, res) => {
  res.json(JOURNEY_DEFINITIONS);
});

/**
 * GET /api/journeys/transitions
 * Returns recent state transitions across all journeys, ordered
 * newest first. Useful for the dashboard's journey activity feed.
 *
 * @param {number} [req.query.limit=50] - Maximum transitions to return
 */
app.get('/api/journeys/transitions', (req, res) => {
  const limit = parseInt(req.query.limit) || 50;
  const transitions = getJourneyEngine().getRecentTransitions(limit);
  res.json({ count: transitions.length, transitions });
});

/**
 * GET /api/journeys/intelligence
 * Returns journey intelligence: common paths, stuck journeys,
 * cross-channel handoff rates, and top journey triggers.
 *
 * @param {number} [req.query.limit=10] - Maximum results per intelligence category
 */
app.get('/api/journeys/intelligence', (req, res) => {
  const limit = parseInt(req.query.limit) || 10;
  res.json(getJourneyEngine().getJourneyIntelligence({ limit }));
});

/**
 * GET /api/journeys/customer/:id
 * Returns all journeys (active and completed) for a specific customer,
 * plus customer-scoped journey intelligence.
 */
app.get('/api/journeys/customer/:id', (req, res) => {
  const profile = getResolver().getProfile(req.params.id);
  const customerId = profile?.customer_id || req.params.id;
  const journeys = getJourneyEngine().getAllJourneys(customerId);
  res.json({
    count: journeys.length,
    journeys,
    intelligence: getJourneyEngine().getJourneyIntelligence({ customerId })
  });
});

// --- Next-Best-Action (NBA) ------------------------------------

/**
 * GET /api/nba/stats
 * Returns NBA engine statistics: evaluation counts, action distribution,
 * deflection rates, and rule hit frequencies.
 */
app.get('/api/nba/stats', (req, res) => {
  res.json(getNBAEngine().getStats());
});

/**
 * GET /api/nba/actions
 * Returns the recent NBA action log -- actions recommended to agents
 * or triggered as automated responses.
 *
 * @param {number} [req.query.limit=50] - Maximum actions to return
 */
app.get('/api/nba/actions', (req, res) => {
  const limit = parseInt(req.query.limit) || 50;
  const actions = getNBAEngine().getActionLog(limit);
  res.json({ count: actions.length, actions });
});

// --- Analytics / Dashboard -------------------------------------

/**
 * GET /api/dashboard
 * Composite endpoint returning all data the ops dashboard needs in a
 * single request: metrics counters, distributions, journey intelligence,
 * identity stats, NBA stats, pipeline latency, and circuit breaker state.
 */
app.get('/api/dashboard', (req, res) => {
  const { getCircuitState } = require('./src/classifier/llm-client');
  res.json({
    ...getMetrics().getDashboard(),
    journeyIntelligence: getJourneyEngine().getJourneyIntelligence(),
    identity: getResolver().getStats(),
    nba: getNBAEngine().getStats(),
    latency: pipeline ? pipeline.getLatencyStats() : null,
    circuitBreaker: getCircuitState()
  });
});

/**
 * GET /api/dashboard/kpis
 * Returns computed KPIs: total contacts, SLA compliance, average wait time,
 * deflection rate, average contact value, and sentiment distribution.
 */
app.get('/api/dashboard/kpis', (req, res) => {
  res.json(getMetrics().getKPIs());
});

/**
 * GET /api/dashboard/operations
 * Returns the operational summary (top intents, channel mix, error rates)
 * plus journey intelligence and NBA stats for the operations view.
 */
app.get('/api/dashboard/operations', (req, res) => {
  res.json({
    operationalSummary: getMetrics().getOperationalSummary(),
    journeyIntelligence: getJourneyEngine().getJourneyIntelligence(),
    nba: getNBAEngine().getStats()
  });
});

/**
 * GET /api/metrics/timeseries
 * Returns bucketed time-series data for charting. Supports configurable
 * time range and bucket size for zoom-in/zoom-out in the dashboard.
 *
 * @param {number} [req.query.from] - Start timestamp in milliseconds (epoch)
 * @param {number} [req.query.to] - End timestamp in milliseconds (epoch)
 * @param {number} [req.query.bucket=60000] - Bucket size in milliseconds (default: 1 minute)
 */
app.get('/api/metrics/timeseries', (req, res) => {
  const fromMs = req.query.from ? parseInt(req.query.from) : undefined;
  const toMs = req.query.to ? parseInt(req.query.to) : undefined;
  const bucketMs = req.query.bucket ? parseInt(req.query.bucket) : 60000;
  res.json(getMetrics().getTimeSeries({ fromMs, toMs, bucketMs }));
});

// --- Metrics History (from SQLite) -----------------------------

/**
 * GET /api/metrics/history
 * Returns historical metrics snapshots from the SQLite persistence layer.
 * Only available when PERSIST=true. Returns an empty array otherwise.
 *
 * @param {number} [req.query.hours=24] - How many hours of history to retrieve
 */
app.get('/api/metrics/history', (req, res) => {
  if (!config.PERSIST) return res.json({ snapshots: [] });
  try {
    const { getDatabase } = require('./src/store/database');
    const db = getDatabase();
    const hours = parseInt(req.query.hours) || 24;
    const cutoff = new Date(Date.now() - hours * 3600000).toISOString();
    const rows = db.prepare(
      'SELECT counters, kpis, timestamp FROM metrics_snapshots WHERE timestamp > ? ORDER BY timestamp'
    ).all(cutoff);
    res.json({
      snapshots: rows.map(r => ({
        counters: JSON.parse(r.counters),
        kpis: JSON.parse(r.kpis),
        timestamp: r.timestamp
      }))
    });
  } catch {
    res.json({ snapshots: [] });
  }
});

// --- Events Export ---------------------------------------------

/**
 * GET /api/events/export
 * Exports archived events from SQLite in JSON or CSV format.
 * Requires PERSIST=true. Supports time-range filtering and a hard
 * cap of 10,000 rows to prevent memory exhaustion on large exports.
 *
 * @param {string} [req.query.from] - ISO 8601 start date (default: 24h ago)
 * @param {string} [req.query.to] - ISO 8601 end date (default: now)
 * @param {number} [req.query.limit=1000] - Max rows (capped at 10,000)
 * @param {string} [req.query.format='json'] - Output format: 'json' or 'csv'
 */
app.get('/api/events/export', (req, res) => {
  if (!config.PERSIST) return res.status(400).json({ error: 'Persistence not enabled' });
  try {
    const { getDatabase } = require('./src/store/database');
    const db = getDatabase();
    const from = req.query.from || new Date(Date.now() - 86400000).toISOString();
    const to = req.query.to || new Date().toISOString();
    const limit = Math.min(parseInt(req.query.limit) || 1000, 10000);
    const format = req.query.format || 'json';

    const rows = db.prepare(
      'SELECT event_id, customer_id, channel, event_type, classification_domain, classification_intent, timestamp FROM events_archive WHERE timestamp >= ? AND timestamp <= ? ORDER BY timestamp DESC LIMIT ?'
    ).all(from, to, limit);

    if (format === 'csv') {
      const header = 'event_id,customer_id,channel,event_type,classification_domain,classification_intent,timestamp\n';
      const body = rows.map(r =>
        [r.event_id, r.customer_id, r.channel, r.event_type, r.classification_domain || '', r.classification_intent || '', r.timestamp].join(',')
      ).join('\n');
      res.setHeader('Content-Type', 'text/csv');
      res.setHeader('Content-Disposition', 'attachment; filename=events-export.csv');
      res.send(header + body);
    } else {
      res.json({ count: rows.length, events: rows });
    }
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Platform Status -------------------------------------------

/**
 * GET /api/status
 * Returns comprehensive platform status: pipeline state, processed count,
 * classifier stats, identity/journey/NBA stats, memory usage, and database info.
 */
app.get('/api/status', (req, res) => {
  const mem = process.memoryUsage();
  res.json({
    ...pipeline.getStatus(),
    memory: {
      rssMB: Math.round(mem.rss / 1024 / 1024),
      heapUsedMB: Math.round(mem.heapUsed / 1024 / 1024),
      heapTotalMB: Math.round(mem.heapTotal / 1024 / 1024)
    },
    database: config.PERSIST ? getDatabaseStats() : null
  });
});

// --- Dead Letters ----------------------------------------------

/**
 * GET /api/dead-letters
 * Returns events that failed processing and were routed to the dead letter
 * store. Only populated when PERSIST=true. Useful for debugging pipeline
 * errors and identifying malformed event payloads.
 */
app.get('/api/dead-letters', (req, res) => {
  if (!config.PERSIST) return res.json({ count: 0, deadLetters: [] });
  try {
    const { getDatabase } = require('./src/store/database');
    const db = getDatabase();
    const rows = db.prepare('SELECT * FROM dead_letters ORDER BY id DESC LIMIT 50').all();
    res.json({ count: rows.length, deadLetters: rows });
  } catch {
    res.json({ count: 0, deadLetters: [] });
  }
});

// --- Event Stream (SSE) ----------------------------------------

/**
 * GET /api/stream
 * Server-Sent Events endpoint for lightweight, HTTP-based real-time
 * event streaming. An alternative to WebSocket for clients that only
 * need to receive (not send) events, such as monitoring dashboards
 * or log aggregators. The connection stays open indefinitely; the
 * listener is cleaned up when the client disconnects.
 */
app.get('/api/stream', (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive'
  });

  const handler = (event) => {
    res.write('data: ' + JSON.stringify(event) + '\n\n');
  };

  fabric.on('enriched', handler);

  req.on('close', () => {
    fabric.removeListener('enriched', handler);
  });
});

// --- Simulation Helpers ----------------------------------------

/**
 * GET /api/channels
 * Returns the list of valid channel identifiers accepted by the event schema.
 * Used by the dashboard's event simulator UI to populate channel dropdowns.
 */
app.get('/api/channels', (req, res) => {
  res.json({ channels: CHANNELS });
});

// --- UI Root ---------------------------------------------------

/** Serve the ops dashboard single-page application */
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'ui', 'index.html'));
});

// ===============================================================
// Boot -- async initialization then start HTTP server
// ===============================================================
// The boot sequence is ordered to ensure dependencies are ready
// before consumers initialize:
//   1. Database (SQLite WAL) is opened and schema is applied
//   2. Services hydrate their in-memory state from persisted data
//   3. Pipeline is initialized (triggers classifier index build)
//   4. HTTP server begins accepting connections
// ===============================================================

/**
 * Initializes all platform subsystems and starts the HTTP server.
 * This is the application entry point, called at module load time.
 *
 * @async
 * @throws {Error} If pipeline or classifier initialization fails (causes process.exit(1))
 */
async function boot() {
  const bootStart = performance.now();

  // Step 1: Initialize the persistence layer (SQLite WAL mode).
  // Hydration loads previously-persisted profiles, journeys, NBA state,
  // and metrics snapshots back into the in-memory stores so the platform
  // resumes from where it left off after a restart.
  if (config.PERSIST) {
    initDatabase();
    getResolver().hydrate();
    getJourneyEngine().hydrate();
    getNBAEngine().hydrate();
    getMetrics().hydrate();
  }

  const hydrateMs = Math.round(performance.now() - bootStart);

  // Step 2: Build the processing pipeline and classifier index.
  // If Ollama is available, this builds a semantic embedding index (~768-dim vectors).
  // Otherwise, it falls back to TF-IDF with cosine similarity.
  pipeline = await getPipeline();
  classifier = await getClassifier();

  const stats = classifier.getStats();
  const bootMs = Math.round(performance.now() - bootStart);

  // Gather DB stats for the startup banner
  const dbStats = config.PERSIST ? getDatabaseStats() : null;
  const dbSizeStr = dbStats ? (dbStats.sizeKB + 'KB') : 'disabled';
  const cacheSource = stats.embeddingStats?.source || 'n/a';
  const profileCount = dbStats?.counts?.customer_profiles || 0;
  const journeyCount = dbStats?.counts?.journeys || 0;

  // Step 3: Start accepting connections
  server.listen(PORT, () => {
    const pad = (s, n) => String(s).padEnd(n);
    console.log('');
    console.log('  ╔══════════════════════════════════════════════════╗');
    console.log('  ║  NEXUS CX INTELLIGENCE PLATFORM                 ║');
    console.log('  ╠══════════════════════════════════════════════════╣');
    console.log('  ║  Status:    ONLINE                              ║');
    console.log('  ║  Port:      ' + pad(PORT, 38) + '║');
    console.log('  ║  Dashboard: http://localhost:' + PORT + '/               ║');
    console.log('  ║  API:       http://localhost:' + PORT + '/api            ║');
    console.log('  ║  WebSocket: ws://localhost:' + PORT + '/                 ║');
    console.log('  ╠══════════════════════════════════════════════════╣');
    console.log('  ║  Layers:                                        ║');
    console.log('  ║    1. Event Fabric      ✓ Kinesis (local)       ║');
    console.log('  ║    2. Classifier        ✓ ' + (stats.method === 'embedding' ? 'Semantic Embedding   ' : 'TF-IDF/Cosine        ') + '  ║');
    console.log('  ║    3. Journey Engine    ✓ State Machine         ║');
    console.log('  ║    4. NBA Engine        ✓ Rules + ML Scoring    ║');
    console.log('  ║    5. Analytics         ✓ Real-time Metrics     ║');
    if (stats.embeddingsReady) {
      console.log('  ╠══════════════════════════════════════════════════╣');
      console.log('  ║  Classifier: nomic-embed-text (768-dim)         ║');
      console.log('  ║  Index: ' + pad(stats.intentCount, 3) + ' intents, ' +
        pad(stats.embeddingStats.exemplarsEmbedded, 3) + ' exemplars, ' +
        pad(stats.embeddingStats.buildTimeMs, 5) + 'ms build ║');
      console.log('  ║  Cache:  ' + pad(cacheSource, 41) + '║');
    }
    console.log('  ╠══════════════════════════════════════════════════╣');
    console.log('  ║  Persistence: ' + pad(config.PERSIST ? 'SQLite WAL (' + dbSizeStr + ')' : 'disabled (in-memory only)', 35) + '║');
    console.log('  ║  Profiles: ' + pad(profileCount, 5) + ' Journeys: ' + pad(journeyCount, 22) + '║');
    console.log('  ║  Boot: ' + pad(bootMs + 'ms total, ' + hydrateMs + 'ms hydrate', 43) + '║');
    console.log('  ╚══════════════════════════════════════════════════╝');
    console.log('');
  });
}

// ===============================================================
// Graceful Shutdown
// ===============================================================
// Handles SIGTERM (container orchestrators, systemd) and SIGINT
// (Ctrl+C in development). The shutdown sequence:
//   1. Set guard flag to prevent re-entrant shutdown
//   2. Stop the metrics broadcast interval
//   3. Close all WebSocket connections with 1001 (Going Away)
//   4. Destroy the Event Fabric (stops Kinesis simulation loop)
//   5. Destroy metrics flush timers
//   6. Close the SQLite database (flushes WAL)
//   7. Close the HTTP server (stops accepting new connections)
//   8. Force-exit after 10 seconds if graceful shutdown stalls
// ===============================================================

/** @type {boolean} Guard flag to prevent re-entrant shutdown */
let shuttingDown = false;

/**
 * Initiates graceful shutdown of all platform subsystems.
 *
 * @param {string} signal - The signal that triggered shutdown (e.g., 'SIGTERM', 'SIGINT')
 */
function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  const start = Date.now();
  log.info({ signal }, 'Shutdown initiated');

  if (metricsInterval) clearInterval(metricsInterval);

  // Close WebSocket clients with 1001 (Going Away) status code
  for (const ws of wsClients) {
    try { ws.close(1001, 'Server shutting down'); } catch {}
  }
  wsClients.clear();

  fabric.destroy();
  getMetrics().destroy();
  closeDatabase();

  server.close(() => {
    log.info({ durationMs: Date.now() - start }, 'Shutdown complete');
    process.exit(0);
  });

  // Hard timeout: force exit if graceful shutdown takes longer than 10 seconds.
  // This prevents zombie processes when a WebSocket client or database lock
  // prevents clean shutdown.
  setTimeout(() => {
    log.warn('Forced shutdown after timeout');
    process.exit(1);
  }, 10000);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

boot().catch(err => {
  log.fatal({ err }, 'Failed to start');
  process.exit(1);
});

module.exports = { app, server };
