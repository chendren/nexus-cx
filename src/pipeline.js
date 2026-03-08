/**
 * @module nexus-cx/pipeline
 * @description Central orchestrator that wires the Event Fabric to all five processing layers.
 *
 * Two-phase processing model:
 *   **Phase 1 (synchronous, deterministic):**
 *     Identity resolution → Multi-intent classification → Journey state machine → NBA → Metrics
 *
 *   **Phase 2 (async, non-blocking):**
 *     LLM analytics via nemotron-mini for post-interaction enrichment:
 *       - Sentiment, emotion, effort scoring, topic extraction on every interaction
 *       - Classification fallback ONLY when TF-IDF confidence is below threshold
 *
 * Design constraint: the LLM does NOT perform primary classification. Classification is
 * deterministic (TF-IDF + cosine similarity against the static taxonomy). The LLM's role
 * is analytics enrichment and edge-case fallback only.
 *
 * Also provides:
 *   - Per-stage latency tracking via `getLatencyStats()` (rolling averages)
 *   - Dead letter capture for failed events (persisted to SQLite when enabled)
 *   - Analysis store for persisting classified/enriched events
 *
 * @requires ./events/fabric
 * @requires ./classifier/classifier
 * @requires ./identity/resolver
 * @requires ./journey/state-machine
 * @requires ./nba/engine
 * @requires ./analytics/metrics
 *
 * @see {@link module:nexus-cx/events/fabric} Event Fabric (Kinesis simulation)
 * @see {@link module:nexus-cx/classifier/classifier} Intent classifier
 */
const { getFabric } = require('./events/fabric');
const { getClassifier } = require('./classifier/classifier');
const { getResolver } = require('./identity/resolver');
const { getJourneyEngine } = require('./journey/state-machine');
const { getNBAEngine } = require('./nba/engine');
const { getMetrics } = require('./analytics/metrics');
const config = require('./config');
const { createLogger } = require('./logger');

const log = createLogger('pipeline');

class Pipeline {
  constructor() {
    this.fabric = getFabric();
    this.classifier = null; // Initialized async in start()
    this.resolver = getResolver();
    this.journeyEngine = getJourneyEngine();
    this.nbaEngine = getNBAEngine();
    this.metrics = getMetrics();
    this.isRunning = false;
    this.processedCount = 0;
    this.llmEnrichmentCount = 0;
    this.analysisStore = new Map();
    this.analysisStoreMax = config.ANALYSIS_STORE_MAX;
    this.latencyStats = { identity: 0, classify: 0, journey: 0, nba: 0, metrics: 0, total: 0, count: 0 };
    this._deadLetterStmt = null;
  }

  async start() {
    if (this.isRunning) return;

    // Initialize classifier (async — builds embedding index)
    this.classifier = await getClassifier();

    this.isRunning = true;
    log.info('Pipeline started — all processors registered');
    return this;
  }

  async _processEvent(event) {
    try {
      const t0 = performance.now();

      // ─── Phase 1: Deterministic classification + orchestration ───

      // Stage 1: Identity Resolution
      const profile = this.resolver.resolve(event);
      const t1 = performance.now();

      // Stage 2: Deterministic Classification (embedding or TF-IDF)
      if (event.event_type === 'interaction' && event.payload?.content) {
        const classification = await this.classifier.classify(event.payload.content);
        event.classification = classification;
        event.analysis = this.classifier.analyzeDeterministic(event.payload.content, classification);
        this.fabric.publish('classified-events', event);
      }

      // Stage 3: Journey State Machine
      if (event.classification) {
        const journeyResult = this.journeyEngine.processEvent(event);
        event.journey = journeyResult;

        if (profile) {
          const allJourneys = this.journeyEngine.getAllJourneys(profile.customer_id);
          profile.active_journeys = journeyResult.journeys.map(journey => ({
            journey_id: journey.journey_id,
            journey_type: journey.journey_type,
            current_state: journey.current_state,
            sla_status: journey.sla_status
          }));
          profile.journey_summary = {
            active: allJourneys.filter(journey => journey.status === 'active').length,
            completed: allJourneys.filter(journey => journey.status === 'completed').length
          };
          this.resolver.profiles?.put?.(profile);
        }

        if (journeyResult.transitions.length > 0) {
          for (const t of journeyResult.transitions) {
            this.fabric.publish('journey-events', { transition: t, event_id: event.event_id });
            this.metrics.recordJourneyTransition(t);
          }
        }

        // Stage 4: Next-Best-Action
        const bestJourney = journeyResult.journeys[0] || null;
        const nbaResult = this.nbaEngine.evaluate({
          event,
          classification: event.classification,
          profile,
          journey: bestJourney,
          analysis: event.analysis
        });
        event.nba = nbaResult;
        event.routing = nbaResult.routing;
        event.outcomes = {
          ...(event.outcomes || {}),
          deflected: nbaResult.routing?.should_deflect === true
            || nbaResult.actions.some(action => ['self_service_redirect', 'digital_deflection'].includes(action.type))
        };

        if (nbaResult.actions.length > 0) {
          this.fabric.publish('nba-events', { actions: nbaResult.actions, event_id: event.event_id });
          for (const a of nbaResult.actions) {
            this.metrics.recordNBAAction(a);
          }
        }
      }

      // Stage 5: Metrics
      this.metrics.recordEvent(event);
      this.metrics.archiveEvent(event);
      this.processedCount++;

      const totalMs = performance.now() - t0;
      const identityMs = t1 - t0;
      this._updateLatency(identityMs, totalMs);

      // Emit enriched event for WebSocket broadcast
      this.fabric.emit('enriched', event);

      // ─── Phase 2: Async LLM Enrichment (non-blocking) ───
      if (event.event_type === 'interaction' && event.payload?.content) {
        this._asyncLLMEnrichment(event, profile);
      }

    } catch (err) {
      log.error({ err, event_id: event.event_id }, 'Processing error');
      this.fabric.emit('pipeline_error', { error: err, event });
      this._captureDeadLetter(event, err);
    }
  }

  /**
   * Async LLM enrichment — runs in background after the synchronous pipeline.
   * Does NOT block event processing. Results are emitted separately.
   *
   * Per design doc, LLM is used for:
   *   1. Post-interaction analytics (always) — sentiment, emotion, effort, topics
   *   2. Classification fallback (ONLY when needsLLMFallback=true)
   */
  async _asyncLLMEnrichment(event, profile) {
    const content = event.payload?.content;
    if (!content) return;

    try {
      const needsFallback = event.classification?.needsLLMFallback === true;

      // Post-interaction analytics always runs — that IS the LLM's proper role.
      // LLM classification ONLY when deterministic classifier had low confidence.
      const promises = [this.classifier.analyzeInteraction(content)];
      if (needsFallback) {
        this.classifier.llmStats.fallbacks++;
        promises.push(this.classifier.classifyWithLLM(content));
      }

      const results = await Promise.all(promises);
      const analysis = results[0];
      const llmClassification = needsFallback ? results[1] : null;

      if (!llmClassification && !analysis) return;

      this.llmEnrichmentCount++;

      const enrichment = {
        event_id: event.event_id,
        customer_id: event.customer_id,
        timestamp: new Date().toISOString(),
        llmClassification: null,
        analysis: null,
        classificationOverride: false
      };

      // LLM classification — only present when TF-IDF had low confidence
      if (llmClassification) {
        enrichment.llmClassification = llmClassification;

        if (llmClassification.classification) {
          enrichment.classificationOverride = true;
          enrichment.overrideReason = 'TF-IDF confidence ' +
            (event.classification.primary.confidence) +
            ' below threshold, LLM classified as ' +
            llmClassification.classification.fullKey;
        }
      }

      // Post-interaction analytics — the LLM's primary role
      if (analysis) {
        enrichment.analysis = analysis;
        event.analysis = {
          ...(event.analysis || {}),
          ...analysis
        };

        // Update customer profile sentiment
        if (profile && analysis.sentimentScore !== undefined) {
          if (!profile.sentiment_history) profile.sentiment_history = [];
          profile.sentiment_history.push({
            score: analysis.sentimentScore,
            sentiment: analysis.sentiment,
            timestamp: new Date().toISOString(),
            channel: event.channel
          });
          if (profile.sentiment_history.length > 20) {
            profile.sentiment_history = profile.sentiment_history.slice(-20);
          }
          this.resolver.profiles?.put?.(profile);
        }
      }

      // Store the enrichment
      this.analysisStore.set(event.event_id, enrichment);
      if (this.analysisStore.size > this.analysisStoreMax) {
        const keys = [...this.analysisStore.keys()];
        for (let i = 0; i < 100; i++) {
          this.analysisStore.delete(keys[i]);
        }
      }

      // Emit LLM enrichment event for WebSocket
      this.fabric.emit('llm_enrichment', enrichment);

      // Record analytics
      if (analysis) {
        this.metrics.setGauge('last_sentiment', analysis.sentiment);
        this.metrics.setGauge('last_sentiment_score', analysis.sentimentScore);
        this.metrics.setGauge('last_urgency', analysis.urgency);
      }

    } catch (err) {
      log.warn({ err, event_id: event.event_id }, 'LLM enrichment failed');
      this.fabric.emit('llm_enrichment_error', {
        event_id: event.event_id,
        customer_id: event.customer_id,
        error: err.message,
        timestamp: new Date().toISOString()
      });
    }
  }

  _updateLatency(identityMs, totalMs) {
    const n = this.latencyStats.count;
    this.latencyStats.identity = (this.latencyStats.identity * n + identityMs) / (n + 1);
    this.latencyStats.total = (this.latencyStats.total * n + totalMs) / (n + 1);
    this.latencyStats.count = n + 1;
  }

  getLatencyStats() {
    return {
      avgIdentityMs: Math.round(this.latencyStats.identity * 100) / 100,
      avgTotalMs: Math.round(this.latencyStats.total * 100) / 100,
      eventsProcessed: this.latencyStats.count
    };
  }

  _captureDeadLetter(event, err) {
    if (!config.PERSIST) return;
    try {
      if (!this._deadLetterStmt) {
        const { getDatabase } = require('./store/database');
        this._deadLetterStmt = getDatabase().prepare(
          `INSERT INTO dead_letters (event_id, error, event_data, timestamp)
           VALUES (?, ?, ?, ?)`
        );
      }
      this._deadLetterStmt.run(
        event?.event_id || '',
        err?.message || String(err),
        JSON.stringify({ customer_id: event?.customer_id, channel: event?.channel, content: (event?.payload?.content || '').substring(0, 500) }),
        new Date().toISOString()
      );
    } catch {}
  }

  async ingest(event) {
    this.fabric.publish('raw-events', event);
    await this._processEvent(event);
    return event;
  }

  getAnalysis(eventId) {
    return this.analysisStore.get(eventId) || null;
  }

  getRecentAnalyses(limit = 20) {
    const entries = [...this.analysisStore.values()];
    return entries.slice(-limit);
  }

  getStatus() {
    const { getCircuitState } = require('./classifier/llm-client');
    return {
      running: this.isRunning,
      processedCount: this.processedCount,
      llmEnrichmentCount: this.llmEnrichmentCount,
      latency: this.getLatencyStats(),
      circuitBreaker: getCircuitState(),
      fabricMetrics: this.fabric.getMetrics(),
      classifierStats: this.classifier.getStats(),
      identityStats: this.resolver.getStats(),
      journeyStats: this.journeyEngine.getStats(),
      journeyIntelligence: this.journeyEngine.getJourneyIntelligence(),
      nbaStats: this.nbaEngine.getStats(),
      dashboardMetrics: this.metrics.getDashboard()
    };
  }
}

let instance = null;
async function getPipeline() {
  if (!instance) {
    instance = new Pipeline();
    await instance.start();
  }
  return instance;
}

module.exports = { Pipeline, getPipeline };
