/**
 * @module classifier/classifier
 * @description Deterministic intent classifier for the Nexus CX platform. Uses semantic
 * embeddings (nomic-embed-text, 768-dim) as the primary classification method, with
 * TF-IDF as an automatic fallback when the embedding model is unavailable.
 *
 * Architectural layer: **Classifier**
 *
 * Classification strategy (per architecture design doc):
 * - Classification is DETERMINISTIC at runtime, not LLM-based.
 * - Multi-intent detection: a single message can match multiple taxonomy intents
 *   when their similarity scores exceed the threshold.
 * - LLM (nemotron-mini) is used ONLY for:
 *     1. Fallback classification when embedding/TF-IDF confidence is below threshold
 *     2. Post-interaction analytics (sentiment, emotion, effort, topics)
 *
 * The classifier maintains two parallel indexes:
 * - **Embedding index**: Centroid vectors (one per intent) built from taxonomy exemplars
 *   via nomic-embed-text. Cached to SQLite for fast restart.
 * - **TF-IDF index**: Term frequency-inverse document frequency vectors built from
 *   taxonomy exemplars. Instant to build, used when embedding model is unavailable.
 *
 * A trained MLP reranker ({@link module:classifier/reranker}) post-processes embedding
 * results to suppress cross-domain semantic bleed (e.g., "billing" intents appearing
 * in "technical_support" results due to shared vocabulary).
 *
 * @see {@link module:classifier/reranker} for the MLP multi-intent filter
 * @see {@link module:classifier/llm-client} for Ollama communication
 * @see {@link module:classifier/circuit-breaker} for failure resilience
 * @see {@link module:pipeline} for classifier integration in the event pipeline
 * @requires module:classifier/taxonomy — intent taxonomy with exemplars
 * @requires module:classifier/llm-client — Ollama embedding and chat APIs
 * @requires module:classifier/reranker — trained re-ranking model
 * @requires module:logger — structured logging
 * @requires module:config — persistence and model configuration
 */
const { flattenTaxonomy } = require('./taxonomy');
const { ollamaChat, ollamaEmbed, checkModels } = require('./llm-client');
const { Reranker } = require('./reranker');
const { createLogger } = require('../logger');
const config = require('../config');

const log = createLogger('classifier');

/** @constant {string} Allowed characters for tokenization (lowercase alphanumeric + space) */
const ALPHA_NUMERIC = 'abcdefghijklmnopqrstuvwxyz0123456789 ';

/** @constant {number} Below this confidence, LLM fallback classification is triggered */
const LLM_CONFIDENCE_THRESHOLD = 0.3;

/**
 * @constant {number} Minimum embedding similarity for multi-intent detection.
 * Embedding cosine similarities cluster higher (~0.5-0.9) than TF-IDF (~0.1-0.5),
 * so this threshold is calibrated to separate genuine secondary intents from noise.
 */
const MULTI_INTENT_THRESHOLD = 0.70;

/** @type {Array<Object>} Flattened taxonomy: one entry per intent with domain, intent, exemplars */
const TAXONOMY_FLAT = flattenTaxonomy();
/** @type {string} Comma-separated list of all intent keys for LLM prompt construction */
const TAXONOMY_LABELS = TAXONOMY_FLAT.map(t => t.fullKey).join(', ');

/**
 * System prompt for LLM fallback classification. Only used when deterministic
 * classification confidence falls below LLM_CONFIDENCE_THRESHOLD. Instructs the
 * model to output structured JSON matching the taxonomy schema.
 * @constant {string}
 */
const CLASSIFICATION_PROMPT = `You are a customer interaction classifier. Classify the customer message into exactly one of these intent categories:

${TAXONOMY_LABELS}

Respond with ONLY a JSON object in this exact format, nothing else:
{"domain":"<domain>","intent":"<intent>","confidence":<0.0-1.0>,"sentiment":"<positive|negative|neutral|frustrated>","sentiment_score":<-1.0 to 1.0>,"topics":["<topic1>","<topic2>"],"urgency":"<low|medium|high|critical>","summary":"<one sentence summary>"}`;

/**
 * System prompt for post-interaction analytics. This is the LLM's primary role
 * in the platform: extracting sentiment, emotion, effort score, and topic insights
 * from customer interactions after the deterministic pipeline has completed.
 * @constant {string}
 */
const ANALYSIS_PROMPT = `You are a CX interaction analyst. Analyze this customer interaction and provide insights.

Respond with ONLY a JSON object:
{"sentiment":"<positive|negative|neutral|frustrated|angry>","sentiment_score":<-1.0 to 1.0>,"emotion":"<primary emotion>","topics":["<key topics>"],"urgency":"<low|medium|high|critical>","customer_effort_score":<1-5 where 5 is high effort>,"resolution_likelihood":"<low|medium|high>","recommended_tone":"<empathetic|professional|urgent|celebratory>","key_phrases":["<important phrases>"],"summary":"<one sentence>"}`;

// ── Sentiment and urgency lexicons for deterministic (heuristic) analysis ──
// These term sets power the fast-path analysis that runs on every interaction
// without requiring an LLM call. They provide instant sentiment, urgency,
// and effort scoring based on keyword matching.

/** @constant {Set<string>} Terms indicating positive customer sentiment */
const POSITIVE_TERMS = new Set([
  'thanks', 'thank', 'awesome', 'great', 'love', 'excellent', 'good', 'happy',
  'appreciate', 'perfect', 'wonderful', 'resolved', 'fixed'
]);
/** @constant {Set<string>} Terms indicating negative customer sentiment */
const NEGATIVE_TERMS = new Set([
  'bad', 'broken', 'issue', 'problem', 'angry', 'upset', 'frustrated', 'terrible',
  'awful', 'annoyed', 'cancel', 'complaint', 'unacceptable', 'stuck', 'failed'
]);
/** @constant {Set<string>} Terms indicating time-sensitive or critical urgency */
const URGENT_TERMS = new Set([
  'urgent', 'immediately', 'asap', 'today', 'now', 'critical', 'emergency',
  'cannot', "can't", 'outage', 'down', 'cancel', 'fraud'
]);
/** @constant {Set<string>} Terms indicating high customer effort (repeat contacts, transfers) */
const EFFORT_TERMS = new Set([
  'again', 'repeated', 'multiple', 'still', 'already', 'twice', 'third', 'waiting',
  'transfer', 'explain', 'every', 'keep', 'hours'
]);

/**
 * Deterministic intent classifier with multi-intent detection. Maintains
 * both an embedding-based and TF-IDF-based index for resilient classification.
 * Also provides deterministic and LLM-based interaction analysis.
 */
class Classifier {
  constructor() {
    /** @type {Array<Object>} Flattened taxonomy entries */
    this.taxonomy = TAXONOMY_FLAT;

    // ── TF-IDF index (synchronous fallback) ───────────────────────────
    /** @type {Map<string, number>} Word to vocabulary index mapping */
    this.vocabulary = new Map();
    /** @type {Map<string, number>} Inverse document frequency per word */
    this.idf = new Map();
    /** @type {Array<Object>} Centroid TF-IDF vectors per intent */
    this.tfidfVectors = [];
    /** @type {boolean} Whether TF-IDF index has been built */
    this.tfidfBuilt = false;

    // ── Semantic embedding index (primary) ────────────────────────────
    /** @type {Array<Object>} Centroid embedding vectors per intent: { ...taxonomyEntry, vector: Float64Array(768) } */
    this.embeddingVectors = [];
    /** @type {boolean} Whether embedding index is ready for queries */
    this.embeddingsReady = false;

    // ── Trained re-ranker (post-processing for multi-intent) ──────────
    /** @type {Reranker|null} MLP model that suppresses cross-domain semantic bleed */
    this.reranker = null;

    /** @type {boolean} Whether the LLM model is available for analytics and fallback */
    this.llmAvailable = false;
    /** @type {Object} LLM usage statistics */
    this.llmStats = { calls: 0, fallbacks: 0, errors: 0, avgLatencyMs: 0 };
    /** @type {Object} Embedding usage statistics */
    this.embeddingStats = { exemplarsEmbedded: 0, queriesProcessed: 0, avgLatencyMs: 0, buildTimeMs: 0 };
  }

  /**
   * Initialize models — check availability and build embedding index.
   * Called once at startup.
   */
  async init() {
    const models = await checkModels();
    this.llmAvailable = models.llm;

    if (models.llm) {
      log.info('LLM (nemotron-mini) available — analytics mode active');
    }

    // Always build TF-IDF as immediate fallback
    this._buildTfIdf();

    if (models.embeddings) {
      log.info('Embedding model (nomic-embed-text) available — building semantic index...');
      await this._buildEmbeddingIndex();

      // Train or load the re-ranking model
      if (this.embeddingsReady) {
        try {
          this.reranker = new Reranker();
          await this.reranker.init(
            this.taxonomy,
            this.embeddingVectors,
            this._cosineSimilarity.bind(this)
          );
        } catch (err) {
          log.warn({ err }, 'Reranker training failed, using raw scores');
          this.reranker = null;
        }
      }
    } else {
      log.info('Embedding model not available — using TF-IDF fallback');
    }

    return this;
  }

  /**
   * Build the TF-IDF vector space index from taxonomy exemplars. This runs
   * synchronously and completes in under 1ms, making it suitable as an
   * always-available fallback. Each intent gets a centroid vector computed
   * by averaging the TF-IDF vectors of its exemplar sentences.
   * @private
   */
  _buildTfIdf() {
    const allDocs = [];
    for (const entry of this.taxonomy) {
      for (const exemplar of entry.exemplars) {
        allDocs.push(this._tokenize(exemplar));
      }
    }

    const wordSet = new Set();
    for (const doc of allDocs) {
      for (const word of doc) wordSet.add(word);
    }
    let idx = 0;
    for (const word of wordSet) {
      this.vocabulary.set(word, idx++);
    }

    const N = allDocs.length;
    for (const word of wordSet) {
      const docsWithWord = allDocs.filter(doc => doc.includes(word)).length;
      this.idf.set(word, Math.log(N / (1 + docsWithWord)) + 1);
    }

    for (const entry of this.taxonomy) {
      const vectors = entry.exemplars.map(ex => this._tfidfVector(this._tokenize(ex)));
      const centroid = this._averageVectors(vectors, this.vocabulary.size);
      this.tfidfVectors.push({ ...entry, vector: centroid });
    }

    this.tfidfBuilt = true;
  }

  /**
   * Build the semantic embedding index by embedding all taxonomy exemplars via
   * nomic-embed-text (768-dim) and computing a centroid vector per intent. On
   * first build, this makes ~600 embedding API calls to Ollama (~30s total).
   * Results are cached to SQLite so subsequent startups skip the embedding step.
   *
   * Cache invalidation: If the taxonomy changes (detected via hash), the cache
   * is cleared and the index is rebuilt from scratch.
   * @private
   */
  async _buildEmbeddingIndex() {
    const start = Date.now();
    this.embeddingVectors = [];

    // Try loading from SQLite cache
    const taxonomyHash = this._hashTaxonomy();
    if (config.PERSIST && this._loadEmbeddingCache(taxonomyHash)) {
      this.embeddingStats.buildTimeMs = Date.now() - start;
      this.embeddingsReady = this.embeddingVectors.length === this.taxonomy.length;
      if (this.embeddingsReady) {
        log.info({
          centroids: this.embeddingVectors.length,
          buildTimeMs: this.embeddingStats.buildTimeMs,
          source: 'cache'
        }, 'Embedding index loaded from cache (768-dim)');
      }
      return;
    }

    // Build from Ollama
    for (const entry of this.taxonomy) {
      const exemplarEmbeddings = [];

      for (const exemplar of entry.exemplars) {
        try {
          const vec = await ollamaEmbed(exemplar);
          exemplarEmbeddings.push(vec);
          this.embeddingStats.exemplarsEmbedded++;
        } catch (err) {
          log.warn({ err }, 'Failed to embed exemplar');
        }
      }

      if (exemplarEmbeddings.length > 0) {
        const centroid = this._averageVectors(exemplarEmbeddings, exemplarEmbeddings[0].length);
        this.embeddingVectors.push({ ...entry, vector: centroid });
      }
    }

    this.embeddingStats.buildTimeMs = Date.now() - start;
    this.embeddingsReady = this.embeddingVectors.length === this.taxonomy.length;

    if (this.embeddingsReady) {
      log.info({
        exemplars: this.embeddingStats.exemplarsEmbedded,
        buildTimeMs: this.embeddingStats.buildTimeMs,
        centroids: this.embeddingVectors.length,
        source: 'ollama'
      }, 'Semantic embedding index built (768-dim)');

      // Cache to SQLite
      if (config.PERSIST) {
        this._saveEmbeddingCache(taxonomyHash);
      }
    }
  }

  /**
   * Compute a hash of the taxonomy for cache invalidation. Uses a simple
   * string hash of intent keys and exemplar counts. When this hash changes,
   * the embedding cache in SQLite is invalidated and rebuilt.
   * @returns {string} Hash string
   * @private
   */
  _hashTaxonomy() {
    let str = '';
    for (const entry of this.taxonomy) {
      str += entry.fullKey + ':' + entry.exemplars.length + ';';
    }
    let h = 0;
    for (let i = 0; i < str.length; i++) {
      h = ((h << 5) - h + str.charCodeAt(i)) | 0;
    }
    return String(Math.abs(h));
  }

  /**
   * Attempt to load pre-computed centroid embeddings from the SQLite embedding_cache table.
   * Verifies the stored taxonomy hash matches the current taxonomy before loading.
   * @param {string} taxonomyHash - Current taxonomy hash for validation
   * @returns {boolean} True if cache was successfully loaded
   * @private
   */
  _loadEmbeddingCache(taxonomyHash) {
    try {
      const { getDatabase } = require('../store/database');
      const db = getDatabase();

      // Check if cache has the right hash by reading the first entry
      const first = db.prepare('SELECT text_hash FROM embedding_cache LIMIT 1').get();
      if (!first) return false;

      // The model field stores hash:model for the first centroid entry
      const hashRow = db.prepare(
        "SELECT vector FROM embedding_cache WHERE text_hash = ?"
      ).get('_taxonomy_hash');
      if (!hashRow) return false;

      const storedHash = hashRow.vector.toString('utf-8');
      if (storedHash !== taxonomyHash) {
        log.info('Embedding cache invalidated — taxonomy changed');
        db.prepare('DELETE FROM embedding_cache').run();
        return false;
      }

      // Load centroid vectors
      const rows = db.prepare(
        "SELECT text_hash, vector FROM embedding_cache WHERE text_hash != '_taxonomy_hash' ORDER BY rowid"
      ).all();

      if (rows.length !== this.taxonomy.length) return false;

      for (let i = 0; i < this.taxonomy.length; i++) {
        const entry = this.taxonomy[i];
        const row = rows[i];
        const buffer = row.vector;
        const vector = new Float64Array(buffer.buffer, buffer.byteOffset, buffer.byteLength / 8);
        this.embeddingVectors.push({ ...entry, vector });
        this.embeddingStats.exemplarsEmbedded += entry.exemplars.length;
      }

      return true;
    } catch {
      return false;
    }
  }

  /**
   * Persist computed centroid embeddings to SQLite for fast restart. Stores
   * the taxonomy hash alongside the vectors so stale caches can be detected.
   * Uses a transaction for atomicity.
   * @param {string} taxonomyHash - Current taxonomy hash to store with the cache
   * @private
   */
  _saveEmbeddingCache(taxonomyHash) {
    try {
      const { getDatabase } = require('../store/database');
      const db = getDatabase();
      const insert = db.prepare(
        'INSERT OR REPLACE INTO embedding_cache (text_hash, vector, model, created_at) VALUES (?, ?, ?, ?)'
      );

      const now = new Date().toISOString();
      const saveTx = db.transaction(() => {
        db.prepare('DELETE FROM embedding_cache').run();
        insert.run('_taxonomy_hash', Buffer.from(taxonomyHash, 'utf-8'), config.OLLAMA_EMBED_MODEL, now);
        for (let i = 0; i < this.embeddingVectors.length; i++) {
          const ev = this.embeddingVectors[i];
          const buf = Buffer.from(ev.vector.buffer, ev.vector.byteOffset, ev.vector.byteLength);
          insert.run('centroid_' + i + '_' + ev.fullKey, buf, config.OLLAMA_EMBED_MODEL, now);
        }
      });
      saveTx();
      log.info({ centroids: this.embeddingVectors.length }, 'Embedding cache saved to SQLite');
    } catch (err) {
      log.warn({ err }, 'Failed to save embedding cache');
    }
  }

  /**
   * Classify text with multi-intent detection.
   * Uses semantic embeddings (primary) or TF-IDF (fallback).
   */
  async classify(text, topK = 5) {
    if (this.embeddingsReady) {
      return this._classifyWithEmbeddings(text, topK);
    }
    return this._classifyWithTfIdf(text, topK);
  }

  /**
   * Semantic embedding classification — embeds query text via nomic-embed-text,
   * cosine similarity against 20 intent centroid embeddings.
   */
  async _classifyWithEmbeddings(text, topK) {
    const start = Date.now();

    let queryVector;
    try {
      queryVector = await ollamaEmbed(text);
    } catch (err) {
      // Embedding failed — fall back to TF-IDF
      return this._classifyWithTfIdf(text, topK);
    }

    const latency = Date.now() - start;
    this.embeddingStats.queriesProcessed++;
    this.embeddingStats.avgLatencyMs = Math.round(
      (this.embeddingStats.avgLatencyMs * (this.embeddingStats.queriesProcessed - 1) + latency) /
      this.embeddingStats.queriesProcessed
    );

    const scores = this.embeddingVectors.map(iv => ({
      domain: iv.domain,
      domainLabel: iv.domainLabel,
      intent: iv.intent,
      intentLabel: iv.intentLabel,
      fullKey: iv.fullKey,
      confidence: this._cosineSimilarity(queryVector, iv.vector)
    }));

    return this._buildClassificationResult(scores, topK, 'embedding', latency);
  }

  /**
   * TF-IDF fallback classification — synchronous, used when embedding model unavailable.
   */
  _classifyWithTfIdf(text, topK) {
    if (!this.tfidfBuilt) this._buildTfIdf();

    const tokens = this._tokenize(text);
    const queryVector = this._tfidfVector(tokens);

    const scores = this.tfidfVectors.map(iv => ({
      domain: iv.domain,
      domainLabel: iv.domainLabel,
      intent: iv.intent,
      intentLabel: iv.intentLabel,
      fullKey: iv.fullKey,
      confidence: this._cosineSimilarity(queryVector, iv.vector)
    }));

    return this._buildClassificationResult(scores, topK, 'tfidf', 0);
  }

  /**
   * Build the classification result from ranked similarity scores. Handles
   * multi-intent detection by collecting all intents above the method-specific
   * threshold. Applies the trained reranker (if available) to suppress
   * cross-domain semantic bleed in embedding results.
   *
   * @param {Array<Object>} scores - Scored intents with confidence values
   * @param {number} topK - Number of alternatives to include
   * @param {string} method - Classification method ('embedding' or 'tfidf')
   * @param {number} latencyMs - Embedding query latency in ms
   * @returns {Object} Classification result with primary, detectedIntents, alternatives, and metadata
   * @private
   */
  _buildClassificationResult(scores, topK, method, latencyMs) {
    scores.sort((a, b) => b.confidence - a.confidence);

    // Trained re-ranking for embeddings — suppresses cross-domain semantic bleed
    if (method === 'embedding' && this.reranker) {
      scores = this.reranker.rerank(scores);
      scores.sort((a, b) => b.confidence - a.confidence);
    }

    const best = scores[0];

    // Multi-intent: dynamic threshold based on method
    // Embedding similarities cluster higher (~0.5-0.9) than TF-IDF (~0.1-0.5)
    const threshold = method === 'embedding' ? MULTI_INTENT_THRESHOLD : 0.2;

    const detected = [];
    for (const s of scores) {
      if (s.confidence >= threshold) {
        detected.push({
          domain: s.domain,
          domainLabel: s.domainLabel,
          intent: s.intent,
          intentLabel: s.intentLabel,
          fullKey: s.fullKey,
          confidence: Math.round(s.confidence * 1000) / 1000
        });
      }
    }

    return {
      primary: {
        domain: best.domain,
        domainLabel: best.domainLabel,
        intent: best.intent,
        intentLabel: best.intentLabel,
        fullKey: best.fullKey,
        confidence: Math.round(best.confidence * 1000) / 1000
      },
      detectedIntents: detected,
      alternatives: scores.slice(1, topK).map(s => ({
        fullKey: s.fullKey,
        intentLabel: s.intentLabel,
        confidence: Math.round(s.confidence * 1000) / 1000
      })),
      multiIntent: detected.length > 1,
      needsLLMFallback: best.confidence < LLM_CONFIDENCE_THRESHOLD,
      method,
      latencyMs
    };
  }

  /**
   * LLM fallback classification — async. Called ONLY when embedding/TF-IDF
   * confidence is below threshold. Per design doc, the LLM does NOT do primary
   * classification at runtime.
   */
  async classifyWithLLM(text) {
    if (!this.llmAvailable) return null;

    const start = Date.now();
    this.llmStats.calls++;

    try {
      const raw = await ollamaChat(CLASSIFICATION_PROMPT, text, 0);
      const latency = Date.now() - start;
      this.llmStats.avgLatencyMs = Math.round(
        (this.llmStats.avgLatencyMs * (this.llmStats.calls - 1) + latency) / this.llmStats.calls
      );

      const parsed = this._parseLLMJson(raw);
      if (!parsed) {
        this.llmStats.errors++;
        return null;
      }

      const fullKey = parsed.domain + '.' + parsed.intent;
      const validIntent = this.taxonomy.find(t => t.fullKey === fullKey);

      return {
        method: 'llm',
        model: 'nemotron-mini',
        latencyMs: latency,
        classification: validIntent ? {
          domain: parsed.domain,
          intent: parsed.intent,
          fullKey,
          confidence: parsed.confidence || 0.8
        } : null,
        sentiment: parsed.sentiment || 'neutral',
        sentimentScore: parsed.sentiment_score || 0,
        topics: parsed.topics || [],
        urgency: parsed.urgency || 'medium',
        summary: parsed.summary || '',
        raw: parsed
      };
    } catch (err) {
      this.llmStats.errors++;
      return null;
    }
  }

  /**
   * Post-interaction analysis via LLM — sentiment, emotion, effort, topics.
   * This IS the LLM's proper role per the design doc.
   */
  async analyzeInteraction(text) {
    if (!this.llmAvailable) return null;

    try {
      const raw = await ollamaChat(ANALYSIS_PROMPT, text, 0);
      const parsed = this._parseLLMJson(raw);
      if (!parsed) return null;

      return {
        method: 'llm_analysis',
        model: 'nemotron-mini',
        sentiment: parsed.sentiment || 'neutral',
        sentimentScore: parsed.sentiment_score || 0,
        emotion: parsed.emotion || 'neutral',
        topics: parsed.topics || [],
        urgency: parsed.urgency || 'medium',
        customerEffortScore: parsed.customer_effort_score || 3,
        resolutionLikelihood: parsed.resolution_likelihood || 'medium',
        recommendedTone: parsed.recommended_tone || 'professional',
        keyPhrases: parsed.key_phrases || [],
        summary: parsed.summary || ''
      };
    } catch (err) {
      return null;
    }
  }

  /**
   * Deterministic (heuristic) interaction analysis that runs on every event
   * without requiring an LLM call. Provides instant sentiment, urgency,
   * customer effort score, resolution likelihood, and topic extraction
   * using lexicon-based keyword matching. This is the "fast path" analysis
   * that feeds the journey engine and NBA engine immediately; the LLM
   * analytics pipeline runs asynchronously in the background.
   *
   * @param {string} text - Interaction text to analyze
   * @param {Object|null} [classification=null] - Classification result for context
   * @returns {Object} Analysis result with sentiment, urgency, effort score, topics, etc.
   */
  analyzeDeterministic(text, classification = null) {
    const tokens = this._tokenize(text);
    const lower = (text || '').toLowerCase();

    const positiveMatches = this._countMatches(tokens, POSITIVE_TERMS);
    const negativeMatches = this._countMatches(tokens, NEGATIVE_TERMS);
    const urgentMatches = this._countMatches(tokens, URGENT_TERMS);
    const effortMatches = this._countMatches(tokens, EFFORT_TERMS);
    const sentimentScore = clamp((positiveMatches - negativeMatches) / Math.max(tokens.length || 1, 6), -1, 1);

    let sentiment = 'neutral';
    if (sentimentScore >= 0.2) {
      sentiment = 'positive';
    } else if (negativeMatches >= 2 || lower.includes('frustrated') || lower.includes('angry')) {
      sentiment = negativeMatches >= 4 || lower.includes('angry') ? 'angry' : 'frustrated';
    } else if (sentimentScore <= -0.15) {
      sentiment = 'negative';
    }

    let urgency = 'low';
    if (urgentMatches >= 3 || lower.includes('service down') || lower.includes('cancel my account')) {
      urgency = 'critical';
    } else if (urgentMatches >= 2 || classification?.primary?.fullKey === 'account.cancellation') {
      urgency = 'high';
    } else if (urgentMatches >= 1 || classification?.primary?.domain === 'billing') {
      urgency = 'medium';
    }

    const customerEffortScore = Math.min(
      5,
      Math.max(
        1,
        1
          + effortMatches
          + (negativeMatches >= 2 ? 1 : 0)
          + (lower.includes('still') || lower.includes('again') ? 1 : 0)
          + (sentiment === 'frustrated' || sentiment === 'angry' ? 1 : 0)
      )
    );
    const resolutionLikelihood = customerEffortScore >= 4
      ? 'low'
      : classification?.primary?.domain === 'sales'
        ? 'high'
        : customerEffortScore >= 3
          ? 'medium'
          : 'high';

    const topics = this._extractTopics(tokens, classification);
    const keyPhrases = this._extractKeyPhrases(lower);
    const recommendedTone = sentiment === 'positive'
      ? 'celebratory'
      : urgency === 'critical' || sentiment === 'angry'
        ? 'urgent'
        : sentiment === 'frustrated' || sentiment === 'negative'
          ? 'empathetic'
          : 'professional';

    return {
      method: 'heuristic_analysis',
      sentiment,
      sentimentScore: round(sentimentScore),
      emotion: sentiment === 'positive' ? 'relief' : sentiment,
      topics,
      urgency,
      customerEffortScore,
      resolutionLikelihood,
      recommendedTone,
      keyPhrases,
      summary: this._buildDeterministicSummary(classification, sentiment, urgency, topics)
    };
  }

  /**
   * Parse JSON from LLM output, handling common formatting issues like
   * markdown code fences and leading/trailing text. Tries multiple
   * extraction strategies in order of likelihood.
   * @param {string} raw - Raw LLM response text
   * @returns {Object|null} Parsed JSON object, or null on failure
   * @private
   */
  _parseLLMJson(raw) {
    if (!raw) return null;
    try { return JSON.parse(raw.trim()); } catch {}

    let cleaned = raw;
    if (cleaned.includes('```json')) {
      const start = cleaned.indexOf('```json') + 7;
      const end = cleaned.indexOf('```', start);
      if (end > start) cleaned = cleaned.substring(start, end).trim();
    } else if (cleaned.includes('```')) {
      const start = cleaned.indexOf('```') + 3;
      const end = cleaned.indexOf('```', start);
      if (end > start) cleaned = cleaned.substring(start, end).trim();
    }
    try { return JSON.parse(cleaned); } catch {}

    const braceStart = raw.indexOf('{');
    const braceEnd = raw.lastIndexOf('}');
    if (braceStart >= 0 && braceEnd > braceStart) {
      try { return JSON.parse(raw.substring(braceStart, braceEnd + 1)); } catch {}
    }
    return null;
  }

  /**
   * Count how many tokens appear in a given lexicon set.
   * @param {string[]} tokens - Tokenized input words
   * @param {Set<string>} lexicon - Lexicon to match against
   * @returns {number} Number of matching tokens
   * @private
   */
  _countMatches(tokens, lexicon) {
    let count = 0;
    for (const token of tokens) {
      if (lexicon.has(token)) count++;
    }
    return count;
  }

  /**
   * Extract topic keywords from tokenized text, augmented with classification context.
   * Returns up to 5 topics combining the classification domain, matched keywords,
   * and the primary intent.
   * @param {string[]} tokens - Tokenized input words
   * @param {Object|null} classification - Classification result for domain/intent context
   * @returns {string[]} Array of topic strings (max 5)
   * @private
   */
  _extractTopics(tokens, classification) {
    const topics = [];
    if (classification?.primary?.domain) topics.push(classification.primary.domain);
    const candidates = [
      'billing', 'refund', 'payment', 'invoice', 'password', 'account', 'upgrade',
      'pricing', 'service', 'network', 'device', 'app', 'login', 'cancel', 'support'
    ];
    for (const c of candidates) {
      if (tokens.includes(c) && !topics.includes(c)) topics.push(c);
      if (topics.length >= 4) break;
    }
    if (classification?.primary?.intent && !topics.includes(classification.primary.intent)) {
      topics.push(classification.primary.intent);
    }
    return topics.slice(0, 5);
  }

  /**
   * Extract known key phrases from lowercase text via substring matching.
   * @param {string} lowerText - Lowercased input text
   * @returns {string[]} Matched key phrases (max 5)
   * @private
   */
  _extractKeyPhrases(lowerText) {
    const phrases = [];
    const candidates = [
      'service down', 'charged twice', 'reset password', 'cancel my account',
      'upgrade plan', 'billing issue', 'app keeps crashing', 'need help now'
    ];
    for (const phrase of candidates) {
      if (lowerText.includes(phrase)) phrases.push(phrase);
    }
    return phrases.slice(0, 5);
  }

  /**
   * Generate a one-line summary from deterministic analysis components.
   * @param {Object|null} classification - Classification result
   * @param {string} sentiment - Detected sentiment
   * @param {string} urgency - Detected urgency level
   * @param {string[]} topics - Extracted topics
   * @returns {string} Human-readable summary sentence
   * @private
   */
  _buildDeterministicSummary(classification, sentiment, urgency, topics) {
    const label = classification?.primary?.intentLabel || classification?.primary?.fullKey || 'interaction';
    const topicLabel = topics.length > 0 ? topics.join(', ') : 'general support';
    return label + ' interaction with ' + sentiment + ' sentiment, ' + urgency + ' urgency, and topics: ' + topicLabel + '.';
  }

  /**
   * Tokenize input text: lowercase, strip non-alphanumeric characters,
   * split on whitespace, remove stop words and single-character tokens.
   * Uses character-by-character filtering instead of regex per code conventions.
   * @param {string} text - Input text to tokenize
   * @returns {string[]} Array of cleaned tokens
   * @private
   */
  _tokenize(text) {
    const lower = text.toLowerCase();
    let cleaned = '';
    for (let i = 0; i < lower.length; i++) {
      const ch = lower[i];
      cleaned += ALPHA_NUMERIC.includes(ch) ? ch : ' ';
    }
    return cleaned.split(' ').filter(w => w.length > 1 && !STOP_WORDS.has(w));
  }

  /**
   * Compute the TF-IDF vector for a set of tokens against the vocabulary.
   * Term frequency is normalized by document length; IDF values are pre-computed
   * during index building.
   * @param {string[]} tokens - Tokenized input
   * @returns {Float64Array} TF-IDF vector (dimensionality = vocabulary size)
   * @private
   */
  _tfidfVector(tokens) {
    const vec = new Float64Array(this.vocabulary.size);
    const tf = new Map();
    for (const token of tokens) {
      tf.set(token, (tf.get(token) || 0) + 1);
    }
    for (const [word, count] of tf) {
      const idx = this.vocabulary.get(word);
      if (idx !== undefined) {
        vec[idx] = (count / tokens.length) * (this.idf.get(word) || 0);
      }
    }
    return vec;
  }

  /**
   * Compute the element-wise average of multiple vectors to produce a centroid.
   * Used to create intent-level representative vectors from multiple exemplars.
   * @param {Array<Float64Array>} vectors - Vectors to average
   * @param {number} size - Vector dimensionality (used when vectors array is empty)
   * @returns {Float64Array} Centroid vector
   * @private
   */
  _averageVectors(vectors, size) {
    if (vectors.length === 0) return new Float64Array(size || 0);
    const avg = new Float64Array(vectors[0].length);
    for (const vec of vectors) {
      for (let i = 0; i < vec.length; i++) avg[i] += vec[i];
    }
    for (let i = 0; i < avg.length; i++) avg[i] /= vectors.length;
    return avg;
  }

  /**
   * Compute cosine similarity between two vectors. Returns 0 if either
   * vector has zero magnitude. Handles vectors of different lengths by
   * using the minimum length.
   * @param {Float64Array} a - First vector
   * @param {Float64Array} b - Second vector
   * @returns {number} Cosine similarity in range [-1, 1]
   * @private
   */
  _cosineSimilarity(a, b) {
    const len = Math.min(a.length, b.length);
    let dot = 0, magA = 0, magB = 0;
    for (let i = 0; i < len; i++) {
      dot += a[i] * b[i];
      magA += a[i] * a[i];
      magB += b[i] * b[i];
    }
    magA = Math.sqrt(magA);
    magB = Math.sqrt(magB);
    if (magA === 0 || magB === 0) return 0;
    return dot / (magA * magB);
  }

  /**
   * Get comprehensive classifier statistics for the status endpoint.
   * Includes index state, model availability, reranker status, and usage metrics.
   * @returns {Object} Classifier statistics
   */
  getStats() {
    return {
      built: this.tfidfBuilt || this.embeddingsReady,
      method: this.embeddingsReady ? 'embedding' : 'tfidf',
      embeddingModel: this.embeddingsReady ? 'nomic-embed-text' : null,
      embeddingDimensions: this.embeddingsReady ? 768 : null,
      vocabularySize: this.vocabulary.size,
      intentCount: this.embeddingsReady ? this.embeddingVectors.length : this.tfidfVectors.length,
      domains: [...new Set(this.taxonomy.map(t => t.domain))],
      embeddingsReady: this.embeddingsReady,
      rerankerTrained: this.reranker ? this.reranker.trained : false,
      rerankerMetrics: this.reranker ? { ...this.reranker.metrics } : null,
      llmAvailable: this.llmAvailable,
      embeddingStats: { ...this.embeddingStats },
      llmStats: { ...this.llmStats }
    };
  }
}

/**
 * Clamp a numeric value to the [min, max] range.
 * @param {number} value - Value to clamp
 * @param {number} min - Minimum bound
 * @param {number} max - Maximum bound
 * @returns {number} Clamped value
 */
function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

/**
 * Round a value to 3 decimal places.
 * @param {number} value - Value to round
 * @returns {number} Rounded value
 */
function round(value) {
  return Math.round(value * 1000) / 1000;
}

/**
 * Standard English stop words removed during tokenization.
 * Filtering these improves TF-IDF discrimination by removing
 * high-frequency, low-information words.
 * @constant {Set<string>}
 */
const STOP_WORDS = new Set([
  'i', 'me', 'my', 'we', 'our', 'you', 'your', 'he', 'she', 'it', 'they',
  'the', 'a', 'an', 'is', 'am', 'are', 'was', 'were', 'be', 'been', 'being',
  'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could', 'should',
  'may', 'might', 'can', 'to', 'of', 'in', 'for', 'on', 'with', 'at', 'by',
  'from', 'as', 'into', 'through', 'during', 'before', 'after', 'above', 'below',
  'and', 'but', 'or', 'nor', 'not', 'so', 'yet', 'both', 'either', 'neither',
  'this', 'that', 'these', 'those', 'if', 'then', 'else', 'when', 'where',
  'how', 'what', 'which', 'who', 'whom', 'why', 'all', 'each', 'every',
  'any', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'only', 'own',
  'same', 'than', 'too', 'very', 'just', 'about', 'also'
]);

// ── Singleton ─────────────────────────────────────────────────────────
/** @type {Classifier|null} */
let instance = null;

/**
 * Get or create the singleton Classifier instance. On first call, initializes
 * the classifier (checks model availability, builds embedding/TF-IDF indexes,
 * trains the reranker). Subsequent calls return the cached instance.
 * @returns {Promise<Classifier>} Initialized classifier instance
 */
async function getClassifier() {
  if (!instance) {
    instance = new Classifier();
    await instance.init();
  }
  return instance;
}

module.exports = { Classifier, getClassifier };
