/**
 * @module classifier/reranker
 * @description Trained MLP (Multi-Layer Perceptron) binary classifier that post-processes
 * embedding-based classification results to suppress cross-domain semantic bleed. When
 * multiple intents score highly due to shared vocabulary between domains (e.g., "payment"
 * appearing in both billing and treasury contexts), the reranker applies a learned penalty
 * to irrelevant intents.
 *
 * Architectural layer: **Classifier**
 *
 * Model architecture:
 * ```
 * 8 input features --> 16 hidden units (ReLU) --> 1 output (sigmoid)
 * ```
 *
 * Training:
 * - Loss: Weighted binary cross-entropy (positive class upweighted to handle class imbalance)
 * - Optimizer: SGD with momentum (0.9)
 * - Data: Synthetic queries generated from taxonomy exemplars (single-intent, same-group
 *   multi-intent, and cross-group multi-intent combinations)
 * - Cache: Trained weights are saved to disk (reranker-weights.json) and reloaded on
 *   restart. Automatic retraining occurs when the taxonomy hash changes.
 *
 * Features per (query, candidate_intent) pair:
 * | Index | Feature           | Description                                    |
 * |-------|-------------------|------------------------------------------------|
 * | 0     | cosine_sim        | Raw cosine similarity score                    |
 * | 1     | normalized_rank   | Position / total_intents (0 = best)            |
 * | 2     | gap_from_primary  | primary_score - candidate_score                |
 * | 3     | score_ratio       | candidate_score / primary_score                |
 * | 4     | same_group        | 1 if same affinity group as primary intent     |
 * | 5     | is_general        | 1 if candidate is from the 'general' domain    |
 * | 6     | domain_density    | Fraction of top-5 from same domain             |
 * | 7     | group_density     | Fraction of top-5 from same affinity group     |
 *
 * @see {@link module:classifier/classifier} for reranker integration in classification
 * @see {@link module:classifier/llm-client} for embedding generation during training
 * @requires fs — file system for weight persistence
 * @requires path — path resolution for weight file location
 * @requires module:classifier/llm-client — embedding API for synthetic training data
 * @requires module:logger — structured logging
 */
const fs = require('fs');
const path = require('path');
const { ollamaEmbed } = require('./llm-client');
const { createLogger } = require('../logger');

const log = createLogger('reranker');

/** @constant {string} File path for cached model weights */
const WEIGHTS_PATH = path.join(__dirname, 'reranker-weights.json');

/**
 * Domain affinity groups define which domains share semantic space and are
 * expected to co-occur in multi-intent messages. Intents within the same
 * group receive a "same_group" feature of 1, while cross-group combinations
 * are treated as potential semantic bleed.
 * @constant {Array<Set<string>>}
 */
const DOMAIN_GROUPS = [
  new Set([
    'tax_payments', 'savings_bonds', 'treasury_payments', 'debt_collection',
    'currency_compliance', 'tax_filing', 'tax_identity', 'tax_compliance',
    'treasury_accounts', 'foreign_tax', 'financial_crimes', 'government_payments',
    'estate_gift_tax', 'business_tax'
  ]),
  new Set(['billing', 'technical_support', 'account', 'sales'])
];
const GENERAL_DOMAIN = 'general';

/**
 * Look up which affinity group a domain belongs to.
 * @param {string} domain - Domain name
 * @returns {number} Group index (>=0), -1 for 'general', -2 for ungrouped
 */
function getDomainGroup(domain) {
  if (domain === GENERAL_DOMAIN) return -1;
  for (let i = 0; i < DOMAIN_GROUPS.length; i++) {
    if (DOMAIN_GROUPS[i].has(domain)) return i;
  }
  return -2;
}

// ── MLP Hyperparameters ───────────────────────────────────────────────
/** @constant {number} Number of input features per candidate */
const INPUT_SIZE = 8;
/** @constant {number} Hidden layer width */
const HIDDEN_SIZE = 16;
/** @constant {number} SGD learning rate */
const LEARNING_RATE = 0.05;
/** @constant {number} SGD momentum coefficient */
const MOMENTUM = 0.9;
/** @constant {number} Training epochs */
const EPOCHS = 200;
/** @constant {number} Exemplars sampled per intent for training data generation */
const EXEMPLARS_PER_INTENT = 2;
/** @constant {number} Top-K candidates scored during training for each query */
const TOP_K_CANDIDATES = 10;
/** @constant {number} Scale factor for confidence penalty applied to irrelevant intents */
const RELEVANCE_PENALTY_SCALE = 0.12;

/**
 * MLP binary classifier for multi-intent re-ranking. Trained on synthetic data
 * derived from taxonomy exemplars. Applies learned confidence penalties to
 * intents the model considers irrelevant (semantic bleed).
 */
class Reranker {
  constructor() {
    /** @type {Float64Array|null} Input-to-hidden weights [INPUT_SIZE x HIDDEN_SIZE] */
    this.w1 = null;
    /** @type {Float64Array|null} Hidden layer biases [HIDDEN_SIZE] */
    this.b1 = null;
    /** @type {Float64Array|null} Hidden-to-output weights [HIDDEN_SIZE] */
    this.w2 = null;
    /** @type {Float64Array|null} Output bias [1] */
    this.b2 = null;
    /** @type {boolean} Whether the model has been trained or loaded */
    this.trained = false;
    /** @type {Object} Training metrics (accuracy, loss, sample counts, timing) */
    this.metrics = {};
  }

  /**
   * Initialize — load cached weights or train from taxonomy.
   * @param {Array} taxonomy - flattened taxonomy entries
   * @param {Array} embeddingVectors - centroid embeddings per intent
   * @param {Function} cosineSimilarityFn - cosine similarity function
   */
  async init(taxonomy, embeddingVectors, cosineSimilarityFn) {
    const taxonomyHash = hashTaxonomy(taxonomy);

    if (this._loadWeights(taxonomyHash)) {
      log.info({ accuracy: this.metrics.accuracy, samples: this.metrics.trainingSamples }, 'Loaded cached model');
      return this;
    }

    log.info('Training re-ranking model from taxonomy...');
    await this._train(taxonomy, embeddingVectors, cosineSimilarityFn);
    this._saveWeights(taxonomyHash);
    log.info({
      accuracy: this.metrics.accuracy,
      samples: this.metrics.trainingSamples,
      positives: this.metrics.positives,
      negatives: this.metrics.negatives,
      trainTimeMs: this.metrics.trainTimeMs
    }, 'Re-ranking model trained');

    return this;
  }

  /**
   * Re-rank scored intents using the trained MLP. For each non-primary intent,
   * the model predicts a relevance score [0, 1]. Intents deemed irrelevant
   * receive a confidence penalty proportional to (1 - relevance), scaled by
   * RELEVANCE_PENALTY_SCALE. The primary intent (rank 0) is never penalized.
   *
   * @param {Array<Object>} scores - Scored intents sorted by confidence descending
   * @returns {Array<Object>} Re-ranked intents with adjusted confidence values
   */
  rerank(scores) {
    if (!this.trained || scores.length < 2) return scores;

    const primaryScore = scores[0].confidence;
    const primaryDomain = scores[0].domain;
    const primaryGroup = getDomainGroup(primaryDomain);
    const top5 = scores.slice(0, Math.min(5, scores.length));
    const top5Domains = top5.map(s => s.domain);
    const top5Groups = top5.map(s => getDomainGroup(s.domain));

    return scores.map((s, i) => {
      if (i === 0) return s;

      const features = extractFeatures(
        s, i, primaryScore, primaryDomain, primaryGroup,
        scores.length, top5Domains, top5Groups
      );
      const relevance = this._forward(features);
      const penalty = RELEVANCE_PENALTY_SCALE * (1 - relevance);

      return { ...s, confidence: s.confidence - penalty };
    });
  }

  /**
   * MLP forward pass: compute relevance score for a feature vector.
   * Hidden layer uses ReLU activation; output uses sigmoid for [0, 1] probability.
   * Logit is clamped to [-20, 20] for numerical stability.
   *
   * @param {Float64Array} features - 8-element feature vector
   * @returns {number} Relevance probability in [0, 1]
   * @private
   */
  _forward(features) {
    const hidden = new Float64Array(HIDDEN_SIZE);
    for (let j = 0; j < HIDDEN_SIZE; j++) {
      let sum = this.b1[j];
      for (let i = 0; i < INPUT_SIZE; i++) {
        sum += features[i] * this.w1[i * HIDDEN_SIZE + j];
      }
      hidden[j] = sum > 0 ? sum : 0;
    }

    let logit = this.b2[0];
    for (let j = 0; j < HIDDEN_SIZE; j++) {
      logit += hidden[j] * this.w2[j];
    }
    logit = Math.max(-20, Math.min(20, logit));
    return 1 / (1 + Math.exp(-logit));
  }

  /**
   * Train the MLP on synthetic data derived from the taxonomy. Generates
   * single-intent, same-group multi-intent, and cross-group multi-intent
   * queries, embeds them via Ollama, scores against the centroid index,
   * and trains the network using full-batch SGD with momentum.
   *
   * Uses weighted binary cross-entropy to handle class imbalance (typically
   * ~90% negative samples since most candidate intents are irrelevant to
   * any given query).
   *
   * @param {Array<Object>} taxonomy - Flattened taxonomy entries
   * @param {Array<Object>} embeddingVectors - Centroid embedding vectors per intent
   * @param {Function} cosineSimilarityFn - Cosine similarity function
   * @private
   */
  async _train(taxonomy, embeddingVectors, cosineSimilarityFn) {
    const start = Date.now();
    const queries = generateSyntheticQueries(taxonomy);
    const allFeatures = [];
    const allLabels = [];

    for (const query of queries) {
      let queryVec;
      try {
        queryVec = await ollamaEmbed(query.text);
      } catch {
        continue;
      }

      const scores = embeddingVectors.map(iv => ({
        domain: iv.domain,
        fullKey: iv.fullKey,
        confidence: cosineSimilarityFn(queryVec, iv.vector)
      }));
      scores.sort((a, b) => b.confidence - a.confidence);

      const primaryScore = scores[0].confidence;
      const primaryDomain = scores[0].domain;
      const primaryGroup = getDomainGroup(primaryDomain);
      const top5Domains = scores.slice(0, 5).map(s => s.domain);
      const top5Groups = scores.slice(0, 5).map(s => getDomainGroup(s.domain));

      const k = Math.min(TOP_K_CANDIDATES, scores.length);
      for (let c = 0; c < k; c++) {
        const s = scores[c];
        const label = query.sourceIntents.includes(s.fullKey) ? 1 : 0;
        allFeatures.push(extractFeatures(
          s, c, primaryScore, primaryDomain, primaryGroup,
          scores.length, top5Domains, top5Groups
        ));
        allLabels.push(label);
      }
    }

    const numPos = allLabels.filter(l => l === 1).length;
    const numNeg = allLabels.length - numPos;
    const posWeight = numNeg / Math.max(numPos, 1);

    this._initWeights();

    const n = allFeatures.length;
    const vw1 = new Float64Array(INPUT_SIZE * HIDDEN_SIZE);
    const vb1 = new Float64Array(HIDDEN_SIZE);
    const vw2 = new Float64Array(HIDDEN_SIZE);
    const vb2 = new Float64Array(1);
    let lastLoss = 0;

    for (let epoch = 0; epoch < EPOCHS; epoch++) {
      let totalLoss = 0;
      const gw1 = new Float64Array(INPUT_SIZE * HIDDEN_SIZE);
      const gb1 = new Float64Array(HIDDEN_SIZE);
      const gw2 = new Float64Array(HIDDEN_SIZE);
      const gb2 = new Float64Array(1);

      for (let s = 0; s < n; s++) {
        const x = allFeatures[s];
        const y = allLabels[s];
        const w = y === 1 ? posWeight : 1;

        // Forward
        const hidden = new Float64Array(HIDDEN_SIZE);
        const pre = new Float64Array(HIDDEN_SIZE);
        for (let j = 0; j < HIDDEN_SIZE; j++) {
          let sum = this.b1[j];
          for (let i = 0; i < INPUT_SIZE; i++) {
            sum += x[i] * this.w1[i * HIDDEN_SIZE + j];
          }
          pre[j] = sum;
          hidden[j] = sum > 0 ? sum : 0;
        }

        let logit = this.b2[0];
        for (let j = 0; j < HIDDEN_SIZE; j++) {
          logit += hidden[j] * this.w2[j];
        }
        logit = Math.max(-20, Math.min(20, logit));
        const pred = 1 / (1 + Math.exp(-logit));

        // Weighted BCE loss
        const eps = 1e-7;
        totalLoss += -w * (y * Math.log(pred + eps) + (1 - y) * Math.log(1 - pred + eps));

        // Backward
        const dOut = w * (pred - y);
        gb2[0] += dOut;
        for (let j = 0; j < HIDDEN_SIZE; j++) {
          gw2[j] += hidden[j] * dOut;
        }
        for (let j = 0; j < HIDDEN_SIZE; j++) {
          const dH = dOut * this.w2[j] * (pre[j] > 0 ? 1 : 0);
          gb1[j] += dH;
          for (let i = 0; i < INPUT_SIZE; i++) {
            gw1[i * HIDDEN_SIZE + j] += x[i] * dH;
          }
        }
      }

      // SGD + momentum
      for (let k = 0; k < INPUT_SIZE * HIDDEN_SIZE; k++) {
        vw1[k] = MOMENTUM * vw1[k] - LEARNING_RATE * (gw1[k] / n);
        this.w1[k] += vw1[k];
      }
      for (let k = 0; k < HIDDEN_SIZE; k++) {
        vb1[k] = MOMENTUM * vb1[k] - LEARNING_RATE * (gb1[k] / n);
        this.b1[k] += vb1[k];
        vw2[k] = MOMENTUM * vw2[k] - LEARNING_RATE * (gw2[k] / n);
        this.w2[k] += vw2[k];
      }
      vb2[0] = MOMENTUM * vb2[0] - LEARNING_RATE * (gb2[0] / n);
      this.b2[0] += vb2[0];

      lastLoss = totalLoss / n;
    }

    // Evaluate
    let correct = 0;
    for (let s = 0; s < n; s++) {
      const pred = this._forward(allFeatures[s]);
      if ((pred >= 0.5 ? 1 : 0) === allLabels[s]) correct++;
    }

    this.trained = true;
    this.metrics = {
      accuracy: Math.round((correct / n) * 100),
      loss: Math.round(lastLoss * 1000) / 1000,
      trainingSamples: n,
      positives: numPos,
      negatives: numNeg,
      posWeight: Math.round(posWeight * 100) / 100,
      epochs: EPOCHS,
      trainTimeMs: Date.now() - start
    };
  }

  /**
   * Initialize network weights using Xavier/He initialization scaled for
   * the fan-in + fan-out of each layer. Biases start at zero.
   * @private
   */
  _initWeights() {
    const scale1 = Math.sqrt(2 / (INPUT_SIZE + HIDDEN_SIZE));
    const scale2 = Math.sqrt(2 / (HIDDEN_SIZE + 1));
    this.w1 = new Float64Array(INPUT_SIZE * HIDDEN_SIZE);
    this.b1 = new Float64Array(HIDDEN_SIZE);
    this.w2 = new Float64Array(HIDDEN_SIZE);
    this.b2 = new Float64Array(1);
    for (let i = 0; i < this.w1.length; i++) this.w1[i] = (Math.random() - 0.5) * 2 * scale1;
    for (let i = 0; i < this.w2.length; i++) this.w2[i] = (Math.random() - 0.5) * 2 * scale2;
  }

  /**
   * Load cached model weights from disk. Validates the taxonomy hash to detect
   * stale caches from a previous taxonomy version.
   * @param {string} taxonomyHash - Current taxonomy hash for validation
   * @returns {boolean} True if weights were loaded successfully and hash matches
   * @private
   */
  _loadWeights(taxonomyHash) {
    try {
      const raw = fs.readFileSync(WEIGHTS_PATH, 'utf-8');
      const data = JSON.parse(raw);
      if (data.taxonomyHash !== taxonomyHash) return false;
      this.w1 = new Float64Array(data.w1);
      this.b1 = new Float64Array(data.b1);
      this.w2 = new Float64Array(data.w2);
      this.b2 = new Float64Array(data.b2);
      this.metrics = data.metrics || {};
      this.trained = true;
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Save trained model weights and metadata to disk for cache on next startup.
   * @param {string} taxonomyHash - Current taxonomy hash stored with the weights
   * @private
   */
  _saveWeights(taxonomyHash) {
    const data = {
      version: 1,
      taxonomyHash,
      architecture: INPUT_SIZE + ' -> ' + HIDDEN_SIZE + ' -> 1',
      w1: Array.from(this.w1),
      b1: Array.from(this.b1),
      w2: Array.from(this.w2),
      b2: Array.from(this.b2),
      metrics: this.metrics,
      savedAt: new Date().toISOString()
    };
    fs.writeFileSync(WEIGHTS_PATH, JSON.stringify(data));
  }
}

// ═══════════════════════════════════════════════════
// Feature extraction — shared between training and inference
// ═══════════════════════════════════════════════════

/**
 * Extract the 8-element feature vector for a (query, candidate_intent) pair.
 * Used identically during both training and inference to ensure consistency.
 *
 * @param {Object} score - Candidate intent with domain and confidence
 * @param {number} rank - Candidate's position in the ranked list (0 = primary)
 * @param {number} primaryScore - Confidence of the primary (top-ranked) intent
 * @param {string} primaryDomain - Domain of the primary intent
 * @param {number} primaryGroup - Affinity group index of the primary intent
 * @param {number} totalIntents - Total number of intents in the taxonomy
 * @param {string[]} top5Domains - Domains of the top 5 ranked intents
 * @param {number[]} top5Groups - Affinity group indices of the top 5 ranked intents
 * @returns {Float64Array} 8-element feature vector
 */
function extractFeatures(score, rank, primaryScore, primaryDomain, primaryGroup, totalIntents, top5Domains, top5Groups) {
  const group = getDomainGroup(score.domain);
  const domainCount = top5Domains.filter(d => d === score.domain).length;
  const groupCount = top5Groups.filter(g => g === group).length;

  return new Float64Array([
    score.confidence,
    rank / totalIntents,
    primaryScore - score.confidence,
    score.confidence / (primaryScore || 1),
    (group === primaryGroup && group >= 0) ? 1 : 0,
    score.domain === GENERAL_DOMAIN ? 1 : 0,
    domainCount / 5,
    groupCount / 5
  ]);
}

// ═══════════════════════════════════════════════════
// Synthetic training data generation
// ═══════════════════════════════════════════════════

/**
 * Generate synthetic training queries from taxonomy exemplars. Produces three
 * categories of training data:
 * 1. **Single-intent**: Individual exemplars (positive for their own intent only)
 * 2. **Same-group multi-intent**: Concatenated exemplars from the same affinity group
 *    (positive for both source intents)
 * 3. **Cross-group multi-intent**: Concatenated exemplars from different affinity groups
 *    (positive for both source intents, teaches the model to allow genuine cross-group needs)
 *
 * @param {Array<Object>} taxonomy - Flattened taxonomy entries with exemplars
 * @returns {Array<{text: string, sourceIntents: string[]}>} Synthetic training queries
 */
function generateSyntheticQueries(taxonomy) {
  const queries = [];

  // Single-intent: 2 exemplars per intent
  for (const entry of taxonomy) {
    const count = Math.min(EXEMPLARS_PER_INTENT, entry.exemplars.length);
    for (let i = 0; i < count; i++) {
      queries.push({ text: entry.exemplars[i], sourceIntents: [entry.fullKey] });
    }
  }

  // Group entries by affinity group
  const groups = {};
  for (const entry of taxonomy) {
    const g = getDomainGroup(entry.domain);
    const key = g >= 0 ? String(g) : entry.domain;
    if (!groups[key]) groups[key] = [];
    groups[key].push(entry);
  }

  // Same-group multi-intent combos
  for (const entries of Object.values(groups)) {
    if (entries.length < 2) continue;
    for (let c = 0; c < Math.min(4, entries.length - 1); c++) {
      const a = entries[c];
      const b = entries[(c + 1) % entries.length];
      if (a.fullKey === b.fullKey) continue;
      queries.push({
        text: a.exemplars[0] + ' and also ' + b.exemplars[0],
        sourceIntents: [a.fullKey, b.fullKey]
      });
    }
  }

  // Cross-group multi-intent combos
  const groupKeys = Object.keys(groups);
  for (let g1 = 0; g1 < groupKeys.length; g1++) {
    for (let g2 = g1 + 1; g2 < groupKeys.length; g2++) {
      const e1 = groups[groupKeys[g1]];
      const e2 = groups[groupKeys[g2]];
      if (!e1 || !e2 || !e1.length || !e2.length) continue;
      for (let c = 0; c < Math.min(2, e1.length); c++) {
        const a = e1[c];
        const b = e2[c % e2.length];
        queries.push({
          text: a.exemplars[0] + ' and I also need ' + b.exemplars[0],
          sourceIntents: [a.fullKey, b.fullKey]
        });
      }
    }
  }

  return queries;
}

/**
 * Compute a deterministic hash of the taxonomy for cache validation.
 * @param {Array<Object>} taxonomy - Flattened taxonomy entries
 * @returns {string} Hash string (absolute value of 32-bit hash)
 */
function hashTaxonomy(taxonomy) {
  let str = '';
  for (const entry of taxonomy) {
    str += entry.fullKey + ':' + entry.exemplars.length + ';';
  }
  let h = 0;
  for (let i = 0; i < str.length; i++) {
    h = ((h << 5) - h + str.charCodeAt(i)) | 0;
  }
  return String(Math.abs(h));
}

module.exports = { Reranker };
