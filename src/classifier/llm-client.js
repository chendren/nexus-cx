/**
 * @module classifier/llm-client
 * @description Low-level HTTP client for the local Ollama inference server. Provides two
 * service interfaces used across the classifier layer:
 *
 * 1. **Embedding** (nomic-embed-text, 768-dim): Generates semantic vector representations
 *    used by the deterministic classifier for cosine-similarity-based intent matching.
 * 2. **Chat/LLM** (nemotron-mini): Powers post-interaction analytics (sentiment, emotion,
 *    effort scoring, topic extraction) and serves as a classification fallback when
 *    embedding confidence is below threshold.
 *
 * Architectural layer: **Classifier** (infrastructure)
 *
 * Both services expose "safe" variants that wrap calls in circuit breakers to prevent
 * cascading failures when Ollama is unavailable. The raw variants are used during
 * startup (embedding index build) where failures should propagate immediately.
 *
 * In production, embedding calls would route to Amazon Bedrock (Titan Embeddings)
 * and LLM calls to Amazon Bedrock (Claude or similar). The function signatures are
 * designed to map directly to those APIs.
 *
 * @see {@link module:classifier/circuit-breaker} for failure protection
 * @see {@link module:classifier/classifier} for embedding and LLM consumption
 * @see {@link module:config} for Ollama URL, model names, and timeout configuration
 * @requires http — Node.js HTTP client
 * @requires module:config — Ollama connection parameters
 * @requires module:logger — structured logging
 */
const http = require('http');
const config = require('../config');
const { createLogger } = require('../logger');

const log = createLogger('ollama');
/** @type {string} Ollama API base URL from configuration */
const OLLAMA_BASE = config.OLLAMA_BASE_URL;
/** @type {string} LLM model identifier (default: nemotron-mini) */
const MODEL = config.OLLAMA_LLM_MODEL;
/** @type {string} Embedding model identifier (default: nomic-embed-text) */
const EMBED_MODEL = config.OLLAMA_EMBED_MODEL;

/**
 * Send a chat completion request to Ollama. Uses the OpenAI-compatible
 * `/v1/chat/completions` endpoint with streaming disabled for simplicity.
 *
 * @param {string} systemPrompt - System-level instruction (classifier prompt or analysis prompt)
 * @param {string} userMessage - User message to classify or analyze
 * @param {number} [temperature=0] - Sampling temperature (0 = deterministic)
 * @returns {Promise<string>} Trimmed response text from the model
 * @throws {Error} On connection failure, timeout, or unparseable response
 * @example
 * const result = await ollamaChat('You are a classifier...', 'My bill is wrong');
 */
function ollamaChat(systemPrompt, userMessage, temperature = 0) {
  return new Promise((resolve, reject) => {
    const payload = JSON.stringify({
      model: MODEL,
      messages: [
        { role: 'system', content: systemPrompt },
        { role: 'user', content: userMessage }
      ],
      stream: false,
      temperature,
      options: { num_predict: 300 }
    });

    const url = new URL(OLLAMA_BASE);
    const options = {
      hostname: url.hostname,
      port: parseInt(url.port) || 11434,
      path: '/v1/chat/completions',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(payload)
      }
    };

    const req = http.request(options, (res) => {
      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('end', () => {
        try {
          const data = JSON.parse(body);
          const content = data.choices?.[0]?.message?.content || '';
          resolve(content.trim());
        } catch (e) {
          reject(new Error('Failed to parse Ollama response: ' + e.message));
        }
      });
    });

    req.on('error', (e) => reject(new Error('Ollama connection failed: ' + e.message)));
    req.setTimeout(config.OLLAMA_TIMEOUT_MS, () => {
      req.destroy();
      reject(new Error('Ollama request timed out'));
    });

    req.write(payload);
    req.end();
  });
}

/**
 * Generate a 768-dimensional semantic embedding vector for the given text.
 * Uses Ollama's native `/api/embeddings` endpoint with nomic-embed-text.
 * Returns a Float64Array for efficient numeric operations (cosine similarity).
 *
 * @param {string} text - Input text to embed
 * @returns {Promise<Float64Array>} 768-dimensional embedding vector
 * @throws {Error} On connection failure, timeout, empty embedding, or parse error
 */
function ollamaEmbed(text) {
  return new Promise((resolve, reject) => {
    const payload = JSON.stringify({
      model: EMBED_MODEL,
      prompt: text
    });

    const url = new URL(OLLAMA_BASE);
    const options = {
      hostname: url.hostname,
      port: parseInt(url.port) || 11434,
      path: '/api/embeddings',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(payload)
      }
    };

    const req = http.request(options, (res) => {
      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('end', () => {
        try {
          const data = JSON.parse(body);
          if (!data.embedding || data.embedding.length === 0) {
            reject(new Error('Empty embedding returned'));
            return;
          }
          resolve(new Float64Array(data.embedding));
        } catch (e) {
          reject(new Error('Failed to parse embedding response: ' + e.message));
        }
      });
    });

    req.on('error', (e) => reject(new Error('Ollama embedding failed: ' + e.message)));
    req.setTimeout(config.OLLAMA_EMBED_TIMEOUT_MS, () => {
      req.destroy();
      reject(new Error('Ollama embedding request timed out'));
    });

    req.write(payload);
    req.end();
  });
}

/**
 * Check which Ollama models are currently loaded and available.
 * Queries the `/api/tags` endpoint and checks for the presence of
 * the expected LLM (nemotron) and embedding (nomic-embed) models.
 *
 * @returns {Promise<{llm: boolean, embeddings: boolean}>} Model availability flags
 */
async function checkModels() {
  return new Promise((resolve) => {
    const req = http.get(config.OLLAMA_BASE_URL + '/api/tags', (res) => {
      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('end', () => {
        try {
          const data = JSON.parse(body);
          const models = data.models || [];
          const names = models.map(m => m.model || m.name || '');
          resolve({
            llm: names.some(n => n.startsWith('nemotron')),
            embeddings: names.some(n => n.startsWith('nomic-embed'))
          });
        } catch {
          resolve({ llm: false, embeddings: false });
        }
      });
    });
    req.on('error', () => resolve({ llm: false, embeddings: false }));
    req.setTimeout(3000, () => { req.destroy(); resolve({ llm: false, embeddings: false }); });
  });
}

/**
 * Check if the LLM model is available. Backwards-compatible convenience wrapper.
 * @returns {Promise<boolean>} True if the LLM model is loaded in Ollama
 */
async function isAvailable() {
  const models = await checkModels();
  return models.llm;
}

// ── Circuit-breaker-protected wrappers ────────────────────────────────
// These "safe" variants route through the circuit breaker, returning null
// when the breaker is open instead of throwing. Used in hot-path operations
// where a timeout should not block the pipeline.
const { embedBreaker, chatBreaker } = require('./circuit-breaker');

/**
 * Circuit-breaker-protected embedding call. Returns null when the
 * embedding circuit breaker is open (fast-fail).
 * @param {string} text - Input text to embed
 * @returns {Promise<Float64Array|null>} Embedding vector or null on breaker open
 */
async function ollamaEmbedSafe(text) {
  return embedBreaker.call(() => ollamaEmbed(text));
}

/**
 * Circuit-breaker-protected chat call. Returns null when the
 * chat circuit breaker is open (fast-fail).
 * @param {string} systemPrompt - System prompt
 * @param {string} userMessage - User message
 * @param {number} [temperature=0] - Sampling temperature
 * @returns {Promise<string|null>} Response text or null on breaker open
 */
async function ollamaChatSafe(systemPrompt, userMessage, temperature = 0) {
  return chatBreaker.call(() => ollamaChat(systemPrompt, userMessage, temperature));
}

/**
 * Get the current state of both circuit breakers (embed + chat).
 * Used by the status and health endpoints.
 * @returns {{embed: Object, chat: Object}} Circuit breaker state snapshots
 */
function getCircuitState() {
  return {
    embed: embedBreaker.getState(),
    chat: chatBreaker.getState()
  };
}

module.exports = {
  ollamaChat, ollamaEmbed,
  ollamaChatSafe, ollamaEmbedSafe,
  isAvailable, checkModels,
  getCircuitState,
  MODEL, EMBED_MODEL
};
