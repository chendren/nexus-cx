/**
 * @module config
 * @description Centralized configuration for the Nexus CX platform. All tunable runtime
 * parameters are defined here with sensible defaults. Environment variables override
 * defaults at startup, following the twelve-factor app methodology.
 *
 * Configuration is organized by architectural concern:
 * - Server: HTTP/WebSocket binding
 * - Ollama: Local LLM and embedding model connectivity
 * - Event Fabric: Stream sizing and backpressure
 * - Metrics: Time-series retention
 * - Persistence: SQLite write-through cache toggle
 * - Pipeline: In-memory buffer limits and session TTL
 * - Rate Limiting: API abuse protection
 *
 * @see {@link module:logger} for log-level consumption
 * @see {@link module:events/fabric} for FABRIC_STREAM_SIZE usage
 * @see {@link module:analytics/metrics} for METRICS_SERIES_SIZE usage
 * @see {@link module:pipeline} for pipeline buffer limits
 * @requires process.env — environment variable source
 */

/**
 * Parse an environment variable as an integer, returning a fallback
 * if the variable is undefined or cannot be parsed to a finite number.
 * @param {string} key - Environment variable name
 * @param {number} fallback - Default value when the variable is absent or invalid
 * @returns {number} Parsed integer or fallback
 */
function envInt(key, fallback) {
  const val = process.env[key];
  if (val === undefined) return fallback;
  const parsed = parseInt(val, 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

/**
 * Read an environment variable as a string, returning a fallback
 * when the variable is undefined or empty.
 * @param {string} key - Environment variable name
 * @param {string} fallback - Default value when the variable is absent
 * @returns {string} Environment value or fallback
 */
function envStr(key, fallback) {
  return process.env[key] || fallback;
}

/**
 * Parse an environment variable as a boolean. Truthy values are '1' and 'true';
 * all other values (including undefined) resolve to the fallback.
 * @param {string} key - Environment variable name
 * @param {boolean} fallback - Default value when the variable is absent
 * @returns {boolean} Parsed boolean or fallback
 */
function envBool(key, fallback) {
  const val = process.env[key];
  if (val === undefined) return fallback;
  return val === '1' || val === 'true';
}

/**
 * Frozen configuration object consumed by all platform modules.
 * @type {Object}
 */
const config = {
  // ── Server ──────────────────────────────────────────────────────────
  /** @type {number} HTTP/WebSocket listen port */
  PORT: envInt('CX_PORT', 3143),

  // ── Ollama (Local LLM / Embedding) ─────────────────────────────────
  /** @type {string} Ollama API base URL */
  OLLAMA_BASE_URL: envStr('OLLAMA_BASE_URL', 'http://localhost:11434'),
  /** @type {string} LLM model name for analytics and fallback classification */
  OLLAMA_LLM_MODEL: envStr('OLLAMA_LLM_MODEL', 'nemotron-mini'),
  /** @type {string} Embedding model name for deterministic classification */
  OLLAMA_EMBED_MODEL: envStr('OLLAMA_EMBED_MODEL', 'nomic-embed-text'),
  /** @type {number} Timeout in ms for LLM chat requests */
  OLLAMA_TIMEOUT_MS: envInt('OLLAMA_TIMEOUT_MS', 15000),
  /** @type {number} Timeout in ms for embedding requests */
  OLLAMA_EMBED_TIMEOUT_MS: envInt('OLLAMA_EMBED_TIMEOUT_MS', 10000),

  // ── Event Fabric ───────────────────────────────────────────────────
  /** @type {number} Max events per stream ring buffer before oldest are evicted */
  FABRIC_STREAM_SIZE: envInt('FABRIC_STREAM_SIZE', 10000),

  // ── Metrics ────────────────────────────────────────────────────────
  /** @type {number} Max time-series data points retained in memory */
  METRICS_SERIES_SIZE: envInt('METRICS_SERIES_SIZE', 10000),

  // ── Persistence ────────────────────────────────────────────────────
  /** @type {boolean} Enable SQLite write-through persistence */
  PERSIST: envBool('PERSIST', true),
  /** @type {string} Directory for SQLite database files */
  DATA_DIR: envStr('DATA_DIR', './data'),

  // ── Logging ────────────────────────────────────────────────────────
  /** @type {string} Pino log level (trace, debug, info, warn, error, fatal) */
  LOG_LEVEL: envStr('LOG_LEVEL', 'info'),

  // ── Pipeline Buffer Limits ─────────────────────────────────────────
  /** @type {number} Max LLM analysis results cached in memory */
  ANALYSIS_STORE_MAX: envInt('ANALYSIS_STORE_MAX', 500),
  /** @type {number} Max NBA action log entries retained in memory */
  NBA_ACTION_LOG_MAX: envInt('NBA_ACTION_LOG_MAX', 5000),
  /** @type {number} Max journey transition history entries retained */
  JOURNEY_HISTORY_MAX: envInt('JOURNEY_HISTORY_MAX', 10000),
  /** @type {number} Session TTL in milliseconds (default: 24 hours) */
  SESSION_TTL_MS: envInt('SESSION_TTL_MS', 86400000), // 24h

  // ── Rate Limiting ──────────────────────────────────────────────────
  /** @type {number} Sliding window duration in ms for rate limiting */
  RATE_LIMIT_WINDOW_MS: envInt('RATE_LIMIT_WINDOW_MS', 60000),
  /** @type {number} Max requests allowed per rate-limit window */
  RATE_LIMIT_MAX: envInt('RATE_LIMIT_MAX', 200)
};

module.exports = config;
