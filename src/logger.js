/**
 * @module logger
 * @description Structured JSON logging facade for the Nexus CX platform. Wraps pino
 * to provide consistent, machine-parseable log output across all architectural layers.
 * Every module obtains its own child logger via {@link createLogger}, which automatically
 * tags log entries with the originating module name for filtering and correlation.
 *
 * In production (AWS), structured JSON logs feed directly into CloudWatch Logs Insights
 * for cross-service tracing. Locally, the same format enables grep-friendly debugging.
 *
 * @see {@link module:config} for LOG_LEVEL configuration
 * @requires pino — high-performance structured logger
 * @requires module:config — LOG_LEVEL setting
 */
const pino = require('pino');
const config = require('./config');

/**
 * Root pino logger instance shared across the platform.
 * All child loggers inherit this configuration.
 *
 * - Level: controlled by config.LOG_LEVEL (defaults to 'info')
 * - Transport: uses pino/file for debug mode (stdout), undefined otherwise for default serialization
 * - Formatters: emits level as human-readable label instead of numeric code
 * - Timestamp: ISO 8601 format for cross-system compatibility
 * - Base fields: includes service name in every log entry
 *
 * @type {import('pino').Logger}
 */
const rootLogger = pino({
  level: config.LOG_LEVEL,
  transport: config.LOG_LEVEL === 'debug'
    ? { target: 'pino/file', options: { destination: 1 } }
    : undefined,
  formatters: {
    level(label) {
      return { level: label };
    }
  },
  timestamp: pino.stdTimeFunctions.isoTime,
  base: { service: 'nexus-cx' }
});

/**
 * Create a child logger scoped to a specific module. The module name is
 * embedded in every log entry, enabling targeted log filtering (e.g.,
 * `jq 'select(.module == "classifier")'`).
 *
 * @param {string} module - Logical module name (e.g., 'classifier', 'pipeline', 'nba')
 * @returns {import('pino').Logger} Child logger instance
 * @example
 * const log = createLogger('journey');
 * log.info({ journeyId }, 'Journey created');
 */
function createLogger(module) {
  return rootLogger.child({ module });
}

module.exports = { rootLogger, createLogger };
