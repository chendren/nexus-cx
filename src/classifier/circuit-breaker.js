/**
 * @module classifier/circuit-breaker
 * @description Circuit breaker implementation that protects the platform against
 * Ollama service unavailability. Follows the standard three-state circuit breaker
 * pattern (CLOSED / OPEN / HALF_OPEN) to prevent cascading failures when the
 * local LLM or embedding model is down.
 *
 * Architectural layer: **Classifier** (resilience infrastructure)
 *
 * State transitions:
 * ```
 * CLOSED ──(consecutive failures >= threshold)──> OPEN
 * OPEN   ──(recovery timeout elapsed)───────────> HALF_OPEN
 * HALF_OPEN ──(probe succeeds)──────────────────> CLOSED
 * HALF_OPEN ──(probe fails)─────────────────────> OPEN
 * ```
 *
 * Two singleton instances are exported for the Ollama embedding and chat services.
 * When a breaker is OPEN, calls return null immediately (fast-fail) rather than
 * waiting for a timeout, allowing the classifier to fall back to TF-IDF or
 * heuristic analysis without blocking.
 *
 * @see {@link module:classifier/llm-client} for circuit-breaker-protected wrappers
 * @see {@link module:classifier/classifier} for fallback strategy on breaker open
 * @requires module:logger — structured logging
 * @requires module:config — timeout configuration
 */
const { createLogger } = require('../logger');
const config = require('../config');

const log = createLogger('circuit-breaker');

/**
 * Enumeration of valid circuit breaker states.
 * @readonly
 * @enum {string}
 */
const STATES = { CLOSED: 'CLOSED', OPEN: 'OPEN', HALF_OPEN: 'HALF_OPEN' };

/**
 * Generic circuit breaker for wrapping unreliable async service calls.
 * Tracks consecutive failures and transitions through CLOSED -> OPEN -> HALF_OPEN
 * states to balance availability with failure detection.
 */
class CircuitBreaker {
  /**
   * @param {string} name - Identifier for this breaker instance (used in logs and status)
   * @param {Object} [options] - Configuration overrides
   * @param {number} [options.failureThreshold=3] - Consecutive failures before opening
   * @param {number} [options.recoveryTimeoutMs=30000] - Time in ms to wait before probing
   */
  constructor(name, options = {}) {
    this.name = name;
    this.failureThreshold = options.failureThreshold || 3;
    this.recoveryTimeoutMs = options.recoveryTimeoutMs || 30000;
    this.state = STATES.CLOSED;
    this.failureCount = 0;
    this.lastFailureTime = 0;
    this.totalTrips = 0;
  }

  /**
   * Execute an async function through the circuit breaker. When the breaker
   * is OPEN, returns null immediately without invoking the function.
   * When HALF_OPEN, allows a single probe call to test recovery.
   * @param {Function} fn - Async function to execute
   * @returns {Promise<*|null>} Function result, or null if circuit is open
   * @throws {Error} Re-throws the function's error after recording the failure
   */
  async call(fn) {
    if (this.state === STATES.OPEN) {
      // Check if enough time has passed to attempt a recovery probe
      if (Date.now() - this.lastFailureTime >= this.recoveryTimeoutMs) {
        this.state = STATES.HALF_OPEN;
        log.info({ breaker: this.name }, 'Circuit half-open — probing');
      } else {
        return null; // Fast-fail: don't waste time on a known-down service
      }
    }

    try {
      const result = await fn();
      this._onSuccess();
      return result;
    } catch (err) {
      this._onFailure();
      throw err;
    }
  }

  /**
   * Record a successful call. Resets failure count and closes the circuit.
   * @private
   */
  _onSuccess() {
    if (this.state === STATES.HALF_OPEN) {
      log.info({ breaker: this.name }, 'Circuit closed — recovered');
    }
    this.state = STATES.CLOSED;
    this.failureCount = 0;
  }

  /**
   * Record a failed call. Increments failure count and opens the circuit
   * when the threshold is reached.
   * @private
   */
  _onFailure() {
    this.failureCount++;
    this.lastFailureTime = Date.now();

    if (this.failureCount >= this.failureThreshold) {
      this.state = STATES.OPEN;
      this.totalTrips++;
      log.warn({ breaker: this.name, failures: this.failureCount, trips: this.totalTrips },
        'Circuit opened — fast-failing');
    }
  }

  /**
   * Get the current state of the circuit breaker. Automatically transitions
   * from OPEN to HALF_OPEN if the recovery timeout has elapsed.
   * @returns {{name: string, state: string, failureCount: number, totalTrips: number, lastFailureTime: string|null}}
   */
  getState() {
    // Lazy state promotion: check if recovery window has passed
    if (this.state === STATES.OPEN && Date.now() - this.lastFailureTime >= this.recoveryTimeoutMs) {
      this.state = STATES.HALF_OPEN;
    }
    return {
      name: this.name,
      state: this.state,
      failureCount: this.failureCount,
      totalTrips: this.totalTrips,
      lastFailureTime: this.lastFailureTime ? new Date(this.lastFailureTime).toISOString() : null
    };
  }

  /**
   * Check whether the circuit is currently open (fast-failing).
   * @returns {boolean} True if the circuit is in OPEN state
   */
  isOpen() {
    return this.state === STATES.OPEN;
  }
}

// Singleton circuit breakers for the two Ollama service endpoints.
// Separate breakers allow embedding to fail independently of chat and vice versa.
/** @type {CircuitBreaker} Circuit breaker for Ollama embedding requests */
const embedBreaker = new CircuitBreaker('ollama-embed');
/** @type {CircuitBreaker} Circuit breaker for Ollama chat/LLM requests */
const chatBreaker = new CircuitBreaker('ollama-chat');

module.exports = { CircuitBreaker, embedBreaker, chatBreaker };
