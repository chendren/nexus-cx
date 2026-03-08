/**
 * @module events/fabric
 * @description The Event Fabric is the unified streaming backbone of the Nexus CX platform.
 * It locally simulates Amazon Kinesis Data Streams using Node.js EventEmitter, providing
 * a single bus through which all customer interaction events flow. Every channel
 * (voice, chat, web, mobile, email, SMS, WhatsApp) publishes to this fabric, and
 * downstream processors (classifier, identity, journey, NBA, analytics) subscribe
 * to the appropriate streams.
 *
 * Architectural layer: **Event Fabric**
 *
 * Key design decisions:
 * - Ring buffer per stream: bounded memory usage via configurable max size
 * - Synchronous processor execution: processors run inline on publish() for
 *   deterministic ordering guarantees (matches Kinesis shard-level ordering)
 * - Built-in throughput metrics: events/sec calculated via sliding 1-second window
 * - Singleton pattern: all modules share a single fabric instance
 *
 * Pre-configured streams:
 * - `raw-events`: Unprocessed inbound events from all channels
 * - `classified-events`: Events after intent classification
 * - `journey-events`: Journey state transition events
 * - `nba-events`: Next-best-action recommendation events
 * - `action-events`: Action delivery events
 *
 * @see {@link module:events/schema} for the canonical event format
 * @see {@link module:pipeline} for the processor wiring
 * @see {@link module:config} for FABRIC_STREAM_SIZE configuration
 * @requires events — Node.js EventEmitter
 * @requires module:config — stream size limits
 */
const EventEmitter = require('events');
const config = require('../config');

/**
 * Unified event streaming backbone. Extends EventEmitter to support
 * named streams, ring-buffered retention, processor registration,
 * and real-time throughput metrics.
 * @extends EventEmitter
 */
class EventFabric extends EventEmitter {
  constructor() {
    super();
    this.setMaxListeners(100);
    /** @type {Map<string, {events: Array, maxSize: number}>} Named streams with ring buffer storage */
    this.streams = new Map();
    /** @type {Array<{name: string, fn: Function, streamFilter: string|null}>} Registered stream processors */
    this.processors = [];
    /** @type {Object} Real-time throughput and distribution metrics */
    this.metrics = {
      totalEvents: 0,
      eventsByChannel: {},
      eventsByType: {},
      eventsPerSecond: 0,
      _recentTimestamps: []
    };

    // Sliding window throughput calculation: count events in the last 1 second
    this._metricsInterval = setInterval(() => {
      const now = Date.now();
      this.metrics._recentTimestamps = this.metrics._recentTimestamps.filter(t => now - t < 1000);
      this.metrics.eventsPerSecond = this.metrics._recentTimestamps.length;
    }, 1000);
  }

  /**
   * Create a named stream with bounded ring buffer storage.
   * @param {string} name - Stream identifier
   * @param {number} [maxSize=config.FABRIC_STREAM_SIZE] - Maximum events before oldest are evicted
   * @returns {EventFabric} This instance (for chaining)
   */
  createStream(name, maxSize = config.FABRIC_STREAM_SIZE) {
    this.streams.set(name, { events: [], maxSize });
    return this;
  }

  /**
   * Publish an event to a named stream. If the stream does not exist, it is
   * auto-created. After storing the event in the ring buffer, this method:
   * 1. Updates throughput and distribution metrics
   * 2. Emits EventEmitter events for real-time subscribers
   * 3. Runs all registered processors that match the stream
   *
   * @param {string} streamName - Target stream name
   * @param {Object} event - Event object conforming to the canonical schema
   * @returns {Object} The published event
   */
  publish(streamName, event) {
    const stream = this.streams.get(streamName);
    if (!stream) {
      this.createStream(streamName);
      return this.publish(streamName, event);
    }

    // Ring buffer: push new event and evict oldest if at capacity
    stream.events.push(event);
    if (stream.events.length > stream.maxSize) {
      stream.events.shift();
    }

    // Update aggregate metrics for dashboard consumption
    this.metrics.totalEvents++;
    this.metrics.eventsByChannel[event.channel] = (this.metrics.eventsByChannel[event.channel] || 0) + 1;
    this.metrics.eventsByType[event.event_type] = (this.metrics.eventsByType[event.event_type] || 0) + 1;
    this.metrics._recentTimestamps.push(Date.now());

    // Emit events for real-time WebSocket subscribers and stream-specific listeners
    this.emit('event', { stream: streamName, event });
    this.emit(`stream:${streamName}`, event);

    // Execute registered processors synchronously to maintain ordering guarantees.
    // Processor errors are captured and emitted rather than propagated, so a
    // single failing processor cannot block the pipeline.
    for (const processor of this.processors) {
      if (!processor.streamFilter || processor.streamFilter === streamName) {
        try {
          processor.fn(event, streamName);
        } catch (err) {
          this.emit('processor_error', { processor: processor.name, error: err, event });
        }
      }
    }

    return event;
  }

  /**
   * Register a processor function to be invoked on every published event.
   * Processors run synchronously in registration order.
   * @param {string} name - Processor identifier (used in error reporting)
   * @param {Function} fn - Processor function: (event, streamName) => void
   * @param {string|null} [streamFilter=null] - If set, only events on this stream trigger the processor
   * @returns {EventFabric} This instance (for chaining)
   */
  registerProcessor(name, fn, streamFilter = null) {
    this.processors.push({ name, fn, streamFilter });
    return this;
  }

  /**
   * Retrieve recent events from a named stream. Returns events from the tail
   * of the ring buffer (most recent), supporting pagination via limit and offset.
   * @param {string} name - Stream name
   * @param {Object} [options] - Pagination options
   * @param {number} [options.limit=100] - Maximum events to return
   * @param {number} [options.offset=0] - Number of most recent events to skip
   * @returns {Array<Object>} Array of events
   */
  getStream(name, { limit = 100, offset = 0 } = {}) {
    const stream = this.streams.get(name);
    if (!stream) return [];
    return stream.events.slice(-(offset + limit), offset ? -offset : undefined);
  }

  /**
   * Get aggregate metrics for the fabric. Strips internal timestamp array
   * from the returned snapshot.
   * @returns {Object} Metrics snapshot (totalEvents, eventsByChannel, eventsByType, eventsPerSecond)
   */
  getMetrics() {
    return { ...this.metrics, _recentTimestamps: undefined };
  }

  /**
   * Tear down the fabric: stop the metrics interval and remove all listeners.
   * Called during graceful shutdown.
   */
  destroy() {
    clearInterval(this._metricsInterval);
    this.removeAllListeners();
  }
}

// ── Singleton ─────────────────────────────────────────────────────────
// All modules share a single fabric instance. On first access, the five
// core pipeline streams are pre-created.

/** @type {EventFabric|null} */
let instance = null;

/**
 * Get or create the singleton EventFabric instance.
 * Pre-creates the five core platform streams on first call.
 * @returns {EventFabric} The shared fabric instance
 */
function getFabric() {
  if (!instance) {
    instance = new EventFabric();
    instance.createStream('raw-events');
    instance.createStream('classified-events');
    instance.createStream('journey-events');
    instance.createStream('nba-events');
    instance.createStream('action-events');
  }
  return instance;
}

module.exports = { EventFabric, getFabric };
