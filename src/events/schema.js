/**
 * @module events/schema
 * @description Canonical event schema for the Nexus CX Intelligence Platform. Defines the
 * universal event format that every interaction across every channel conforms to. This
 * schema serves as the contract between the ingest layer and all downstream processors
 * (classifier, identity, journey, NBA, analytics).
 *
 * Architectural layer: **Event Fabric**
 *
 * Design principles:
 * - **Mutable enrichment**: Events are created with null slots (identity, analysis,
 *   classification, journey, routing, nba) that downstream processors fill in-place
 *   as the event flows through the pipeline. This avoids deep-copy overhead.
 * - **Cross-channel correlation**: interaction_id and correlation_id enable tracing
 *   a single customer issue across multiple channels and sessions.
 * - **Input normalization**: Metadata fields accept both camelCase and snake_case
 *   variants, normalizing to snake_case internally for consistency.
 *
 * @see {@link module:events/fabric} for event publishing and streaming
 * @see {@link module:pipeline} for the enrichment pipeline
 * @requires uuid — v4 UUID generation for event/correlation IDs
 */
const { v4: uuidv4 } = require('uuid');

/** @constant {string} Schema version tag for forwards-compatibility detection */
const SCHEMA_VERSION = '2026-03-01';

/** @constant {string[]} Supported interaction channels */
const CHANNELS = ['voice', 'chat', 'web', 'mobile', 'email', 'sms', 'whatsapp'];

/** @constant {string[]} Valid event type categories */
const EVENT_TYPES = ['interaction', 'navigation', 'system', 'lifecycle'];

/**
 * Create a new canonical event with normalized metadata and context.
 * The returned event contains null enrichment slots that downstream
 * pipeline processors populate in-place.
 *
 * @param {Object} params - Event creation parameters
 * @param {string} params.customerId - Customer identifier (may be raw/unresolved)
 * @param {string} [params.sessionId] - Session identifier (auto-generated if absent)
 * @param {string} params.channel - Interaction channel (must be in CHANNELS)
 * @param {string} params.eventType - Event category (must be in EVENT_TYPES)
 * @param {Object} params.payload - Event payload with content and optional metadata
 * @param {string} [params.payload.content] - Message text or interaction content
 * @param {Object} [params.payload.metadata] - Channel-specific metadata (email, phone, tier, etc.)
 * @param {Object} [params.context={}] - Operational context (agent, queue, device, etc.)
 * @returns {Object} Fully constructed canonical event
 * @throws {Error} If channel or eventType is not in the allowed set
 * @example
 * const event = createEvent({
 *   customerId: 'cust-123',
 *   channel: 'chat',
 *   eventType: 'interaction',
 *   payload: { content: 'My bill is wrong', metadata: { customer_tier: 'premium' } }
 * });
 */
function createEvent({ customerId, sessionId, channel, eventType, payload, context = {} }) {
  if (!CHANNELS.includes(channel)) throw new Error(`Invalid channel: ${channel}`);
  if (!EVENT_TYPES.includes(eventType)) throw new Error(`Invalid event type: ${eventType}`);

  const normalizedMetadata = normalizeMetadata(payload?.metadata || {});
  const normalizedContext = normalizeContext(context, normalizedMetadata, channel, eventType);

  return {
    schema_version: SCHEMA_VERSION,
    event_id: uuidv4(),
    interaction_id: normalizedContext.interaction_id || uuidv4(),
    correlation_id: normalizedContext.correlation_id || uuidv4(),
    customer_id: customerId,
    session_id: sessionId || uuidv4(),
    channel,
    source: normalizedContext.source,
    touchpoint: normalizedContext.touchpoint,
    timestamp: new Date().toISOString(),
    event_type: eventType,
    payload: {
      content: payload?.content || '',
      metadata: normalizedMetadata
    },
    context: normalizedContext,
    // Enrichment slots — populated by downstream pipeline processors
    identity: null,
    analysis: null,
    classification: null,
    journey: null,
    routing: null,
    nba: null,
    outcomes: {
      resolved: false,
      deflected: false
    }
  };
}

/**
 * Normalize event metadata into a consistent snake_case format. Accepts both
 * camelCase and snake_case field names, deduplicating and cleaning values.
 * This ensures downstream processors can rely on a stable field contract
 * regardless of which channel or client submitted the event.
 *
 * @param {Object} [metadata={}] - Raw metadata from the event payload
 * @returns {Object} Normalized metadata object
 * @private
 */
function normalizeMetadata(metadata = {}) {
  const tags = Array.isArray(metadata.tags) ? metadata.tags.filter(Boolean) : [];

  return {
    ...metadata,
    email: normalizeEmail(metadata.email),
    phone: normalizePhone(metadata.phone),
    device_id: cleanString(metadata.device_id || metadata.deviceId),
    external_profile_id: cleanString(metadata.external_profile_id || metadata.externalProfileId),
    customer_tier: cleanString(metadata.customer_tier || metadata.customerTier || metadata.segment || 'standard'),
    locale: cleanString(metadata.locale || metadata.language || 'en-US'),
    app_version: cleanString(metadata.app_version || metadata.appVersion),
    page: cleanString(metadata.page || metadata.screen),
    campaign: cleanString(metadata.campaign),
    sentiment_hint: cleanString(metadata.sentiment_hint || metadata.sentimentHint),
    tags
  };
}

/**
 * Normalize operational context into a consistent format. Merges context-level
 * and metadata-level values with context taking precedence. Generates default
 * source and touchpoint identifiers when not explicitly provided.
 *
 * @param {Object} [context={}] - Raw operational context
 * @param {Object} metadata - Normalized metadata (used as fallback source)
 * @param {string} channel - Event channel
 * @param {string} eventType - Event type
 * @returns {Object} Normalized context object
 * @private
 */
function normalizeContext(context = {}, metadata, channel, eventType) {
  return {
    agent_id: cleanString(context.agentId || context.agent_id),
    queue: cleanString(context.queue),
    source_ip: cleanString(context.sourceIp || context.source_ip),
    device: cleanString(context.device || metadata.device_id),
    source: cleanString(context.source || `${channel}:${eventType}`),
    touchpoint: cleanString(context.touchpoint || metadata.page || channel),
    locale: cleanString(context.locale || metadata.locale || 'en-US'),
    customer_tier: cleanString(context.customerTier || context.customer_tier || metadata.customer_tier || 'standard'),
    app_version: cleanString(context.appVersion || context.app_version || metadata.app_version),
    page: cleanString(context.page || metadata.page),
    campaign: cleanString(context.campaign || metadata.campaign),
    queue_wait_minutes: toNumber(context.queueWaitMinutes || context.queue_wait_minutes),
    required_skills: normalizeSkills(context.requiredSkills || context.required_skills),
    interaction_id: cleanString(context.interactionId || context.interaction_id),
    correlation_id: cleanString(context.correlationId || context.correlation_id)
  };
}

/**
 * Trim a string value, returning empty string for non-string inputs.
 * @param {*} value - Value to clean
 * @returns {string} Trimmed string or empty string
 * @private
 */
function cleanString(value) {
  return typeof value === 'string' ? value.trim() : '';
}

/**
 * Normalize an email address: trim whitespace and lowercase.
 * @param {*} value - Raw email value
 * @returns {string} Normalized email or empty string
 * @private
 */
function normalizeEmail(value) {
  const email = cleanString(value).toLowerCase();
  return email || '';
}

/**
 * Normalize a phone number by stripping all non-digit characters.
 * Removes leading country code '1' from 11-digit US numbers to produce
 * a consistent 10-digit format.
 * @param {*} value - Raw phone value
 * @returns {string} Digits-only phone number or empty string
 * @private
 */
function normalizePhone(value) {
  const source = cleanString(value);
  if (!source) return '';

  // Extract digits character-by-character (no regex per code conventions)
  let digits = '';
  for (let i = 0; i < source.length; i++) {
    const ch = source[i];
    if (ch >= '0' && ch <= '9') digits += ch;
  }

  // Strip leading '1' from 11-digit US numbers to normalize to 10 digits
  if (digits.length === 11 && digits[0] === '1') {
    return digits.slice(1);
  }

  return digits;
}

/**
 * Normalize a required_skills array: clean each string, remove empties, cap at 10.
 * @param {*} value - Raw skills array
 * @returns {string[]} Cleaned and capped skills array
 * @private
 */
function normalizeSkills(value) {
  if (!Array.isArray(value)) return [];
  return value
    .map(item => cleanString(item))
    .filter(Boolean)
    .slice(0, 10);
}

/**
 * Coerce a value to a finite number, returning 0 for non-numeric inputs.
 * @param {*} value - Value to convert
 * @returns {number} Finite number or 0
 * @private
 */
function toNumber(value) {
  if (value === null || value === undefined || value === '') return 0;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

/**
 * Validate a canonical event object against the required field contract.
 * Checks for presence of all required fields and valid enum values
 * for channel and event_type.
 *
 * @param {Object} event - Event object to validate
 * @returns {{valid: boolean, error?: string}} Validation result
 * @example
 * const result = validateEvent(event);
 * if (!result.valid) throw new Error(result.error);
 */
function validateEvent(event) {
  const required = [
    'schema_version',
    'event_id',
    'customer_id',
    'session_id',
    'channel',
    'timestamp',
    'event_type',
    'payload',
    'context'
  ];

  for (const field of required) {
    if (!event[field]) return { valid: false, error: `Missing required field: ${field}` };
  }

  if (!CHANNELS.includes(event.channel)) {
    return { valid: false, error: `Invalid channel: ${event.channel}` };
  }
  if (!EVENT_TYPES.includes(event.event_type)) {
    return { valid: false, error: `Invalid event type: ${event.event_type}` };
  }

  return { valid: true };
}

module.exports = {
  createEvent,
  validateEvent,
  CHANNELS,
  EVENT_TYPES,
  SCHEMA_VERSION
};
