/**
 * @module identity/resolver
 * @description Cross-channel customer identity resolution engine. Stitches together
 * customer identities across channels (voice, chat, web, mobile, email, SMS, WhatsApp)
 * using a multi-strategy matching approach. Simulates Amazon Connect Customer Profiles
 * with custom enrichment for local-first operation.
 *
 * Architectural layer: **Identity**
 *
 * Matching strategies (evaluated in priority order):
 * 1. **Direct**: Exact match on customer_id (confidence: 1.0)
 * 2. **Deterministic**: Match on known identifiers — email, phone, device ID,
 *    external profile ID (confidence: 1.0)
 * 3. **Session**: Match on active session ID (confidence: 0.9)
 * 4. **Probabilistic**: Weighted scoring on soft signals — name, tier, device,
 *    IP address (confidence: 0.5-0.95, requires score >= 4)
 *
 * The resolver maintains:
 * - A PersistedStore of customer profiles (write-through to SQLite)
 * - An identity index (Map) for fast identifier-to-customer lookup
 * - A session map for session-based identity continuity
 * - Alias tracking for merging multiple customer IDs into a single profile
 *
 * In production, this maps to Amazon Connect Customer Profiles + a custom
 * identity graph backed by Neptune or DynamoDB.
 *
 * @see {@link module:pipeline} for identity resolution in the event pipeline
 * @see {@link module:store/persisted-store} for the backing store
 * @see {@link module:events/schema} for the event.identity enrichment slot
 * @requires module:store/memory-store — in-memory store for non-persistent mode
 * @requires module:store/persisted-store — write-through store for persistent mode
 * @requires uuid — canonical customer ID generation
 * @requires module:config — PERSIST toggle
 */
const { MemoryStore } = require('../store/memory-store');
const { PersistedStore } = require('../store/persisted-store');
const { v4: uuidv4 } = require('uuid');
const config = require('../config');

/**
 * Cross-channel identity resolution engine. Resolves raw customer identifiers
 * from inbound events into unified customer profiles, supporting alias stitching,
 * multi-channel identity graphs, and probabilistic matching.
 */
class IdentityResolver {
  constructor() {
    /** @type {PersistedStore|MemoryStore} Customer profile store */
    this.profiles = config.PERSIST
      ? new PersistedStore('customer-profiles',
          { partitionKey: 'customer_id' },
          {
            upsertSQL: `INSERT OR REPLACE INTO customer_profiles (customer_id, data, created_at, updated_at)
                         VALUES (@customer_id, @data, @created_at, @updated_at)`,
            deleteSQL: 'DELETE FROM customer_profiles WHERE customer_id = ?',
            selectAllSQL: 'SELECT data FROM customer_profiles',
            serialize: (item) => ({
              customer_id: item.customer_id,
              data: JSON.stringify(item),
              created_at: item.created_at || new Date().toISOString(),
              updated_at: item.last_seen || new Date().toISOString()
            }),
            deserialize: (row) => JSON.parse(row.data)
          })
      : new MemoryStore('customer-profiles', { partitionKey: 'customer_id' });

    /**
     * Identity index: maps identifier keys (e.g., "email:user@example.com",
     * "device:abc123") to canonical customer IDs for O(1) lookup.
     * @type {Map<string, string>}
     */
    this.identityIndex = new Map();
    /**
     * Session-to-customer mapping for session-based identity continuity.
     * @type {Map<string, string>}
     */
    this.sessions = new Map();
    /** @type {Object} Resolution strategy usage counters */
    this.matchStats = {
      newProfiles: 0,
      directMatches: 0,
      deterministicMatches: 0,
      sessionMatches: 0,
      probabilisticMatches: 0,
      aliasLinks: 0
    };
  }

  /**
   * Hydrate the identity resolver from SQLite. Loads all customer profiles
   * and rebuilds the identity index from their stored identifiers.
   * Called once at startup.
   */
  hydrate() {
    if (!this.profiles.hydrate) return;
    this.profiles.hydrate();

    // Rebuild identity index from loaded profiles
    for (const profile of this.profiles.scan()) {
      const identifiers = [];
      for (const key of (profile.identifiers || [])) {
        const colonIdx = key.indexOf(':');
        if (colonIdx > 0) {
          identifiers.push({ type: key.substring(0, colonIdx), value: key.substring(colonIdx + 1), key });
        }
      }
      this._indexProfile(profile, identifiers, profile.primary_customer_id);
    }
  }

  /**
   * Resolve or create a customer identity from an event.
   * Returns the canonical customer profile.
   */
  resolve(event) {
    const canonicalId = this._resolveCanonicalId(event.customer_id);
    const identifiers = this._extractIdentifiers(event);

    let match = this._matchProfile(canonicalId || event.customer_id, event.session_id, identifiers, event);
    let profile = match.profile;

    if (profile) {
      this._touchProfile(profile, {
        rawCustomerId: event.customer_id,
        channel: event.channel,
        sessionId: event.session_id,
        identifiers,
        strategy: match.strategy,
        confidence: match.confidence,
        event
      });
    } else {
      profile = this._createProfile(event.customer_id, event.channel, event.session_id, identifiers, event);
      match = { strategy: 'new_profile', confidence: 1 };
    }

    this.sessions.set(event.session_id, profile.customer_id);

    event.identity = {
      unified_customer_id: profile.customer_id,
      primary_customer_id: profile.primary_customer_id,
      match_type: match.strategy,
      match_confidence: match.confidence,
      matched_identifiers: identifiers.map(identifier => identifier.key),
      aliases: [...profile.aliases]
    };

    return profile;
  }

  /**
   * Attempt to match an event to an existing customer profile using all
   * available strategies in priority order: direct, deterministic, session,
   * probabilistic.
   *
   * @param {string} rawCustomerId - Raw customer ID from the event (may be alias)
   * @param {string} sessionId - Session ID for session-based matching
   * @param {Array<Object>} identifiers - Extracted and normalized identifiers
   * @param {Object} event - Full event object for probabilistic matching context
   * @returns {{profile: Object|null, strategy: string, confidence: number}} Match result
   * @private
   */
  _matchProfile(rawCustomerId, sessionId, identifiers, event) {
    if (rawCustomerId) {
      const direct = this.getProfile(rawCustomerId);
      if (direct) {
        this.matchStats.directMatches++;
        return { profile: direct, strategy: 'direct', confidence: 1 };
      }
    }

    for (const identifier of identifiers) {
      const existingId = this.identityIndex.get(identifier.key);
      if (!existingId) continue;

      const profile = this.profiles.get(existingId);
      if (profile) {
        this.matchStats.deterministicMatches++;
        return { profile, strategy: 'deterministic', confidence: 1 };
      }
    }

    if (sessionId) {
      const sessionCustomerId = this.sessions.get(sessionId);
      if (sessionCustomerId) {
        const profile = this.profiles.get(sessionCustomerId);
        if (profile) {
          this.matchStats.sessionMatches++;
          return { profile, strategy: 'session', confidence: 0.9 };
        }
      }
    }

    const probabilistic = this._findProbabilisticMatch(event, identifiers);
    if (probabilistic) {
      this.matchStats.probabilisticMatches++;
      return probabilistic;
    }

    return { profile: null, strategy: 'none', confidence: 0 };
  }

  /**
   * Attempt probabilistic identity matching using soft signals. Scores each
   * existing profile against the event's metadata (name, tier, device, IP)
   * and returns the best match if the score meets the threshold (>= 4).
   *
   * Scoring weights:
   * - Name match: +2
   * - Tier match (non-standard): +1
   * - Known IP: +1
   * - Known device: +2
   *
   * @param {Object} event - Event with metadata for signal extraction
   * @param {Array<Object>} identifiers - Extracted identifiers
   * @returns {{profile: Object, strategy: string, confidence: number}|null}
   * @private
   */
  _findProbabilisticMatch(event, identifiers) {
    const metadata = event.payload?.metadata || {};
    const normalizedName = normalizeName(metadata.name);
    if (!normalizedName) return null;

    const profiles = this.profiles.scan();
    let bestProfile = null;
    let bestScore = 0;

    for (const profile of profiles) {
      let score = 0;
      const profileName = normalizeName(profile.attributes?.name);
      const eventTier = cleanString(metadata.customer_tier);

      if (profileName && profileName === normalizedName) score += 2;
      if (eventTier && eventTier !== 'standard' && profile.attributes?.customer_tier === eventTier) score += 1;

      const normalizedIds = profile.normalized_identifiers || {};
      if (identifiers.some(identifier => isKnownIdentifier(normalizedIds.ip, identifier, 'ip'))) score += 1;
      if (identifiers.some(identifier => isKnownIdentifier(normalizedIds.device, identifier, 'device'))) score += 2;

      if (score > bestScore) {
        bestScore = score;
        bestProfile = profile;
      }
    }

    if (bestProfile && bestScore >= 4) {
      return {
        profile: bestProfile,
        strategy: 'probabilistic',
        confidence: Math.min(0.95, 0.5 + bestScore * 0.1)
      };
    }

    return null;
  }

  /**
   * Extract and normalize all identity signals from an event. Pulls identifiers
   * from event fields, payload metadata, and context. Each identifier is typed
   * (direct, email, phone, device, external_profile, ip) and keyed for index lookup.
   *
   * @param {Object} event - Canonical event object
   * @returns {Array<{type: string, value: string, key: string}>} Extracted identifiers
   * @private
   */
  _extractIdentifiers(event) {
    const identifiers = [];
    const metadata = event.payload?.metadata || {};

    pushIdentifier(identifiers, 'direct', event.customer_id);
    pushIdentifier(identifiers, 'email', normalizeEmail(metadata.email));
    pushIdentifier(identifiers, 'phone', normalizePhone(metadata.phone));
    pushIdentifier(identifiers, 'device', cleanString(metadata.device_id || metadata.deviceId || event.context?.device));
    pushIdentifier(identifiers, 'external_profile', cleanString(metadata.external_profile_id || metadata.externalProfileId));
    pushIdentifier(identifiers, 'ip', cleanString(event.context?.source_ip || event.context?.sourceIp));

    return identifiers;
  }

  /**
   * Create a new customer profile for a previously unseen customer. Initializes
   * all profile fields including attributes, identifiers, match history, and
   * empty journey summary. Persists to the store and indexes all identifiers.
   *
   * @param {string} customerId - Customer ID (or null to auto-generate)
   * @param {string} channel - Originating channel
   * @param {string} sessionId - Current session ID
   * @param {Array<Object>} identifiers - Extracted identifiers to index
   * @param {Object} event - Triggering event for metadata extraction
   * @returns {Object} The newly created customer profile
   * @private
   */
  _createProfile(customerId, channel, sessionId, identifiers, event) {
    const now = new Date().toISOString();
    const canonicalId = customerId || 'cust-' + uuidv4();
    const metadata = event.payload?.metadata || {};

    const profile = {
      customer_id: canonicalId,
      unified_customer_id: canonicalId,
      primary_customer_id: customerId || canonicalId,
      created_at: now,
      last_seen: now,
      channels_seen: [channel],
      sessions: sessionId ? [sessionId] : [],
      interaction_count: 1,
      aliases: uniqueStrings([customerId, canonicalId]),
      identifiers: identifiers.map(identifier => identifier.key),
      normalized_identifiers: identifiersToBuckets(identifiers),
      attributes: {
        email: normalizeEmail(metadata.email) || null,
        phone: normalizePhone(metadata.phone) || null,
        name: cleanString(metadata.name) || null,
        segment: cleanString(metadata.segment || 'unknown') || 'unknown',
        customer_tier: cleanString(metadata.customer_tier || 'standard') || 'standard'
      },
      match_history: [{
        strategy: 'new_profile',
        confidence: 1,
        timestamp: now,
        channel
      }],
      sentiment_history: [],
      active_journeys: [],
      journey_summary: {
        active: 0,
        completed: 0
      }
    };

    this.profiles.put(profile);
    this._indexProfile(profile, identifiers, customerId);
    this.matchStats.newProfiles++;
    return profile;
  }

  /**
   * Update an existing profile with data from a new event. Refreshes last_seen,
   * increments interaction count, records new channels/sessions, merges identifiers,
   * updates attributes from metadata, and appends to match history.
   * Match history is capped at 25 entries to bound memory usage.
   *
   * @param {Object} profile - Existing customer profile to update
   * @param {Object} params - Update parameters
   * @param {string} params.rawCustomerId - Raw customer ID from the event
   * @param {string} params.channel - Event channel
   * @param {string} params.sessionId - Event session ID
   * @param {Array<Object>} params.identifiers - Newly extracted identifiers
   * @param {string} params.strategy - Match strategy that found this profile
   * @param {number} params.confidence - Match confidence
   * @param {Object} params.event - Full event for metadata extraction
   * @private
   */
  _touchProfile(profile, { rawCustomerId, channel, sessionId, identifiers, strategy, confidence, event }) {
    profile.last_seen = new Date().toISOString();
    profile.interaction_count = (profile.interaction_count || 0) + 1;

    if (!profile.channels_seen.includes(channel)) {
      profile.channels_seen.push(channel);
    }

    if (sessionId && !profile.sessions.includes(sessionId)) {
      profile.sessions.push(sessionId);
      if (profile.sessions.length > 50) profile.sessions = profile.sessions.slice(-50);
    }

    if (rawCustomerId && !profile.aliases.includes(rawCustomerId)) {
      profile.aliases.push(rawCustomerId);
      this.matchStats.aliasLinks++;
    }

    const metadata = event.payload?.metadata || {};
    const mergedIdentifiers = mergeIdentifierBuckets(profile.normalized_identifiers, identifiersToBuckets(identifiers));
    profile.normalized_identifiers = mergedIdentifiers;
    profile.identifiers = uniqueStrings([
      ...(profile.identifiers || []),
      ...identifiers.map(identifier => identifier.key)
    ]);

    if (!profile.attributes.email && metadata.email) profile.attributes.email = normalizeEmail(metadata.email);
    if (!profile.attributes.phone && metadata.phone) profile.attributes.phone = normalizePhone(metadata.phone);
    if (!profile.attributes.name && metadata.name) profile.attributes.name = cleanString(metadata.name);
    if (metadata.customer_tier) profile.attributes.customer_tier = cleanString(metadata.customer_tier);
    if (metadata.segment) profile.attributes.segment = cleanString(metadata.segment);

    profile.match_history = profile.match_history || [];
    profile.match_history.push({
      strategy,
      confidence,
      timestamp: new Date().toISOString(),
      channel
    });
    if (profile.match_history.length > 25) {
      profile.match_history = profile.match_history.slice(-25);
    }

    this.profiles.put(profile);
    this._indexProfile(profile, identifiers, rawCustomerId);
  }

  /**
   * Index a profile's identifiers and aliases in the identity index for
   * fast O(1) lookup during future resolution attempts.
   * @param {Object} profile - Customer profile
   * @param {Array<Object>} identifiers - Identifiers to index
   * @param {string} rawCustomerId - Raw customer ID to index as alias
   * @private
   */
  _indexProfile(profile, identifiers, rawCustomerId) {
    this.identityIndex.set(`direct:${profile.customer_id}`, profile.customer_id);

    for (const alias of uniqueStrings([rawCustomerId, ...(profile.aliases || [])])) {
      if (!alias) continue;
      this.identityIndex.set(`direct:${alias}`, profile.customer_id);
      this.identityIndex.set(`alias:${alias}`, profile.customer_id);
    }

    for (const identifier of identifiers) {
      this.identityIndex.set(identifier.key, profile.customer_id);
    }
  }

  /**
   * Resolve a raw customer ID to its canonical ID by checking the alias
   * and direct indexes. This handles the case where a customer contacts
   * through different channels with different IDs that have been linked.
   * @param {string} customerId - Raw customer ID to resolve
   * @returns {string|null} Canonical customer ID, or null if unresolvable
   * @private
   */
  _resolveCanonicalId(customerId) {
    if (!customerId) return null;
    return this.identityIndex.get(`alias:${customerId}`)
      || this.identityIndex.get(`direct:${customerId}`)
      || customerId;
  }

  /**
   * Get a customer profile by ID, resolving aliases to canonical ID first.
   * @param {string} customerId - Customer ID (may be alias or canonical)
   * @returns {Object|null} Customer profile, or null if not found
   */
  getProfile(customerId) {
    const canonicalId = this._resolveCanonicalId(customerId);
    return canonicalId ? this.profiles.get(canonicalId) : null;
  }

  /**
   * Get all customer profiles (shallow copies).
   * @returns {Array<Object>} All customer profiles
   */
  getAllProfiles() {
    return this.profiles.scan().map(profile => ({ ...profile }));
  }

  /**
   * Get identity resolver statistics: profile counts, index sizes, alias counts,
   * cross-channel profile count, active sessions, and match strategy distribution.
   * @returns {Object} Identity resolver statistics
   */
  getStats() {
    const allProfiles = this.profiles.scan();
    const crossChannelProfiles = allProfiles.filter(profile => (profile.channels_seen || []).length > 1).length;

    return {
      totalProfiles: this.profiles.count(),
      totalIdentities: this.identityIndex.size,
      totalAliases: allProfiles.reduce((sum, profile) => sum + Math.max(0, (profile.aliases || []).length - 1), 0),
      crossChannelProfiles,
      activeSessions: this.sessions.size,
      matchStats: { ...this.matchStats }
    };
  }
}

// ── Helper functions ──────────────────────────────────────────────────

/**
 * Add a typed identifier to the list if its value is non-empty and not already present.
 * @param {Array<Object>} list - Identifier list to append to
 * @param {string} type - Identifier type (email, phone, device, etc.)
 * @param {*} value - Raw identifier value
 */
function pushIdentifier(list, type, value) {
  const cleaned = cleanString(value);
  if (!cleaned) return;
  const key = `${type}:${cleaned}`;
  if (!list.some(identifier => identifier.key === key)) {
    list.push({ type, value: cleaned, key });
  }
}

/**
 * Organize identifiers into type-keyed buckets for efficient lookup.
 * @param {Array<Object>} identifiers - Typed identifier list
 * @returns {Object} Buckets: { direct: [], email: [], phone: [], device: [], external_profile: [], ip: [] }
 */
function identifiersToBuckets(identifiers) {
  const buckets = {
    direct: [],
    email: [],
    phone: [],
    device: [],
    external_profile: [],
    ip: []
  };

  for (const identifier of identifiers) {
    if (!buckets[identifier.type]) buckets[identifier.type] = [];
    if (!buckets[identifier.type].includes(identifier.value)) {
      buckets[identifier.type].push(identifier.value);
    }
  }

  return buckets;
}

/**
 * Merge two identifier bucket objects, deduplicating values within each bucket.
 * @param {Object} [current={}] - Existing identifier buckets
 * @param {Object} [incoming={}] - New identifier buckets to merge
 * @returns {Object} Merged buckets
 */
function mergeIdentifierBuckets(current = {}, incoming = {}) {
  const merged = {};
  const keys = uniqueStrings([...Object.keys(current), ...Object.keys(incoming)]);

  for (const key of keys) {
    merged[key] = uniqueStrings([...(current[key] || []), ...(incoming[key] || [])]);
  }

  return merged;
}

/**
 * Check if an identifier value exists in a profile's identifier bucket.
 * @param {Array<string>|undefined} bucket - Profile's identifier values for this type
 * @param {Object} identifier - Identifier to check
 * @param {string} type - Expected identifier type
 * @returns {boolean} True if the identifier is known
 */
function isKnownIdentifier(bucket, identifier, type) {
  return identifier.type === type && Array.isArray(bucket) && bucket.includes(identifier.value);
}

function normalizeEmail(value) {
  return cleanString(value).toLowerCase();
}

function normalizePhone(value) {
  const source = cleanString(value);
  if (!source) return '';

  let digits = '';
  for (let i = 0; i < source.length; i++) {
    const ch = source[i];
    if (ch >= '0' && ch <= '9') digits += ch;
  }

  if (digits.length === 11 && digits[0] === '1') return digits.slice(1);
  return digits;
}

/**
 * Normalize a person's name: lowercase, strip non-alphanumeric characters,
 * collapse whitespace. Used for probabilistic name matching.
 * @param {*} value - Raw name value
 * @returns {string} Normalized name or empty string
 */
function normalizeName(value) {
  const lower = cleanString(value).toLowerCase();
  let result = '';
  for (let i = 0; i < lower.length; i++) {
    const ch = lower[i];
    const isLetter = ch >= 'a' && ch <= 'z';
    const isNumber = ch >= '0' && ch <= '9';
    result += (isLetter || isNumber || ch === ' ') ? ch : ' ';
  }
  return result
    .split(' ')
    .filter(Boolean)
    .join(' ');
}

function cleanString(value) {
  return typeof value === 'string' ? value.trim() : '';
}

/**
 * Deduplicate and clean an array of strings, removing empties.
 * @param {Array<*>} values - Values to deduplicate
 * @returns {string[]} Unique, non-empty, trimmed strings
 */
function uniqueStrings(values) {
  const result = [];
  for (const value of values) {
    const cleaned = cleanString(value);
    if (cleaned && !result.includes(cleaned)) result.push(cleaned);
  }
  return result;
}

// ── Singleton ─────────────────────────────────────────────────────────
/** @type {IdentityResolver|null} */
let instance = null;

/**
 * Get or create the singleton IdentityResolver instance.
 * @returns {IdentityResolver} Shared resolver instance
 */
function getResolver() {
  if (!instance) instance = new IdentityResolver();
  return instance;
}

module.exports = { IdentityResolver, getResolver };
