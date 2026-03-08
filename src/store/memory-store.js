/**
 * @module store/memory-store
 * @description In-memory data store that simulates DynamoDB's key-value access patterns
 * locally. Provides partition key + sort key addressing, Global Secondary Index (GSI)
 * support, and basic query/scan operations. Used as the fast-path read store throughout
 * the platform and as the backing store for {@link module:store/persisted-store}.
 *
 * Architectural layer: **Store**
 *
 * Design decision: By mirroring DynamoDB's API surface (put/get/query/scan/GSI), the
 * codebase can run fully offline while remaining structurally compatible with a
 * production DynamoDB deployment. The partition+sort key model enforces access patterns
 * that translate directly to DynamoDB table designs.
 *
 * @see {@link module:store/persisted-store} for the write-through SQLite wrapper
 * @see {@link module:store/database} for the SQLite persistence layer
 */

/**
 * In-memory key-value store with DynamoDB-compatible access patterns.
 * Items are stored in a two-level Map: partition key -> sort key -> item.
 * GSIs are maintained as secondary Map structures updated on every put().
 */
class MemoryStore {
  /**
   * @param {string} name - Human-readable store name (used in logging)
   * @param {Object} options - Store schema definition
   * @param {string} options.partitionKey - Property name used as the partition key
   * @param {string|null} [options.sortKey=null] - Property name used as the sort key
   * @param {Array<{name: string, partitionKey: string, sortKey?: string}>} [options.gsis=[]] - GSI definitions
   * @example
   * const store = new MemoryStore('journeys', {
   *   partitionKey: 'customer_id',
   *   sortKey: 'journey_key',
   *   gsis: [{ name: 'by-type', partitionKey: 'journey_type', sortKey: 'current_state' }]
   * });
   */
  constructor(name, { partitionKey, sortKey = null, gsis = [] } = {}) {
    this.name = name;
    this.partitionKey = partitionKey;
    this.sortKey = sortKey;
    this.gsis = gsis;
    /** @type {Map<string, Map<string, Object>>} Primary index: pk -> sk -> item */
    this.items = new Map();
    /** @type {Map<string, Map<string, Map<string, Object>>>} GSI indexes: gsiName -> gsiPk -> gsiSk -> item */
    this.gsiIndexes = new Map();
    for (const gsi of gsis) {
      this.gsiIndexes.set(gsi.name, new Map());
    }
  }

  /**
   * Extract the partition key and sort key from an item.
   * Uses '__default__' as a sentinel sort key when no sort key is configured.
   * @param {Object} item - Item to extract keys from
   * @returns {{pk: string, sk: string}} Partition and sort key values
   * @private
   */
  _key(item) {
    const pk = item[this.partitionKey];
    const sk = this.sortKey ? item[this.sortKey] : '__default__';
    return { pk, sk };
  }

  /**
   * Insert or overwrite an item. Updates both the primary index and all GSIs.
   * Items are shallow-cloned on write to prevent external mutation.
   * @param {Object} item - Item to store (must contain partition key field)
   * @returns {Object} The stored item
   */
  put(item) {
    const { pk, sk } = this._key(item);
    if (!this.items.has(pk)) this.items.set(pk, new Map());
    this.items.get(pk).set(sk, { ...item });

    // Maintain GSI consistency on every write
    for (const gsi of this.gsis) {
      const gsiPk = item[gsi.partitionKey];
      const gsiSk = gsi.sortKey ? item[gsi.sortKey] : '__default__';
      if (gsiPk !== undefined) {
        const idx = this.gsiIndexes.get(gsi.name);
        if (!idx.has(gsiPk)) idx.set(gsiPk, new Map());
        idx.get(gsiPk).set(gsiSk, { ...item });
      }
    }
    return item;
  }

  /**
   * Retrieve a single item by partition key and optional sort key.
   * @param {string} pk - Partition key value
   * @param {string} [sk] - Sort key value (defaults to '__default__')
   * @returns {Object|null} The item, or null if not found
   */
  get(pk, sk) {
    sk = sk || '__default__';
    const partition = this.items.get(pk);
    if (!partition) return null;
    return partition.get(sk) || null;
  }

  /**
   * Query items within a single partition. Supports sort key prefix filtering,
   * range queries (skBetween), result limiting, and sort direction control.
   * Mirrors DynamoDB Query operation semantics.
   * @param {string} pk - Partition key value
   * @param {Object} [options] - Query options
   * @param {string} [options.skPrefix] - Sort key prefix filter
   * @param {[string, string]} [options.skBetween] - Sort key range [start, end] (inclusive)
   * @param {number} [options.limit] - Maximum items to return
   * @param {boolean} [options.scanForward=true] - Sort ascending (true) or descending (false)
   * @returns {Array<Object>} Matching items
   */
  query(pk, { skPrefix, skBetween, limit, scanForward = true } = {}) {
    const partition = this.items.get(pk);
    if (!partition) return [];

    let entries = [...partition.entries()];

    if (skPrefix) {
      entries = entries.filter(([sk]) => sk.startsWith(skPrefix));
    }
    if (skBetween) {
      const [start, end] = skBetween;
      entries = entries.filter(([sk]) => sk >= start && sk <= end);
    }

    entries.sort((a, b) => scanForward ? a[0].localeCompare(b[0]) : b[0].localeCompare(a[0]));

    if (limit) entries = entries.slice(0, limit);
    return entries.map(([, item]) => item);
  }

  /**
   * Query a Global Secondary Index by its partition key.
   * @param {string} gsiName - Name of the GSI to query
   * @param {string} pk - GSI partition key value
   * @param {Object} [options] - Query options
   * @param {number} [options.limit] - Maximum items to return
   * @param {boolean} [options.scanForward=true] - Sort ascending (true) or descending (false)
   * @returns {Array<Object>} Matching items
   * @throws {Error} If the named GSI does not exist
   */
  queryGSI(gsiName, pk, { limit, scanForward = true } = {}) {
    const idx = this.gsiIndexes.get(gsiName);
    if (!idx) throw new Error(`GSI ${gsiName} not found`);
    const partition = idx.get(pk);
    if (!partition) return [];

    let entries = [...partition.entries()];
    entries.sort((a, b) => scanForward ? a[0].localeCompare(b[0]) : b[0].localeCompare(a[0]));
    if (limit) entries = entries.slice(0, limit);
    return entries.map(([, item]) => item);
  }

  /**
   * Full table scan with optional filter predicate.
   * Equivalent to DynamoDB Scan — use sparingly on large datasets.
   * @param {Object} [options] - Scan options
   * @param {Function} [options.filter] - Predicate function: (item) => boolean
   * @returns {Array<Object>} All matching items
   */
  scan({ filter } = {}) {
    const results = [];
    for (const partition of this.items.values()) {
      for (const item of partition.values()) {
        if (!filter || filter(item)) results.push(item);
      }
    }
    return results;
  }

  /**
   * Delete a single item by partition key and optional sort key.
   * @param {string} pk - Partition key value
   * @param {string} [sk] - Sort key value (defaults to '__default__')
   */
  delete(pk, sk) {
    sk = sk || '__default__';
    const partition = this.items.get(pk);
    if (partition) partition.delete(sk);
  }

  /**
   * Count total items across all partitions.
   * @returns {number} Total item count
   */
  count() {
    let c = 0;
    for (const partition of this.items.values()) c += partition.size;
    return c;
  }

  /**
   * Remove all items from the primary index and all GSIs.
   */
  clear() {
    this.items.clear();
    for (const idx of this.gsiIndexes.values()) idx.clear();
  }
}

module.exports = { MemoryStore };
