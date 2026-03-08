/**
 * @module store/persisted-store
 * @description Write-through cache that wraps {@link module:store/memory-store|MemoryStore}
 * with SQLite persistence. Provides the same API as MemoryStore (put, get, query,
 * queryGSI, scan, delete, count, clear) while transparently persisting writes to SQLite.
 *
 * Architectural layer: **Store**
 *
 * Data flow:
 * ```
 * Reads:  caller --> MemoryStore (always in-memory, microsecond latency)
 * Writes: caller --> MemoryStore + SQLite (write-through, ~50us SQLite overhead)
 * Startup: SQLite --> MemoryStore (hydrate() loads all rows into memory)
 * ```
 *
 * Design decision: This pattern keeps read latency at in-memory speed while
 * ensuring durability across restarts. The SQLite writes are best-effort:
 * failures are logged but do not block the caller, preserving pipeline throughput.
 * In production, this maps to DynamoDB DAX (write-through cache) + DynamoDB.
 *
 * @see {@link module:store/memory-store} for the underlying in-memory store
 * @see {@link module:store/database} for the SQLite persistence layer
 * @see {@link module:config} for the PERSIST toggle
 * @requires module:store/memory-store — in-memory backing store
 * @requires module:store/database — SQLite database access
 * @requires module:config — PERSIST flag
 * @requires module:logger — structured logging
 */
const { MemoryStore } = require('./memory-store');
const { getDatabase } = require('./database');
const config = require('../config');
const { createLogger } = require('../logger');

const log = createLogger('persisted-store');

/**
 * Write-through cached store combining MemoryStore speed with SQLite durability.
 * When config.PERSIST is false, behaves identically to a plain MemoryStore.
 */
class PersistedStore {
  /**
   * @param {string} name - Store name (used in logging and diagnostics)
   * @param {Object} memoryConfig - MemoryStore schema configuration
   * @param {string} memoryConfig.partitionKey - Partition key field name
   * @param {string} [memoryConfig.sortKey] - Sort key field name
   * @param {Array} [memoryConfig.gsis] - GSI definitions
   * @param {Object} persistConfig - SQLite persistence configuration
   * @param {string} persistConfig.upsertSQL - Parameterized INSERT OR REPLACE SQL
   * @param {string} [persistConfig.deleteSQL] - Parameterized DELETE SQL (null to skip)
   * @param {string} [persistConfig.selectAllSQL] - SELECT SQL for hydration (null to skip)
   * @param {Function} persistConfig.serialize - Transform item to SQL row: (item) => row object
   * @param {Function} persistConfig.deserialize - Transform SQL row to item: (row) => item
   */
  constructor(name, memoryConfig, persistConfig) {
    this.store = new MemoryStore(name, memoryConfig);
    this.name = name;
    // Disable persistence entirely when config.PERSIST is false
    this.persist = config.PERSIST ? persistConfig : null;
    /** @type {Object|null} Lazily prepared SQL statements */
    this._statements = null;
  }

  /**
   * Lazily prepare SQL statements on first use. Caches prepared statements
   * for subsequent operations to avoid repeated parsing overhead.
   * @returns {Object|null} Prepared statements { upsert, deleteOne, selectAll }, or null
   * @private
   */
  _getStatements() {
    if (this._statements) return this._statements;
    if (!this.persist) return null;

    let db;
    try { db = getDatabase(); } catch { return null; }
    const pc = this.persist;

    this._statements = {
      upsert: db.prepare(pc.upsertSQL),
      deleteOne: pc.deleteSQL ? db.prepare(pc.deleteSQL) : null,
      selectAll: pc.selectAllSQL ? db.prepare(pc.selectAllSQL) : null
    };

    return this._statements;
  }

  /**
   * Store an item in both memory and SQLite. The memory write always succeeds;
   * SQLite failures are logged but do not propagate (best-effort persistence).
   * @param {Object} item - Item to store
   * @returns {Object} The stored item
   */
  put(item) {
    const result = this.store.put(item);

    if (this.persist) {
      try {
        const stmts = this._getStatements();
        if (stmts && stmts.upsert) {
          const row = this.persist.serialize(item);
          stmts.upsert.run(row);
        }
      } catch (err) {
        log.error({ err, store: this.name }, 'SQLite write failed');
      }
    }

    return result;
  }

  /**
   * Retrieve an item from memory (fast path, no SQLite hit).
   * @param {string} pk - Partition key
   * @param {string} [sk] - Sort key
   * @returns {Object|null} The item, or null if not found
   */
  get(pk, sk) {
    return this.store.get(pk, sk);
  }

  /**
   * Query items within a partition from memory.
   * @param {string} pk - Partition key
   * @param {Object} [options] - Query options (skPrefix, skBetween, limit, scanForward)
   * @returns {Array<Object>} Matching items
   */
  query(pk, options) {
    return this.store.query(pk, options);
  }

  /**
   * Query a GSI from memory.
   * @param {string} gsiName - GSI name
   * @param {string} pk - GSI partition key
   * @param {Object} [options] - Query options
   * @returns {Array<Object>} Matching items
   */
  queryGSI(gsiName, pk, options) {
    return this.store.queryGSI(gsiName, pk, options);
  }

  /**
   * Full scan from memory with optional filter.
   * @param {Object} [options] - Scan options with optional filter predicate
   * @returns {Array<Object>} All matching items
   */
  scan(options) {
    return this.store.scan(options);
  }

  /**
   * Delete an item from both memory and SQLite.
   * @param {string} pk - Partition key
   * @param {string} [sk] - Sort key
   */
  delete(pk, sk) {
    this.store.delete(pk, sk);

    if (this.persist) {
      try {
        const stmts = this._getStatements();
        if (stmts && stmts.deleteOne) {
          stmts.deleteOne.run(pk, sk || '__default__');
        }
      } catch (err) {
        log.error({ err, store: this.name }, 'SQLite delete failed');
      }
    }
  }

  /**
   * Count items in memory.
   * @returns {number} Total item count
   */
  count() {
    return this.store.count();
  }

  /**
   * Clear all items from memory. Does NOT clear SQLite (use for testing only).
   */
  clear() {
    this.store.clear();
  }

  /**
   * Hydrate the in-memory store from SQLite. Called once at startup to restore
   * state from the previous session. Each row is deserialized and put() into
   * the MemoryStore. Rows that fail deserialization are logged and skipped.
   * @returns {number} Number of items successfully loaded
   */
  hydrate() {
    if (!this.persist) return 0;

    try {
      const stmts = this._getStatements();
      if (!stmts || !stmts.selectAll) return 0;

      const rows = stmts.selectAll.all();
      let loaded = 0;

      for (const row of rows) {
        try {
          const item = this.persist.deserialize(row);
          if (item) {
            this.store.put(item);
            loaded++;
          }
        } catch (err) {
          log.warn({ err, store: this.name }, 'Failed to deserialize row');
        }
      }

      log.info({ store: this.name, loaded }, 'Hydrated from SQLite');
      return loaded;
    } catch (err) {
      log.error({ err, store: this.name }, 'Hydration failed');
      return 0;
    }
  }
}

module.exports = { PersistedStore };
