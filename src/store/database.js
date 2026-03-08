/**
 * @module store/database
 * @description SQLite persistence layer using better-sqlite3. Provides the durable backing
 * store for all platform state: customer profiles, journey records, transition history,
 * NBA action logs, event archives, embedding caches, and dead letters.
 *
 * Architectural layer: **Store**
 *
 * Configuration:
 * - WAL (Write-Ahead Logging) mode: enables concurrent reads while writes are in-flight
 * - NORMAL synchronous mode: balances durability with write throughput (~50us per write)
 * - Foreign keys enabled for referential integrity
 *
 * The database file is created at `{DATA_DIR}/cx-platform.db` and migrations run
 * automatically on first initialization. Schema changes are idempotent
 * (CREATE TABLE IF NOT EXISTS / CREATE INDEX IF NOT EXISTS).
 *
 * In production, this local SQLite layer would be replaced by DynamoDB (profiles,
 * journeys), Timestream (metrics), and S3 (event archive). The table schemas are
 * designed to map directly to their AWS counterparts.
 *
 * @see {@link module:store/persisted-store} for the write-through cache that wraps this module
 * @see {@link module:store/memory-store} for the in-memory fast path
 * @see {@link module:config} for PERSIST and DATA_DIR settings
 * @requires better-sqlite3 — synchronous SQLite3 binding for Node.js
 * @requires module:config — DATA_DIR, PERSIST settings
 * @requires module:logger — structured logging
 */
const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');
const config = require('../config');
const { createLogger } = require('../logger');

const log = createLogger('database');

/** @type {import('better-sqlite3').Database|null} Singleton database connection */
let db = null;

/**
 * Get the initialized database instance.
 * @returns {import('better-sqlite3').Database} The SQLite database connection
 * @throws {Error} If initDatabase() has not been called
 */
function getDatabase() {
  if (!db) throw new Error('Database not initialized — call initDatabase() first');
  return db;
}

/**
 * Initialize the SQLite database. Creates the data directory if it does not exist,
 * opens the database file, configures pragmas (WAL, NORMAL sync, foreign keys),
 * and runs all migrations. Idempotent: returns the existing connection if already initialized.
 *
 * @param {string} [dataDir] - Override directory for the database file (defaults to config.DATA_DIR)
 * @returns {import('better-sqlite3').Database} The initialized database connection
 */
function initDatabase(dataDir) {
  if (db) return db;

  const dir = dataDir || config.DATA_DIR;
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }

  const dbPath = path.join(dir, 'cx-platform.db');
  db = new Database(dbPath);

  // WAL mode allows readers to proceed concurrently with a single writer
  db.pragma('journal_mode = WAL');
  // NORMAL sync: fsync on WAL checkpoint only (not every commit) for throughput
  db.pragma('synchronous = NORMAL');
  db.pragma('foreign_keys = ON');

  runMigrations(db);

  const stats = fs.statSync(dbPath);
  log.info({ path: dbPath, sizeKB: Math.round(stats.size / 1024) }, 'Database initialized');

  return db;
}

/**
 * Execute all schema migrations. Every statement is idempotent (IF NOT EXISTS).
 *
 * Tables:
 * - customer_profiles: Unified customer identity records (keyed by customer_id)
 * - journeys: Active and completed journey state machines (composite key: customer_id + journey_key)
 * - journey_transitions: Append-only log of all state transitions across all journeys
 * - nba_actions: Append-only log of all NBA actions triggered
 * - metrics_snapshots: Periodic counter/gauge/KPI snapshots for recovery after restart
 * - events_archive: Compressed event records for historical analysis
 * - embedding_cache: Pre-computed embedding vectors keyed by text hash (avoids re-embedding on restart)
 * - dead_letters: Events that failed pipeline processing, preserved for debugging
 *
 * @param {import('better-sqlite3').Database} database - Database connection to migrate
 * @private
 */
function runMigrations(database) {
  const statements = [
    // ── Customer Profiles ───────────────────────────────────────────────
    `CREATE TABLE IF NOT EXISTS customer_profiles (
      customer_id TEXT PRIMARY KEY,
      data TEXT NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )`,

    // ── Journeys ────────────────────────────────────────────────────────
    `CREATE TABLE IF NOT EXISTS journeys (
      customer_id TEXT NOT NULL,
      journey_key TEXT NOT NULL,
      journey_id TEXT NOT NULL,
      journey_type TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'active',
      data TEXT NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (customer_id, journey_key)
    )`,

    `CREATE INDEX IF NOT EXISTS idx_journeys_type_status ON journeys (journey_type, status)`,
    `CREATE INDEX IF NOT EXISTS idx_journeys_status_updated ON journeys (status, updated_at)`,

    // ── Journey Transitions (append-only audit log) ─────────────────────
    `CREATE TABLE IF NOT EXISTS journey_transitions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      journey_id TEXT NOT NULL,
      customer_id TEXT NOT NULL,
      journey_type TEXT NOT NULL,
      from_state TEXT,
      to_state TEXT NOT NULL,
      trigger_intent TEXT NOT NULL,
      channel TEXT NOT NULL,
      data TEXT,
      timestamp TEXT NOT NULL
    )`,

    `CREATE INDEX IF NOT EXISTS idx_transitions_journey ON journey_transitions (journey_id)`,
    `CREATE INDEX IF NOT EXISTS idx_transitions_timestamp ON journey_transitions (timestamp)`,

    // ── NBA Actions (append-only action log) ────────────────────────────
    `CREATE TABLE IF NOT EXISTS nba_actions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      action_id TEXT NOT NULL,
      action_type TEXT NOT NULL,
      urgency TEXT,
      data TEXT NOT NULL,
      timestamp TEXT NOT NULL
    )`,

    `CREATE INDEX IF NOT EXISTS idx_nba_timestamp ON nba_actions (timestamp)`,

    // ── Metrics Snapshots ───────────────────────────────────────────────
    `CREATE TABLE IF NOT EXISTS metrics_snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      counters TEXT NOT NULL,
      gauges TEXT NOT NULL,
      kpis TEXT NOT NULL,
      timestamp TEXT NOT NULL
    )`,

    `CREATE INDEX IF NOT EXISTS idx_metrics_timestamp ON metrics_snapshots (timestamp)`,

    // ── Event Archive ───────────────────────────────────────────────────
    `CREATE TABLE IF NOT EXISTS events_archive (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      event_id TEXT NOT NULL UNIQUE,
      customer_id TEXT NOT NULL,
      channel TEXT NOT NULL,
      event_type TEXT NOT NULL DEFAULT 'interaction',
      classification_domain TEXT,
      classification_intent TEXT,
      data TEXT NOT NULL,
      timestamp TEXT NOT NULL
    )`,

    `CREATE INDEX IF NOT EXISTS idx_events_customer ON events_archive (customer_id)`,
    `CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events_archive (timestamp)`,

    // ── Embedding Cache ─────────────────────────────────────────────────
    // Stores pre-computed centroid vectors so the classifier can skip
    // re-embedding on restart when the taxonomy has not changed.
    `CREATE TABLE IF NOT EXISTS embedding_cache (
      text_hash TEXT PRIMARY KEY,
      vector BLOB NOT NULL,
      model TEXT NOT NULL,
      created_at TEXT NOT NULL
    )`,

    // ── Dead Letters ────────────────────────────────────────────────────
    // Events that failed pipeline processing are captured here for
    // post-mortem analysis rather than being silently dropped.
    `CREATE TABLE IF NOT EXISTS dead_letters (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      event_id TEXT,
      error TEXT NOT NULL,
      event_data TEXT,
      timestamp TEXT NOT NULL
    )`,

    `CREATE INDEX IF NOT EXISTS idx_dead_letters_timestamp ON dead_letters (timestamp)`
  ];

  for (const sql of statements) {
    database.prepare(sql).run();
  }
}

/**
 * Close the database connection and release resources.
 * Called during graceful shutdown.
 */
function closeDatabase() {
  if (db) {
    db.close();
    log.info('Database closed');
    db = null;
  }
}

/**
 * Get database statistics: file size and row counts for all primary tables.
 * Used by the health check and status endpoints.
 * @returns {{path: string, sizeKB: number, counts: Object}|null} Stats object, or null if DB is not initialized
 */
function getDatabaseStats() {
  if (!db) return null;
  const dir = config.DATA_DIR;
  const dbPath = path.join(dir, 'cx-platform.db');
  try {
    const stats = fs.statSync(dbPath);
    const counts = {};
    const tables = ['customer_profiles', 'journeys', 'journey_transitions', 'nba_actions', 'events_archive', 'embedding_cache'];
    for (const table of tables) {
      const row = db.prepare('SELECT COUNT(*) as count FROM ' + table).get();
      counts[table] = row.count;
    }
    return { path: dbPath, sizeKB: Math.round(stats.size / 1024), counts };
  } catch {
    return null;
  }
}

module.exports = { initDatabase, getDatabase, closeDatabase, getDatabaseStats };
