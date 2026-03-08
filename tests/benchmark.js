/**
 * @module nexus-cx/tests/benchmark
 * @description Performance benchmark suite for the Nexus CX Intelligence Platform.
 *
 * Measures throughput, latency percentiles, memory consumption, startup time,
 * and database write performance to ensure the platform meets production SLAs.
 *
 * Benchmark categories and pass/fail thresholds:
 *   - Classification throughput:  >40 classifications/sec (embedding mode; >10k/sec TF-IDF)
 *   - Full pipeline throughput:   >50 events/sec end-to-end
 *   - Concurrent event latency:   p50 <50ms, p95 <200ms, p99 <500ms
 *   - Memory footprint:           RSS <200MB after processing 1,000+ events
 *   - Warm startup (cached):      <3 seconds for classifier rebuild from embedding cache
 *   - SQLite write throughput:    >1,000 inserts/sec (transactional batch)
 *
 * This suite runs directly against the in-memory pipeline (no HTTP server).
 * Results can be saved to data/benchmark-report.json with the --report flag.
 *
 * Run:
 *   node tests/benchmark.js            # console output only
 *   node tests/benchmark.js --report   # also save JSON report
 *
 * @requires perf_hooks
 * @requires ../src/config
 * @requires ../src/classifier/classifier
 * @requires ../src/pipeline
 * @requires ../src/events/schema
 * @requires ../src/store/database
 *
 * @see {@link ../src/pipeline.js} Pipeline under benchmark
 * @see {@link ../src/classifier/classifier.js} Classifier under benchmark
 */
const { performance } = require('perf_hooks');

/** @type {number} Running count of passing benchmarks */
let passed = 0;
/** @type {number} Running count of failing benchmarks */
let failed = 0;
/** @type {Array<{ name: string, passed: boolean, detail: string }>} Detailed results for report output */
const results = [];

/**
 * Records a benchmark assertion result with an optional detail string.
 *
 * @param {string} name - Benchmark name and threshold description
 * @param {boolean} condition - Whether the benchmark passed its threshold
 * @param {string} [detail] - Measured value (e.g., '1234/sec in 405ms')
 */
function assert(name, condition, detail) {
  if (condition) {
    passed++;
    console.log('  PASS  ' + name + (detail ? ' (' + detail + ')' : ''));
  } else {
    failed++;
    console.log('  FAIL  ' + name + (detail ? ' (' + detail + ')' : ''));
  }
  results.push({ name, passed: condition, detail });
}

/**
 * Computes the value at a given percentile from a pre-sorted array.
 * Uses the ceiling method: the smallest value at or above the target rank.
 *
 * @param {number[]} sorted - Array of numbers sorted in ascending order
 * @param {number} p - Percentile to compute (0-100)
 * @returns {number} The value at the given percentile
 */
function percentile(sorted, p) {
  const idx = Math.ceil(sorted.length * p / 100) - 1;
  return sorted[Math.max(0, idx)];
}

/**
 * Benchmarks raw classification throughput.
 * Runs 500 classifications across 10 diverse phrases after a warmup pass.
 * The threshold accounts for Ollama network round-trip (~17ms per embedding call).
 * TF-IDF fallback (no Ollama) typically achieves >10k/sec.
 *
 * @async
 */
async function benchmarkClassification() {
  console.log('\n--- Classification Throughput ---');
  const { getClassifier } = require('../src/classifier/classifier');
  const classifier = await getClassifier();

  const phrases = [
    'I was charged twice on my bill',
    'My internet is not working at all',
    'I want to cancel my subscription',
    'Can you tell me about your premium plans',
    'I need to reset my password right now',
    'How do I replace a lost Treasury check',
    'I need help filing my quarterly tax return',
    'My savings bond matured and I want to redeem it',
    'There is an error on my IRS transcript',
    'I want to report suspicious financial activity'
  ];

  // Warm up: first classification may incur cold-path overhead
  // (lazy model loading, JIT compilation, first Ollama connection)
  await classifier.classify(phrases[0]);

  const count = 500;
  const t0 = performance.now();
  for (let i = 0; i < count; i++) {
    await classifier.classify(phrases[i % phrases.length]);
  }
  const elapsed = performance.now() - t0;
  const perSec = Math.round(count / (elapsed / 1000));

  assert('Classification throughput >40/sec', perSec > 40, perSec + '/sec in ' + Math.round(elapsed) + 'ms');
}

/**
 * Benchmarks full pipeline throughput (all five layers).
 * Processes 200 events across 20 simulated customers and 6 channels.
 * Uses _processEvent() directly to bypass the Event Fabric's async
 * queue and measure raw processing speed.
 *
 * @async
 */
async function benchmarkPipeline() {
  console.log('\n--- Full Pipeline Throughput ---');
  const { getPipeline } = require('../src/pipeline');
  const pipeline = await getPipeline();
  const { createEvent } = require('../src/events/schema');

  const phrases = [
    'I need a refund for the double charge on my account',
    'The app crashes every time I open billing settings',
    'I want to upgrade to enterprise tier',
    'Cancel my service immediately please',
    'Help me resolve this payment dispute',
    'My internet connection keeps dropping out',
    'What promotions are available right now',
    'I need to update my mailing address',
    'Your support agent was amazing thank you',
    'Where can I find my tax documents'
  ];

  const channels = ['voice', 'chat', 'web', 'mobile', 'email', 'sms'];
  const count = 200;
  const t0 = performance.now();

  for (let i = 0; i < count; i++) {
    const event = createEvent({
      customerId: 'bench-' + (i % 20),
      channel: channels[i % channels.length],
      eventType: 'interaction',
      content: phrases[i % phrases.length]
    });
    await pipeline._processEvent(event);
  }

  const elapsed = performance.now() - t0;
  const perSec = Math.round(count / (elapsed / 1000));

  assert('Pipeline throughput >50/sec', perSec > 50, perSec + '/sec in ' + Math.round(elapsed) + 'ms');
}

/**
 * Benchmarks concurrent event processing latency.
 * Fires batches of 10 concurrent events across 10 rounds (100 total)
 * and measures per-event latency. Reports p50, p95, and p99 percentiles.
 * This simulates burst traffic patterns typical in production CX systems
 * where multiple agents submit events simultaneously.
 *
 * @async
 */
async function benchmarkConcurrency() {
  console.log('\n--- Concurrent Event Latency ---');
  const { getPipeline } = require('../src/pipeline');
  const pipeline = await getPipeline();
  const { createEvent } = require('../src/events/schema');

  const concurrency = 10;
  const rounds = 10;
  const latencies = [];

  for (let r = 0; r < rounds; r++) {
    const promises = [];
    for (let i = 0; i < concurrency; i++) {
      const event = createEvent({
        customerId: 'conc-' + (r * concurrency + i) % 15,
        channel: ['voice', 'chat', 'web'][i % 3],
        eventType: 'interaction',
        content: 'Concurrent test event number ' + (r * concurrency + i) + ' about billing issues'
      });
      const t = performance.now();
      promises.push(pipeline._processEvent(event).then(() => {
        latencies.push(performance.now() - t);
      }));
    }
    await Promise.all(promises);
  }

  latencies.sort((a, b) => a - b);
  const p50 = Math.round(percentile(latencies, 50) * 100) / 100;
  const p95 = Math.round(percentile(latencies, 95) * 100) / 100;
  const p99 = Math.round(percentile(latencies, 99) * 100) / 100;

  assert('Concurrent p50 <50ms', p50 < 50, 'p50=' + p50 + 'ms');
  assert('Concurrent p95 <200ms', p95 < 200, 'p95=' + p95 + 'ms');
  assert('Concurrent p99 <500ms', p99 < 500, 'p99=' + p99 + 'ms');
}

/**
 * Measures resident memory after all benchmarks have run.
 * Forces garbage collection if the --expose-gc V8 flag is present.
 * The 200MB threshold ensures the platform stays within a single
 * small container's memory allocation.
 *
 * @async
 */
async function benchmarkMemory() {
  console.log('\n--- Memory Usage ---');
  if (global.gc) global.gc();
  const mem = process.memoryUsage();
  const rssMB = Math.round(mem.rss / 1024 / 1024);
  const heapMB = Math.round(mem.heapUsed / 1024 / 1024);

  assert('RSS <200MB after benchmarks', rssMB < 200, 'RSS=' + rssMB + 'MB, Heap=' + heapMB + 'MB');
}

/**
 * Measures classifier startup time from a warm cache.
 * Creates a fresh Classifier instance and calls init(), which
 * rebuilds the embedding index from the cached embedding file
 * (if available) or falls back to TF-IDF. The 3-second threshold
 * ensures fast container restarts and rolling deployments.
 *
 * @async
 */
async function benchmarkStartup() {
  console.log('\n--- Warm Startup Time ---');
  const { Classifier } = require('../src/classifier/classifier');
  const t0 = performance.now();
  const fresh = new Classifier();
  await fresh.init();
  const elapsed = Math.round(performance.now() - t0);

  assert('Warm startup (classifier) <3s', elapsed < 3000, elapsed + 'ms');
}

/**
 * Benchmarks SQLite write throughput using transactional batch inserts.
 * Inserts 1,000 customer profile rows in a single transaction and measures
 * operations per second. Skipped when PERSIST=false. Cleans up benchmark
 * rows after measurement to avoid polluting the database.
 *
 * @async
 */
async function benchmarkSQLite() {
  console.log('\n--- SQLite Write Throughput ---');
  const config = require('../src/config');
  if (!config.PERSIST) {
    console.log('  SKIP  SQLite benchmarks (PERSIST=false)');
    return;
  }

  const { getDatabase } = require('../src/store/database');
  const db = getDatabase();

  // Prepare a parameterized statement for bulk insertion
  const stmt = db.prepare(
    'INSERT OR REPLACE INTO customer_profiles (customer_id, data, created_at, updated_at) VALUES (?, ?, ?, ?)'
  );

  const count = 1000;
  const now = new Date().toISOString();
  const t0 = performance.now();

  // Wrap all inserts in a single transaction for maximum throughput.
  // SQLite performance drops dramatically without explicit transactions
  // because each individual INSERT would trigger a separate fsync.
  const insertMany = db.transaction(() => {
    for (let i = 0; i < count; i++) {
      stmt.run(
        'bench-sqlite-' + i,
        JSON.stringify({ customer_id: 'bench-sqlite-' + i, tier: 'standard' }),
        now,
        now
      );
    }
  });
  insertMany();

  const elapsed = performance.now() - t0;
  const perSec = Math.round(count / (elapsed / 1000));

  assert('SQLite writes >1000/sec', perSec > 1000, perSec + '/sec in ' + Math.round(elapsed) + 'ms');

  // Clean up benchmark rows to avoid inflating profile counts
  db.prepare("DELETE FROM customer_profiles WHERE customer_id LIKE 'bench-sqlite-%'").run();
}

/**
 * Main benchmark runner. Initializes persistence (if enabled), then
 * runs all benchmark functions sequentially. Reports aggregate results
 * and optionally writes a JSON report to data/benchmark-report.json.
 *
 * Benchmark execution order:
 *   1. Startup -- measures classifier init (least side effects)
 *   2. Classification -- pure classifier throughput
 *   3. Pipeline -- full five-layer processing
 *   4. Concurrency -- parallel event burst latency
 *   5. Memory -- RSS measurement (after all allocations)
 *   6. SQLite -- database I/O (isolated from pipeline benchmarks)
 *
 * @async
 */
async function main() {
  console.log('');
  console.log('  ====================================================');
  console.log('  CX Intelligence Platform - Performance Benchmarks');
  console.log('  ====================================================');

  // Initialize database before benchmarks that may need persistence
  const config = require('../src/config');
  if (config.PERSIST) {
    const { initDatabase } = require('../src/store/database');
    initDatabase();
  }

  const totalStart = performance.now();

  await benchmarkStartup();
  await benchmarkClassification();
  await benchmarkPipeline();
  await benchmarkConcurrency();
  await benchmarkMemory();
  await benchmarkSQLite();

  const totalMs = Math.round(performance.now() - totalStart);

  console.log('');
  console.log('  ========================================');
  console.log('   Results: ' + passed + ' passed, ' + failed + ' failed (' + totalMs + 'ms)');
  console.log('  ========================================');
  console.log('');

  // Optionally write a JSON report for CI/CD artifact collection
  if (process.argv.includes('--report')) {
    const report = {
      timestamp: new Date().toISOString(),
      results,
      totals: { passed, failed, durationMs: totalMs }
    };
    const fs = require('fs');
    const path = require('path');
    const reportPath = path.join(__dirname, '..', 'data', 'benchmark-report.json');
    fs.mkdirSync(path.dirname(reportPath), { recursive: true });
    fs.writeFileSync(reportPath, JSON.stringify(report, null, 2));
    console.log('  Report written to ' + reportPath);
  }

  process.exit(failed > 0 ? 1 : 0);
}

main().catch(err => {
  console.error('Benchmark error:', err);
  process.exit(1);
});
