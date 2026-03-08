/**
 * @module nexus-cx/tests/simulate-traffic
 * @description Traffic simulator for the Nexus CX Intelligence Platform.
 *
 * Generates realistic multi-channel customer interactions and sends them to
 * a running server instance via HTTP POST to /api/events. Used for:
 *   - Manual QA and demo preparation
 *   - Load testing with configurable concurrency
 *   - Validating cross-channel journey detection and NBA triggers
 *   - Populating the dashboard with representative data
 *
 * The simulator includes three simulation modes:
 *   1. Random traffic    -- random channel/customer/content combinations
 *   2. Journey scenarios -- scripted cross-channel sequences (billing dispute,
 *                           churn prevention) that exercise specific journey
 *                           state machine paths
 *   3. Concurrent burst  -- parallel event batches with latency measurement
 *
 * Prerequisites:
 *   The server must be running before starting the simulator.
 *   Default target: http://localhost:3143 (override with CX_API_URL env var)
 *
 * Run:
 *   node tests/simulate-traffic.js                              # 30 random events + journeys
 *   node tests/simulate-traffic.js --count 200 --concurrency 10 # load test
 *   node tests/simulate-traffic.js --no-journeys --report       # random only, save report
 *
 * CLI Options:
 *   --count N         Number of random events to generate (default: 30)
 *   --concurrency N   Parallel HTTP requests per batch (default: 1)
 *   --report          Save results to data/traffic-report.json
 *   --no-journeys     Skip scripted journey scenarios
 *   --help            Show usage information
 *
 * @requires http
 *
 * @see {@link ../server.js} Target server that processes ingested events
 * @see {@link ../src/pipeline.js} Pipeline that events flow through
 */
const http = require('http');

/** @type {string} Base URL for the target API server */
const API_URL = process.env.CX_API_URL || 'http://localhost:3143';
const API_ENDPOINT = new URL(API_URL);

// ---------------------------------------------------------------
// Sample Interaction Corpus
// ---------------------------------------------------------------
// Organized by channel to simulate realistic content patterns:
//   - voice: conversational, complete sentences, emotional language
//   - chat:  informal, shorter messages, lowercase
//   - web:   browsing/navigation descriptions (implicit intent)
//   - mobile: app-centric actions, terse descriptions
//   - email: subject-line format, formal, detailed
//   - sms:   abbreviated, minimal punctuation, keyword-driven
// ---------------------------------------------------------------

/** @type {Object.<string, string[]>} Channel-specific sample interaction texts */
const SAMPLE_INTERACTIONS = {
  voice: [
    'I cannot make a payment on my account, my card keeps getting declined',
    'My internet has been down since this morning, when will it be fixed',
    'I want to cancel my subscription effective immediately',
    'Can you explain why my bill is so high this month',
    'I need to reset my password but the email is not coming through',
    'I am extremely unhappy with the service I have been receiving',
    'Your technician was incredibly helpful, thank you so much',
    'I want to upgrade to your premium plan, what are the options',
    'There seems to be a service outage in my area, is that correct',
    'I was charged twice for the same service and need a refund'
  ],
  chat: [
    'hi, I need help with my billing statement',
    'my app keeps crashing when I try to log in',
    'can I change my plan to something cheaper',
    'I forgot my password and need to reset it',
    'what new services do you offer',
    'I want to provide some feedback about my recent experience',
    'how much would it cost to add another line',
    'my device battery is draining really fast',
    'I would like to update my email address on file',
    'is there a discount for long-term customers'
  ],
  web: [
    'browsing pricing page for premium plans',
    'comparing service tier features',
    'viewing account settings and profile',
    'checking service status page for outages',
    'reading help articles about connectivity issues',
    'filling out support form for billing question',
    'exploring new product offerings',
    'attempting self-service password reset',
    'reviewing current plan details',
    'looking at upgrade options and promotions'
  ],
  mobile: [
    'opened app to check data usage',
    'app crashed while loading account page',
    'tapped notification about plan renewal',
    'used mobile to pay monthly bill',
    'checked signal strength in settings',
    'viewed recent charges in mobile app',
    'submitted feedback through mobile survey',
    'browsed new phones in the mobile store',
    'attempted to change plan from mobile',
    'reported issue through mobile app support'
  ],
  email: [
    'Subject: Billing Error - I was overcharged last month and need a correction',
    'Subject: Service Cancellation Request - Please process my cancellation',
    'Subject: Technical Issue - My internet drops every evening around 8pm',
    'Subject: Account Update - Please update my mailing address',
    'Subject: Feedback - Great experience with your support team yesterday',
    'Subject: Pricing Question - What are your current promotions for new customers',
    'Subject: Complaint - Third time calling about the same issue with no resolution',
    'Subject: New Service - Interested in adding home security bundle',
    'Subject: Password Help - Cannot reset my password through the website',
    'Subject: Upgrade Request - Want to move to the business tier'
  ],
  sms: [
    'HELP cant login',
    'STOP service cancel pls',
    'bill question - why extra charge',
    'internet not working again',
    'thanks for fixing my issue',
    'want to upgrade plan',
    'payment failed what do I do',
    'new phone options?',
    'change my address please',
    'your service is terrible today'
  ]
};

/**
 * Simulated customer pool. Using a fixed set of IDs ensures that
 * the identity resolver will see repeat customers across interactions,
 * enabling journey tracking and cross-channel stitching in the simulation.
 *
 * @type {string[]}
 */
const CUSTOMER_IDS = [
  'cust-alice-001',
  'cust-bob-002',
  'cust-carol-003',
  'cust-dave-004',
  'cust-eve-005',
  'cust-frank-006',
  'cust-grace-007',
  'cust-henry-008'
];

/**
 * Returns a random element from an array.
 *
 * @param {Array} arr - Source array
 * @returns {*} A randomly selected element
 */
function randomItem(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

/**
 * Sends a single event to the server's event ingestion endpoint via HTTP POST.
 * Parses the JSON response body, falling back to raw text if parsing fails.
 *
 * @param {Object} eventData - Event payload matching POST /api/events schema
 * @param {string} eventData.customerId - Customer identifier
 * @param {string} eventData.channel - Interaction channel
 * @param {string} eventData.eventType - Event type
 * @param {string} eventData.content - Interaction content text
 * @returns {Promise<Object>} Parsed server response
 */
function postEvent(eventData) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(eventData);
    const options = {
      hostname: API_ENDPOINT.hostname,
      port: API_ENDPOINT.port || (API_ENDPOINT.protocol === 'https:' ? 443 : 80),
      path: '/api/events',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(data)
      }
    };

    const req = http.request(options, (res) => {
      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('end', () => {
        try {
          resolve(JSON.parse(body));
        } catch (e) {
          resolve({ raw: body });
        }
      });
    });

    req.on('error', reject);
    req.write(data);
    req.end();
  });
}

/**
 * Promise-based delay utility.
 *
 * @param {number} ms - Milliseconds to sleep
 * @returns {Promise<void>}
 */
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Sends random events sequentially with a variable delay (200-500ms) between
 * each to simulate organic traffic patterns. Each event uses a randomly selected
 * channel, customer, and content string from the sample corpus.
 *
 * @async
 * @param {number} [count=30] - Number of events to send
 */
async function simulateRandomTraffic(count = 30) {
  console.log('\n  Simulating ' + count + ' random events across channels...\n');

  const channels = Object.keys(SAMPLE_INTERACTIONS);

  for (let i = 0; i < count; i++) {
    const channel = randomItem(channels);
    const content = randomItem(SAMPLE_INTERACTIONS[channel]);
    const customerId = randomItem(CUSTOMER_IDS);

    try {
      const result = await postEvent({
        customerId,
        channel,
        eventType: 'interaction',
        content
      });

      const classification = result.event?.classification?.primary;
      const classStr = classification
        ? classification.fullKey + ' (' + classification.confidence + ')'
        : 'none';

      console.log(
        '  [' + (i + 1).toString().padStart(2) + '] ' +
        channel.padEnd(8) + ' | ' +
        customerId.padEnd(16) + ' | ' +
        classStr.padEnd(35) + ' | ' +
        content.substring(0, 50)
      );
    } catch (err) {
      console.log('  [' + (i + 1) + '] ERROR: ' + err.message);
    }

    // Variable delay simulates natural inter-arrival time distribution
    await sleep(200 + Math.random() * 300);
  }
}

/**
 * Simulates a scripted billing dispute journey across three channels.
 *
 * Scenario flow:
 *   1. Customer notices incorrect charge on web (browsing)
 *   2. Customer opens chat to report the billing error
 *   3. Customer confirms the error and requests a refund via chat
 *   4. Customer escalates to voice when chat cannot process the refund
 *   5. Customer confirms resolution on voice
 *
 * This exercises the billing_dispute journey definition's state machine
 * and validates cross-channel handoff tracking.
 *
 * @async
 */
async function simulateCrossChannelJourney() {
  console.log('\n  --- Cross-Channel Journey: Billing Dispute ---\n');

  const customerId = 'journey-customer-billing';

  const steps = [
    { channel: 'web', content: 'checking my billing statement online, something looks wrong with the charges', delay: 0 },
    { channel: 'chat', content: 'hi, I see an incorrect charge on my bill for last month, can you help', delay: 800 },
    { channel: 'chat', content: 'yes the charge is definitely wrong, I need this refunded please', delay: 600 },
    { channel: 'voice', content: 'I called because the chat agent could not process my refund, I need a supervisor', delay: 1000 },
    { channel: 'voice', content: 'thank you for resolving this, the refund looks correct now', delay: 500 }
  ];

  for (const step of steps) {
    if (step.delay > 0) await sleep(step.delay);

    const result = await postEvent({
      customerId,
      channel: step.channel,
      eventType: 'interaction',
      content: step.content
    });

    const classification = result.event?.classification?.primary;
    const journeyTransitions = result.event?.journey?.transitions || [];

    console.log(
      '  [' + step.channel.padEnd(7) + '] ' +
      (classification ? classification.fullKey : 'no-class').padEnd(30) +
      (journeyTransitions.length > 0
        ? ' | Journey: ' + journeyTransitions.map(t =>
            (t.from_state || 'NEW') + ' -> ' + t.to_state
          ).join(', ')
        : '')
    );
  }
}

/**
 * Simulates a scripted churn prevention journey.
 *
 * Scenario flow:
 *   1. Customer browses cancellation page on web (signal detection)
 *   2. Customer calls to explicitly cancel service (triggers churn_prevention journey)
 *   3. Customer asks about retention offers (NBA should recommend retention_offer)
 *   4. Customer accepts a discounted upgrade via chat (journey should transition to saved)
 *
 * This exercises the churn_prevention journey definition and validates that
 * the NBA engine triggers retention actions at the appropriate journey states.
 *
 * @async
 */
async function simulateChurnPrevention() {
  console.log('\n  --- Cross-Channel Journey: Churn Prevention ---\n');

  const customerId = 'journey-customer-churn';

  const steps = [
    { channel: 'web', content: 'looking at how to cancel my account', delay: 0 },
    { channel: 'voice', content: 'I want to cancel everything, I am done with this service', delay: 800 },
    { channel: 'voice', content: 'well what kind of pricing discount could you offer me to stay', delay: 600 },
    { channel: 'chat', content: 'ok I will accept the upgrade to premium at the discounted rate, thank you', delay: 500 }
  ];

  for (const step of steps) {
    if (step.delay > 0) await sleep(step.delay);

    const result = await postEvent({
      customerId,
      channel: step.channel,
      eventType: 'interaction',
      content: step.content
    });

    const classification = result.event?.classification?.primary;
    const nbaActions = result.event?.nba?.actions || [];
    const journeyTransitions = result.event?.journey?.transitions || [];

    let line = '  [' + step.channel.padEnd(7) + '] ' +
      (classification ? classification.fullKey : 'no-class').padEnd(30);

    if (journeyTransitions.length > 0) {
      line += ' | Journey: ' + journeyTransitions.map(t =>
        (t.from_state || 'NEW') + ' -> ' + t.to_state
      ).join(', ');
    }

    if (nbaActions.length > 0) {
      line += ' | NBA: ' + nbaActions.map(a => a.type + '(' + a.urgency + ')').join(', ');
    }

    console.log(line);
  }
}

/**
 * Parses CLI arguments into a structured options object.
 * Supports --count, --concurrency, --report, --no-journeys, and --help.
 *
 * @returns {{ count: number, concurrency: number, report: boolean, journeys: boolean }}
 */
function parseArgs() {
  const args = process.argv.slice(2);
  const parsed = { count: 30, concurrency: 1, report: false, journeys: true };
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--count' && args[i + 1]) { parsed.count = parseInt(args[i + 1]); i++; }
    else if (args[i] === '--concurrency' && args[i + 1]) { parsed.concurrency = parseInt(args[i + 1]); i++; }
    else if (args[i] === '--report') { parsed.report = true; }
    else if (args[i] === '--no-journeys') { parsed.journeys = false; }
    else if (args[i] === '--help') {
      console.log('  Usage: node tests/simulate-traffic.js [options]');
      console.log('    --count N         Number of random events (default: 30)');
      console.log('    --concurrency N   Parallel event batches (default: 1)');
      console.log('    --report          Save report to data/traffic-report.json');
      console.log('    --no-journeys     Skip journey scenarios');
      process.exit(0);
    }
  }
  return parsed;
}

/**
 * Sends events in parallel batches with configurable concurrency.
 * Measures per-event latency and reports throughput and percentile statistics.
 * A 50ms inter-batch delay prevents overwhelming the server's event loop
 * when running at high concurrency.
 *
 * @async
 * @param {number} count - Total number of events to send
 * @param {number} concurrency - Number of parallel requests per batch
 * @returns {{ count: number, elapsed: number, perSec: number, p50: number, p95: number }}
 */
async function simulateConcurrent(count, concurrency) {
  console.log('\n  Simulating ' + count + ' events with concurrency ' + concurrency + '...\n');
  const channels = Object.keys(SAMPLE_INTERACTIONS);
  let sent = 0;
  const latencies = [];
  const t0 = performance.now();

  while (sent < count) {
    const batch = Math.min(concurrency, count - sent);
    const promises = [];
    for (let b = 0; b < batch; b++) {
      const idx = sent + b;
      const channel = randomItem(channels);
      const content = randomItem(SAMPLE_INTERACTIONS[channel]);
      const customerId = randomItem(CUSTOMER_IDS);
      const start = performance.now();
      promises.push(
        postEvent({ customerId, channel, eventType: 'interaction', content }).then(result => {
          latencies.push(performance.now() - start);
          const classification = result.event?.classification?.primary;
          const classStr = classification
            ? classification.fullKey + ' (' + classification.confidence + ')'
            : 'none';
          console.log(
            '  [' + (idx + 1).toString().padStart(3) + '] ' +
            channel.padEnd(8) + ' | ' +
            customerId.padEnd(16) + ' | ' +
            classStr.padEnd(35) + ' | ' +
            content.substring(0, 50)
          );
        }).catch(err => {
          latencies.push(performance.now() - start);
          console.log('  [' + (idx + 1) + '] ERROR: ' + err.message);
        })
      );
    }
    await Promise.all(promises);
    sent += batch;
    // Brief pause between batches to avoid saturating the server's event loop
    if (concurrency > 1 && sent < count) await sleep(50);
  }

  const elapsed = performance.now() - t0;
  latencies.sort((a, b) => a - b);
  const p50 = Math.round(latencies[Math.floor(latencies.length * 0.5)] || 0);
  const p95 = Math.round(latencies[Math.floor(latencies.length * 0.95)] || 0);
  const perSec = Math.round(count / (elapsed / 1000));

  console.log('\n  Throughput: ' + perSec + ' events/sec | p50=' + p50 + 'ms | p95=' + p95 + 'ms');
  return { count, elapsed: Math.round(elapsed), perSec, p50, p95 };
}

/**
 * Fetches the platform status from the running server's /api/status endpoint.
 * Used after simulation completes to display final aggregate statistics.
 *
 * @async
 * @returns {Promise<Object>} Parsed platform status response
 */
async function fetchStats() {
  return new Promise((resolve, reject) => {
    http.get(API_URL + '/api/status', (res) => {
      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('end', () => resolve(JSON.parse(body)));
    }).on('error', reject);
  });
}

/**
 * Main simulator entry point. Parses CLI options, verifies server
 * connectivity with a ping event, runs selected simulation modes,
 * and displays final platform statistics. Optionally saves a JSON
 * report for CI/CD artifact collection.
 *
 * Execution order:
 *   1. Connectivity check (fail fast if server is unreachable)
 *   2. Journey scenarios (billing dispute, churn prevention) unless --no-journeys
 *   3. Random/concurrent traffic generation
 *   4. Fetch and display final platform statistics
 *   5. Optionally write report to data/traffic-report.json
 *
 * @async
 */
async function main() {
  const opts = parseArgs();
  console.log('');
  console.log('  ====================================================');
  console.log('  CX Intelligence Platform - Traffic Simulator');
  console.log('  ====================================================');

  // Connectivity check: send a lightweight lifecycle event to verify
  // the server is running and accepting requests before starting simulation
  try {
    await postEvent({ customerId: 'test-ping', channel: 'web', eventType: 'lifecycle', content: '' });
    console.log('  Server connection: OK');
  } catch (err) {
    console.error('\n  ERROR: Cannot connect to server at ' + API_URL);
    console.error('  Make sure the server is running: npm start');
    process.exit(1);
  }

  // Run scripted journey scenarios first so their events are visible
  // in the dashboard before the random traffic flood
  if (opts.journeys) {
    await simulateCrossChannelJourney();
    await simulateChurnPrevention();
  }

  // Run either concurrent burst mode or sequential random traffic
  let trafficStats;
  if (opts.concurrency > 1) {
    trafficStats = await simulateConcurrent(opts.count, opts.concurrency);
  } else {
    await simulateRandomTraffic(opts.count);
  }

  // Fetch and display final platform statistics from the server
  console.log('\n  --- Final Platform Stats ---\n');
  const statsReq = await fetchStats();

  console.log('  Events Processed:   ' + statsReq.processedCount);
  console.log('  Customer Profiles:  ' + statsReq.identityStats.totalProfiles);
  console.log('  Active Journeys:    ' + statsReq.journeyStats.active);
  console.log('  Completed Journeys: ' + statsReq.journeyStats.completed);
  console.log('  NBA Evaluations:    ' + statsReq.nbaStats.totalEvaluations);
  console.log('  NBA Actions:        ' + statsReq.nbaStats.actionsTriggered);
  console.log('  Cross-Channel:      ' + statsReq.journeyStats.crossChannelJourneys);
  console.log('');

  // Optionally persist results for CI/CD dashboards or historical tracking
  if (opts.report) {
    const fs = require('fs');
    const path = require('path');
    const report = {
      timestamp: new Date().toISOString(),
      options: opts,
      traffic: trafficStats || null,
      platform: {
        processedCount: statsReq.processedCount,
        profiles: statsReq.identityStats.totalProfiles,
        activeJourneys: statsReq.journeyStats.active,
        nbaActions: statsReq.nbaStats.actionsTriggered
      }
    };
    const reportPath = path.join(__dirname, '..', 'data', 'traffic-report.json');
    fs.mkdirSync(path.dirname(reportPath), { recursive: true });
    fs.writeFileSync(reportPath, JSON.stringify(report, null, 2));
    console.log('  Report written to ' + reportPath);
  }
}

main().catch(err => {
  console.error('Simulator error:', err);
  process.exit(1);
});
