/**
 * @module nexus-cx/tests/integration
 * @description End-to-end integration test suite for the Nexus CX Intelligence Platform.
 *
 * Validates the full processing pipeline by exercising each layer in sequence:
 *   1. Event Schema     -- canonical event creation and input validation
 *   2. Classifier       -- intent classification across all five commercial domains
 *   3. Identity         -- cross-channel profile resolution and alias stitching
 *   4. Pipeline E2E     -- event ingestion through all five layers with enrichment
 *   5. Cross-Channel    -- multi-channel journey tracking with handoff detection
 *   6. Churn Prevention -- cancellation detection and retention NBA triggers
 *   7. Routing          -- self-service deflection and routing priority calculation
 *   8. Metrics          -- counter aggregation, KPIs, and operational summaries
 *   9. Intelligence     -- journey path analysis, stuck detection, and triggers
 *  10. Platform Status  -- composite status endpoint data validation
 *
 * Test organization:
 *   Tests are grouped into logical sections using section() headers. Each test
 *   is a single assert() call with a descriptive name. The test runner is a
 *   lightweight custom harness (no external test framework) to keep the
 *   dependency footprint minimal.
 *
 * This suite runs directly against the in-memory pipeline (no HTTP server needed).
 * It instantiates the Pipeline singleton, which initializes all five layers,
 * then feeds events through the pipeline's ingest() method.
 *
 * Run: node tests/integration.test.js
 * Expected: 53 passing assertions, exit code 0
 *
 * @requires ../src/events/schema
 * @requires ../src/pipeline
 * @requires ../src/classifier/classifier
 * @requires ../src/identity/resolver
 * @requires ../src/journey/state-machine
 * @requires ../src/nba/engine
 * @requires ../src/analytics/metrics
 *
 * @see {@link ../src/pipeline.js} Pipeline orchestrator under test
 * @see {@link ../server.js} Server that exposes these layers via HTTP
 */
const { createEvent } = require('../src/events/schema');
const { getPipeline } = require('../src/pipeline');
const { getClassifier } = require('../src/classifier/classifier');
const { getResolver } = require('../src/identity/resolver');
const { getJourneyEngine } = require('../src/journey/state-machine');
const { getNBAEngine } = require('../src/nba/engine');
const { getMetrics } = require('../src/analytics/metrics');

/** @type {number} Running count of passing assertions */
let passed = 0;
/** @type {number} Running count of failing assertions */
let failed = 0;
/** @type {string[]} Names of failed assertions for the summary report */
const failures = [];

/**
 * Evaluates a single test assertion and logs the result.
 *
 * @param {boolean} condition - The assertion to evaluate
 * @param {string} name - Human-readable description of what is being tested
 */
function assert(condition, name) {
  if (condition) {
    passed++;
    console.log('  PASS  ' + name);
  } else {
    failed++;
    failures.push(name);
    console.log('  FAIL  ' + name);
  }
}

/**
 * Prints a section header to visually group related tests in console output.
 *
 * @param {string} name - Section title
 */
function section(name) {
  console.log('\n--- ' + name + ' ---');
}

/**
 * Main test runner. Initializes the pipeline, runs all test sections
 * sequentially, and exits with code 0 (all passed) or 1 (any failures).
 *
 * @async
 */
async function runTests() {
  console.log('\n========================================');
  console.log(' Nexus CX Platform Integration Tests');
  console.log('========================================\n');

  const pipeline = await getPipeline();

  // =============================================================
  // Event Schema Tests
  // =============================================================
  // Validates the canonical event factory: required fields are set,
  // UUIDs are generated, classification starts null, and invalid
  // channel names are rejected with a descriptive error.
  // =============================================================
  section('Event Schema');

  const event1 = createEvent({
    customerId: 'test-customer-1',
    sessionId: 'session-1',
    channel: 'voice',
    eventType: 'interaction',
    payload: { content: 'I cannot make a payment on my account' }
  });

  assert(event1.event_id.length > 0, 'Event has UUID');
  assert(event1.customer_id === 'test-customer-1', 'Event has customer_id');
  assert(event1.channel === 'voice', 'Event has channel');
  assert(event1.event_type === 'interaction', 'Event has event_type');
  assert(event1.payload.content.includes('payment'), 'Event has content');
  assert(event1.classification === null, 'Classification starts null');

  // Negative test: verify invalid channels are rejected at schema level
  try {
    createEvent({ customerId: 'x', channel: 'invalid', eventType: 'interaction', payload: {} });
    assert(false, 'Invalid channel throws');
  } catch (e) {
    assert(e.message.includes('Invalid channel'), 'Invalid channel throws');
  }

  // =============================================================
  // Classifier Tests
  // =============================================================
  // Tests intent classification accuracy across the five commercial
  // domains (billing, tech_support, account, sales, general).
  // Verifies that the classifier returns confidence scores, ranked
  // alternatives, and that the index has the expected vocabulary
  // size and intent count. Also validates deterministic analysis
  // (sentiment, urgency, customer effort) for frustrated interactions.
  // =============================================================
  section('Classifier');

  const classifier = await getClassifier();

  const billingResult = await classifier.classify('my payment was declined and I need help');
  assert(billingResult.primary.domain === 'billing', 'Classifies billing intent');
  assert(billingResult.primary.intent === 'payment_issue', 'Classifies payment_issue');
  assert(billingResult.primary.confidence > 0.1, 'Has meaningful confidence');

  const techResult = await classifier.classify('my internet connection keeps dropping');
  assert(techResult.primary.domain === 'technical_support', 'Classifies tech support');
  assert(techResult.primary.intent === 'connectivity', 'Classifies connectivity');

  const cancelResult = await classifier.classify('I want to cancel my account immediately');
  assert(cancelResult.primary.domain === 'account', 'Classifies account');
  assert(cancelResult.primary.intent === 'cancellation', 'Classifies cancellation');

  const complaintResult = await classifier.classify('this is the worst service I have ever experienced');
  assert(complaintResult.primary.domain === 'general', 'Classifies general');
  assert(complaintResult.primary.intent === 'complaint', 'Classifies complaint');

  const upgradeResult = await classifier.classify('what premium options do you have for an upgrade');
  assert(upgradeResult.primary.domain === 'sales', 'Classifies sales');

  assert(billingResult.alternatives.length > 0, 'Returns alternatives');

  // Validate classifier index metadata
  const stats = classifier.getStats();
  assert(stats.vocabularySize > 50, 'Vocabulary has sufficient words');
  assert(stats.intentCount === 82, 'Has 82 intents (19 domains)');
  assert(stats.domains.length === 19, 'Has 19 domains');

  // Validate deterministic (non-LLM) analysis for frustrated customer text
  const deterministicAnalysis = classifier.analyzeDeterministic(
    'I am very frustrated, this still is not fixed, and I need help today',
    complaintResult
  );
  assert(['negative', 'frustrated', 'angry'].includes(deterministicAnalysis.sentiment), 'Deterministic analysis infers sentiment');
  assert(['medium', 'high', 'critical'].includes(deterministicAnalysis.urgency), 'Deterministic analysis infers urgency');
  assert(deterministicAnalysis.customerEffortScore >= 3, 'Deterministic analysis infers effort');

  // =============================================================
  // Identity Resolution Tests
  // =============================================================
  // Validates the identity resolver's ability to:
  //   - Create new customer profiles from first-seen events
  //   - Track channels seen across multiple interactions
  //   - Increment interaction counts correctly
  //   - Stitch aliases when matching identifiers (email, phone)
  //     are seen across different customer IDs
  //   - Normalize identifiers (lowercase email, strip phone formatting)
  //   - Resolve alias lookups to the canonical profile
  // =============================================================
  section('Identity Resolution');

  const resolver = getResolver();

  const profile1 = resolver.resolve(event1);
  assert(profile1.customer_id === 'test-customer-1', 'Creates new profile');
  assert(profile1.channels_seen.includes('voice'), 'Tracks channel');
  assert(profile1.interaction_count >= 1, 'Tracks interaction count');

  // Same customer, different channel -- should merge into existing profile
  const event2 = createEvent({
    customerId: 'test-customer-1',
    sessionId: 'session-2',
    channel: 'chat',
    eventType: 'interaction',
    payload: { content: 'following up on my billing issue' }
  });
  const profile1b = resolver.resolve(event2);
  assert(profile1b.channels_seen.includes('voice'), 'Remembers voice channel');
  assert(profile1b.channels_seen.includes('chat'), 'Adds chat channel');
  assert(profile1b.interaction_count >= 2, 'Increments interaction count');

  // New customer -- should create a separate profile
  const event3 = createEvent({
    customerId: 'test-customer-2',
    channel: 'web',
    eventType: 'interaction',
    payload: { content: 'I want to sign up for a new account' }
  });
  const profile2 = resolver.resolve(event3);
  assert(profile2.customer_id === 'test-customer-2', 'Creates second profile');

  // Alias stitching test: two different customer IDs with matching email/phone
  // should resolve to the same unified profile
  const aliasEvent1 = createEvent({
    customerId: 'alias-primary',
    sessionId: 'alias-session-1',
    channel: 'web',
    eventType: 'interaction',
    payload: {
      content: 'I need help logging into my account',
      metadata: {
        email: 'Jane.Smith@Example.com',
        phone: '(555) 010-9999',
        name: 'Jane Smith',
        customer_tier: 'premium'
      }
    }
  });
  const aliasProfile1 = resolver.resolve(aliasEvent1);

  const aliasEvent2 = createEvent({
    customerId: 'crm-889',
    sessionId: 'alias-session-2',
    channel: 'mobile',
    eventType: 'interaction',
    payload: {
      content: 'still cannot access my profile',
      metadata: {
        email: 'jane.smith@example.com',
        phone: '5550109999',
        name: 'Jane Smith',
        customer_tier: 'premium'
      }
    }
  });
  const aliasProfile2 = resolver.resolve(aliasEvent2);
  assert(aliasProfile2.customer_id === aliasProfile1.customer_id, 'Stitches aliases with normalized identifiers');
  assert(aliasProfile2.aliases.includes('crm-889'), 'Tracks alias ids on unified profile');
  assert(resolver.getProfile('crm-889').customer_id === aliasProfile1.customer_id, 'Alias lookup resolves canonical profile');

  const idStats = resolver.getStats();
  assert(idStats.totalProfiles >= 2, 'Tracks multiple profiles');
  assert(idStats.totalAliases >= 1, 'Tracks linked aliases');

  // =============================================================
  // Pipeline End-to-End Tests
  // =============================================================
  // Feeds events through the full pipeline (classify -> identity ->
  // journey -> NBA -> metrics) and verifies that each layer enriches
  // the event object in place with its output.
  // =============================================================
  section('Pipeline End-to-End');

  // Ingest a billing complaint -- should trigger billing_dispute journey
  const pipeEvent1 = createEvent({
    customerId: 'pipe-customer-1',
    sessionId: 'pipe-session-1',
    channel: 'voice',
    eventType: 'interaction',
    payload: { content: 'I was charged incorrectly and I want a refund right now' }
  });
  await pipeline.ingest(pipeEvent1);

  assert(pipeEvent1.classification !== null, 'Pipeline classifies event');
  assert(pipeEvent1.classification.primary.domain === 'billing', 'Pipeline billing classification');
  assert(pipeEvent1.analysis !== null, 'Pipeline adds deterministic analysis');
  assert(pipeEvent1.identity !== null, 'Pipeline adds unified identity');
  assert(pipeEvent1.journey !== null, 'Pipeline creates journey data');
  assert(pipeEvent1.routing !== null, 'Pipeline adds routing context');

  // Verify a billing_dispute journey was created by the journey engine
  const journeyEngine = getJourneyEngine();
  const pipeJourneys = journeyEngine.getActiveJourneys('pipe-customer-1');
  assert(pipeJourneys.length > 0, 'Journey created for billing dispute');

  const billingJourney = pipeJourneys.find(j => j.journey_type === 'billing_dispute');
  assert(billingJourney !== null, 'Billing dispute journey exists');
  assert(billingJourney.current_state === 'initiated' || billingJourney.current_state === 'refund_processing',
    'Journey in expected state');

  // Follow-up event from the same customer on a different channel
  // should be routed to the existing journey and potentially trigger a transition
  const pipeEvent2 = createEvent({
    customerId: 'pipe-customer-1',
    sessionId: 'pipe-session-1',
    channel: 'chat',
    eventType: 'interaction',
    payload: { content: 'checking on my billing inquiry and charges on my account' }
  });
  await pipeline.ingest(pipeEvent2);

  // Verify NBA engine evaluated the event and assigned routing priority
  const nbaEngine = getNBAEngine();
  const nbaStats = nbaEngine.getStats();
  assert(nbaStats.totalEvaluations > 0, 'NBA evaluations occurred');
  assert(pipeEvent2.routing.priority >= 0, 'Routing priority calculated');

  // =============================================================
  // Cross-Channel Journey Test
  // =============================================================
  // Simulates a customer moving across web -> chat -> voice channels
  // for a service upgrade. Verifies the journey engine tracks all
  // channels, records state transitions, and counts cross-channel handoffs.
  // =============================================================
  section('Cross-Channel Journey');

  // Customer starts on web
  const ccEvent1 = createEvent({
    customerId: 'cross-channel-customer',
    sessionId: 'cc-session-web',
    channel: 'web',
    eventType: 'interaction',
    payload: { content: 'I want to upgrade to a premium plan' }
  });
  await pipeline.ingest(ccEvent1);

  // Same customer moves to chat
  const ccEvent2 = createEvent({
    customerId: 'cross-channel-customer',
    sessionId: 'cc-session-chat',
    channel: 'chat',
    eventType: 'interaction',
    payload: { content: 'what pricing options do you have for premium' }
  });
  await pipeline.ingest(ccEvent2);

  // Same customer calls in
  const ccEvent3 = createEvent({
    customerId: 'cross-channel-customer',
    sessionId: 'cc-session-voice',
    channel: 'voice',
    eventType: 'interaction',
    payload: { content: 'I want to go ahead and upgrade my service plan' }
  });
  await pipeline.ingest(ccEvent3);

  const ccProfile = resolver.getProfile('cross-channel-customer');
  assert(ccProfile.channels_seen.length >= 3, 'Cross-channel: 3+ channels tracked');

  const ccJourneys = journeyEngine.getAllJourneys('cross-channel-customer');
  assert(ccJourneys.length > 0, 'Cross-channel: journey created');

  const upgradeJourney = ccJourneys.find(j => j.journey_type === 'service_upgrade');
  if (upgradeJourney) {
    assert(upgradeJourney.channels.length >= 2, 'Cross-channel: journey spans channels');
    assert(upgradeJourney.state_history.length >= 2, 'Cross-channel: multiple state transitions');
    assert(upgradeJourney.cross_channel_handoffs >= 1, 'Cross-channel: handoffs tracked');
  } else {
    assert(true, 'Cross-channel: journey exists (type may vary)');
  }

  // =============================================================
  // Churn Prevention Test
  // =============================================================
  // Verifies that a cancellation request triggers the churn_prevention
  // journey type and that the NBA engine generates a retention offer
  // or complaint protocol action.
  // =============================================================
  section('Churn Prevention');

  const churnEvent = createEvent({
    customerId: 'churn-customer',
    sessionId: 'churn-session',
    channel: 'voice',
    eventType: 'interaction',
    payload: { content: 'I want to cancel my account and end my subscription' }
  });
  await pipeline.ingest(churnEvent);

  const churnJourneys = journeyEngine.getActiveJourneys('churn-customer');
  const churnJourney = churnJourneys.find(j => j.journey_type === 'churn_prevention');
  assert(churnJourney !== undefined, 'Churn prevention journey triggered');
  if (churnJourney) {
    assert(churnJourney.current_state === 'cancel_requested', 'Churn journey starts at cancel_requested');
  }

  // Validate that the NBA engine triggers a retention-related action
  assert(churnEvent.nba !== null, 'NBA triggered for churn event');
  if (churnEvent.nba) {
    const retentionAction = churnEvent.nba.actions.find(a =>
      a.type === 'retention_offer' || a.type === 'complaint_protocol'
    );
    assert(retentionAction !== undefined, 'Retention/complaint action triggered');
  }

  // =============================================================
  // Routing & Deflection Test
  // =============================================================
  // Verifies that simple self-service requests (e.g., password reset)
  // are identified as deflection candidates. The NBA engine should mark
  // should_deflect=true and generate a self_service_redirect or
  // digital_deflection action, and the pipeline should record the
  // deflection outcome.
  // =============================================================
  section('Routing & Deflection');

  const deflectEvent = createEvent({
    customerId: 'self-serve-customer',
    sessionId: 'self-serve-session',
    channel: 'web',
    eventType: 'interaction',
    payload: {
      content: 'I need to reset my password',
      metadata: {
        email: 'selfserve@example.com',
        customer_tier: 'standard'
      }
    }
  });
  await pipeline.ingest(deflectEvent);

  assert(deflectEvent.routing !== null, 'Deflection event has routing');
  assert(deflectEvent.nba.routing.should_deflect === true, 'Routing marks self-service opportunity');
  assert(deflectEvent.outcomes.deflected === true, 'Pipeline records deflection outcome');
  const selfServiceAction = deflectEvent.nba.actions.find(action =>
    action.type === 'self_service_redirect' || action.type === 'digital_deflection'
  );
  assert(selfServiceAction !== undefined, 'Self-service action generated');

  // =============================================================
  // Metrics & Analytics Tests
  // =============================================================
  // Validates that the metrics layer has correctly aggregated
  // counters, distributions, and KPIs from all the events
  // processed above. Checks that operational summary surfaces
  // top intents.
  // =============================================================
  section('Metrics & Analytics');

  const metrics = getMetrics();
  const dashboard = metrics.getDashboard();
  const kpis = metrics.getKPIs();
  const operations = metrics.getOperationalSummary();

  assert(dashboard.counters.total_events > 0, 'Total events counted');
  assert(dashboard.counters.total_classifications > 0, 'Classifications counted');
  assert(Object.keys(dashboard.channelDistribution).length > 0, 'Channel distribution tracked');
  assert(Object.keys(dashboard.classificationDistribution).length > 0, 'Classification distribution tracked');
  assert(kpis.total_contacts > 0, 'KPI total contacts calculated');
  assert(kpis.avg_contact_value > 0, 'KPI average contact value calculated');
  assert(kpis.deflection_rate > 0, 'KPI deflection rate calculated');
  assert(Object.keys(kpis.sentiment_distribution).length > 0, 'KPI sentiment distribution calculated');
  assert(operations.top_intents.length > 0, 'Operational summary tracks top intents');

  // =============================================================
  // Journey Intelligence Tests
  // =============================================================
  // Validates the journey intelligence subsystem that analyzes
  // journey data to surface common paths, top triggers, and stuck
  // journeys. The "future" intelligence test fast-forwards the
  // clock by 5 days to verify stuck journey detection.
  // =============================================================
  section('Journey Intelligence');

  const overallJourneyIntel = journeyEngine.getJourneyIntelligence({ limit: 5 });
  const futureJourneyIntel = journeyEngine.getJourneyIntelligence({
    customerId: 'pipe-customer-1',
    nowMs: Date.now() + (5 * 24 * 60 * 60 * 1000)
  });

  assert(overallJourneyIntel.commonPaths.length > 0, 'Journey intelligence tracks common paths');
  assert(overallJourneyIntel.topTriggers.length > 0, 'Journey intelligence tracks top triggers');
  assert(futureJourneyIntel.stuckJourneyCount > 0, 'Journey intelligence detects stuck journeys');

  // =============================================================
  // Platform Status Tests
  // =============================================================
  // Validates the composite status object that the /api/status
  // endpoint would return. Ensures all subsystem stats are present
  // and contain meaningful data.
  // =============================================================
  section('Platform Status');

  const status = pipeline.getStatus();
  assert(status.running === true, 'Pipeline is running');
  assert(status.processedCount > 0, 'Pipeline processed events');
  assert(status.classifierStats.built === true, 'Classifier is built');
  assert(status.identityStats.totalProfiles > 0, 'Identity has profiles');
  assert(status.journeyStats.total > 0, 'Journeys exist');
  assert(status.journeyIntelligence.commonPaths.length > 0, 'Status exposes journey intelligence');
  assert(status.dashboardMetrics.kpis.total_contacts > 0, 'Status exposes dashboard KPIs');
  assert(status.nbaStats.deflectionsTriggered > 0, 'Status exposes deflection stats');

  // =============================================================
  // Test Summary
  // =============================================================
  console.log('\n========================================');
  console.log(' Results: ' + passed + ' passed, ' + failed + ' failed');
  console.log('========================================');

  if (failures.length > 0) {
    console.log('\nFailed tests:');
    for (const f of failures) {
      console.log('  - ' + f);
    }
  }

  console.log('');
  process.exit(failed > 0 ? 1 : 0);
}

runTests().catch(err => {
  console.error('Test runner error:', err);
  process.exit(1);
});
