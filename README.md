# Nexus CX

**Real-time omnichannel customer experience intelligence platform.**

`[Node.js 20+]` `[SQLite WAL]` `[Apple Silicon]` `[Ollama]` `[MIT License]`

---

## Overview

Nexus CX is a five-layer intelligence platform that processes customer interactions across every channel in real time. Every voice call, chat message, email, SMS, web session, and mobile event flows through a unified streaming backbone -- the Event Fabric -- where it is classified, identity-resolved, tracked through journey state machines, evaluated for next-best-action recommendations, and surfaced through live operational analytics. The result is a single pane of glass for CX operations: a real-time dashboard that shows what is happening, why, and what to do about it.

The platform is designed to run entirely on a single machine with zero cloud dependencies. There are no managed services to provision, no API keys to configure, and no billing meters ticking. Classification uses semantic embeddings via Ollama's nomic-embed-text model with an MLP reranker for multi-intent filtering, falling back gracefully to TF-IDF cosine similarity when Ollama is unavailable. Persistence uses SQLite in WAL mode for concurrent reads with synchronous writes. Everything starts in under a second.

What makes Nexus CX different from demo projects is that it implements production patterns throughout. Circuit breakers protect against Ollama unavailability with automatic recovery. Rate limiting and Helmet harden the API surface. Structured JSON logging via Pino enables observability. Graceful shutdown drains connections and closes the database cleanly. The event fabric simulates Kinesis Data Streams with ring buffers and pub/sub semantics. Identity resolution uses four matching strategies with alias stitching. Journey tracking uses finite state machines with SLA monitoring and stuck-state detection. These are not stubs -- they are fully functional implementations modeled after AWS-scale production systems, ready to swap in managed services when the time comes.

The taxonomy covers 19 domains, 82 intents, and 629 exemplars spanning commercial CX and U.S. Treasury operations. Thirteen journey types track customers from first contact through resolution. Ten deterministic business rules drive next-best-action recommendations with simulated ML scoring. The platform ships with 74 integration tests, 8 performance benchmarks, a multi-channel traffic simulator, and 7 CloudFormation templates for AWS deployment.

---

## Architecture

```
                          CHANNELS
         voice  chat  email  sms  web  mobile
           |     |      |     |    |     |
           v     v      v     v    v     v
    +----------------------------------------------+
    |            1. EVENT FABRIC                    |
    |       Kinesis simulation, ring buffers,       |
    |       pub/sub, metrics, stream processors     |
    +----------------------------------------------+
                         |
                         v
    +----------------------------------------------+
    |            2. CLASSIFIER                      |
    |       Semantic embeddings (nomic-embed-text)  |
    |       + TF-IDF fallback + MLP reranker        |
    |       19 domains, 82 intents, 629 exemplars   |
    +----------------------------------------------+
                         |
                         v
    +----------------------------------------------+
    |            3. IDENTITY RESOLUTION             |
    |       Cross-channel stitching, alias links,   |
    |       4 matching strategies, unified profiles  |
    +----------------------------------------------+
                         |
                         v
    +----------------------------------------------+
    |            4. JOURNEY ENGINE                  |
    |       13 journey types, FSM transitions,      |
    |       SLA monitoring, stuck detection          |
    +----------------------------------------------+
                         |
                         v
    +----------------------------------------------+
    |            5. NBA ENGINE                      |
    |       10 deterministic rules, ML scoring,     |
    |       routing, deflection, urgency tiers       |
    +----------------------------------------------+
                         |
              +----------+----------+
              |                     |
              v                     v
    +------------------+  +------------------+
    |   ANALYTICS      |  |   DASHBOARD /    |
    |   Real-time KPIs |  |   REST API /     |
    |   Time series    |  |   WebSocket      |
    +------------------+  +------------------+
```

---

## Features

### Event Fabric
- Kinesis Data Streams simulation with ring buffer semantics
- Pub/sub event distribution to registered stream processors
- Per-channel and per-type metrics with events-per-second tracking
- Configurable stream sizes (default 10,000 events per stream)

### Classifier
- 19 domains, 82 intents, 629 exemplars across commercial CX and Treasury operations
- Semantic embedding via Ollama nomic-embed-text (768-dimensional vectors)
- TF-IDF cosine similarity fallback when embeddings are unavailable
- MLP binary reranker (8 features, 16 hidden units) for multi-intent filtering
- Trained reranker weights cached to disk with automatic retrain on taxonomy changes
- Multi-intent detection with configurable confidence thresholds
- LLM enrichment via nemotron-mini for sentiment, emotion, effort, and topic extraction
- Circuit breaker protection for Ollama calls (CLOSED / OPEN / HALF_OPEN states)

### Identity Resolution
- Cross-channel customer identity stitching
- Four matching strategies: direct, deterministic, session-based, probabilistic
- Alias resolution linking multiple identifiers to a single unified profile
- Identifier indexing for fast lookup across all known aliases
- Persistent profiles with SQLite-backed storage

### Journey Engine
- 13 journey types across commercial and Treasury domains
- Finite state machine with defined transitions per journey type
- SLA monitoring with configurable thresholds per journey
- Stuck-state detection for journeys that stall mid-resolution
- Cross-channel journey continuity (start on chat, continue on voice)
- Journey intelligence: common paths, top triggers, handoff analysis

### NBA Engine
- 10 deterministic business rules with priority-ordered evaluation
- Simulated ML scoring for recommendation ranking
- Channel-specific delivery routing
- Deflection tracking (self-service offload metrics)
- Action logging with urgency tiers (critical, high, medium, low)
- Rules include churn prevention, VIP routing, escalation, retention offers

### Analytics
- Real-time KPI calculation: contacts, SLA compliance, wait time, deflection rate, contact value, sentiment
- Time-series metrics with configurable retention
- Memory and resource monitoring
- Per-channel and per-domain breakdowns

### Persistence
- SQLite WAL mode for concurrent reads with synchronous writes
- Write-through cache for customer profiles, journeys, NBA actions, and dead letters
- Embedding vector cache (avoids re-embedding known exemplars)
- Automatic migration on startup
- Graceful database close on shutdown

### Resilience
- Circuit breakers on all Ollama calls with configurable failure threshold and recovery timeout
- Graceful shutdown: drains HTTP connections, closes WebSocket clients, flushes database
- Rate limiting via express-rate-limit (configurable window and max requests)
- Security headers via Helmet
- Request ID tracking (X-Request-ID) on every API call
- 100KB request body limit

### Dashboard
- Real-time WebSocket updates for live event streaming
- KPI cards with sparkline visualizations
- Live event stream with classification and enrichment details
- Journey and routing intelligence panels
- Customer explorer with unified profile views
- Circuit breaker status monitoring
- SLA alert indicators
- Channel simulator for testing event ingestion
- Export functionality

---

## Quick Start

### Prerequisites

- **Node.js 20+** (ARM-native recommended on Apple Silicon)
- **Ollama** with `nomic-embed-text` model pulled (optional -- falls back to TF-IDF)

```bash
# Pull the embedding model (optional, for semantic classification)
ollama pull nomic-embed-text

# Pull the LLM model (optional, for sentiment/emotion enrichment)
ollama pull nemotron-mini
```

### Install and Start

```bash
git clone <repo-url> nexus-cx
cd nexus-cx
npm install
node server.js
```

### Startup Banner

```
  +======================================================+
  |  OMNICHANNEL CX INTELLIGENCE PLATFORM                |
  |======================================================|
  |  Status:    ONLINE                                    |
  |  Port:      3143                                      |
  |  Dashboard: http://localhost:3143/                     |
  |  API:       http://localhost:3143/api                  |
  |  WebSocket: ws://localhost:3143/                       |
  |------------------------------------------------------|
  |  Layers:                                              |
  |    1. Event Fabric      Kinesis (local)               |
  |    2. Classifier        Semantic Embedding            |
  |    3. Journey Engine    State Machine                 |
  |    4. NBA Engine        Rules + ML Scoring            |
  |    5. Analytics         Real-time Metrics             |
  +======================================================+
```

Open the dashboard at **http://localhost:3143/** and use the built-in channel simulator to send test events.

---

## Configuration

All configuration is via environment variables. Copy `.env.example` to `.env` to override defaults.

| Variable | Default | Description |
|---|---|---|
| `CX_PORT` | `3143` | HTTP/WebSocket server port |
| `OLLAMA_BASE_URL` | `http://localhost:11434` | Ollama API endpoint |
| `OLLAMA_LLM_MODEL` | `nemotron-mini` | LLM model for enrichment (sentiment, emotion, topics) |
| `OLLAMA_EMBED_MODEL` | `nomic-embed-text` | Embedding model for semantic classification (768-dim) |
| `OLLAMA_TIMEOUT_MS` | `15000` | Timeout for LLM chat completions |
| `OLLAMA_EMBED_TIMEOUT_MS` | `10000` | Timeout for embedding requests |
| `PERSIST` | `true` | Enable SQLite persistence (`false` for in-memory only) |
| `DATA_DIR` | `./data` | Directory for SQLite database and cached data |
| `LOG_LEVEL` | `info` | Pino log level (trace, debug, info, warn, error, fatal) |
| `FABRIC_STREAM_SIZE` | `10000` | Max events per stream ring buffer |
| `METRICS_SERIES_SIZE` | `10000` | Max time-series data points retained |
| `ANALYSIS_STORE_MAX` | `500` | Max enrichment analysis results stored |
| `JOURNEY_HISTORY_MAX` | `10000` | Max journey records retained |
| `NBA_ACTION_LOG_MAX` | `5000` | Max NBA action log entries retained |
| `SESSION_TTL_MS` | `86400000` | Session expiry for identity matching (default 24h) |
| `RATE_LIMIT_WINDOW_MS` | `60000` | Rate limit sliding window (default 1 minute) |
| `RATE_LIMIT_MAX` | `200` | Max requests per rate limit window |

---

## API Reference

### REST Endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/events` | Ingest a customer interaction event (`{ customerId, channel, content }`) |
| `POST` | `/api/classify` | Classify arbitrary text (`{ text }`) |
| `GET` | `/api/dashboard` | Full dashboard payload: metrics, journey intelligence, identity/NBA summary |
| `GET` | `/api/dashboard/kpis` | KPI snapshot: contacts, SLA, wait time, deflection, contact value, sentiment |
| `GET` | `/api/dashboard/operations` | Operational summary with journey intelligence |
| `GET` | `/api/customers` | List all customer profiles |
| `GET` | `/api/customers/:id` | Single customer profile with journeys and history |
| `GET` | `/api/journeys` | Journey statistics and intelligence summary |
| `GET` | `/api/journeys/transitions` | Recent journey state transitions |
| `GET` | `/api/journeys/intelligence` | Common paths, stuck journeys, cross-channel handoffs, top triggers |
| `GET` | `/api/nba/stats` | NBA engine statistics |
| `GET` | `/api/nba/actions` | Recent NBA action log |
| `GET` | `/api/status` | Full platform status (all layers, database, circuit breakers) |
| `GET` | `/api/health` | Health check |
| `GET` | `/api/health/ready` | Readiness check with component status |

### WebSocket

Connect to `ws://localhost:3143/` for real-time event streaming. The server pushes enriched events as they flow through the pipeline, enabling live dashboard updates without polling.

### Response Headers

All responses include `X-Request-ID` for request tracing.

---

## Dashboard

The single-page ops dashboard at the server root provides five primary surfaces:

- **Event Simulator** -- Send rich interaction payloads with customer tier, channel identifiers, queue wait time, and required skills. Supports all six channels (voice, chat, email, SMS, web, mobile).

- **CX Operations Snapshot** -- Live KPI cards for total contacts, SLA compliance, average wait time, deflection rate, contact value, and active customers. Each KPI includes a sparkline showing recent trend.

- **Command Center** -- Scrolling event stream showing each interaction as it flows through classification, identity resolution, journey tracking, and NBA evaluation. Enrichment insights (sentiment, emotion, effort score) are displayed inline.

- **Journey and Routing Intelligence** -- Journey type distribution charts, top trigger analysis, common resolution paths, cross-channel handoff patterns, and at-risk journey alerts for stuck or SLA-breaching journeys.

- **Customer Explorer** -- Unified profile table showing all known customers with alias stitching, per-customer journey detail, sentiment history, and channel activity breakdown.

---

## Taxonomy

The classifier operates across 19 domains organized into three categories, covering 82 intents with 629 training exemplars.

### Commercial CX (5 domains, 20 intents)

| Domain | Intents |
|---|---|
| billing | 4 |
| technical_support | 4 |
| account | 4 |
| sales | 4 |
| general | 4 |

### Treasury Core (4 domains, 16 intents)

| Domain | Intents |
|---|---|
| tax_payments | 4 |
| savings_bonds | 4 |
| treasury_payments | 4 |
| debt_collection | 4 |
| currency_compliance | 4 |

### Treasury Expanded (10 domains, 46 intents)

| Domain | Intents |
|---|---|
| tax_filing | 5 |
| tax_identity | 5 |
| tax_compliance | 5 |
| treasury_accounts | 4 |
| foreign_tax | 5 |
| financial_crimes | 5 |
| government_payments | 5 |
| estate_gift_tax | 4 |
| business_tax | 4 |

---

## Journey Types

Thirteen journey types across three categories, each implemented as a finite state machine with defined states, transitions, and SLA thresholds.

### Commercial (5 journeys)

| Journey Type | Description |
|---|---|
| `billing_dispute` | Payment disputes, overcharges, refund requests |
| `technical_troubleshooting` | Service outages, connectivity, device issues |
| `new_account_setup` | New customer onboarding and provisioning |
| `churn_prevention` | Cancellation requests and retention workflows |
| `service_upgrade` | Plan changes, tier upgrades, add-on services |

### Treasury Core (4 journeys)

| Journey Type | Description |
|---|---|
| `treasury_check_replacement` | Lost or damaged Treasury check replacement |
| `bond_redemption` | Savings bond maturity and redemption processing |
| `debt_offset_dispute` | Treasury offset program disputes |
| `treasury_account_recovery` | Account access and recovery for Treasury services |

### Treasury Expanded (4 journeys)

| Journey Type | Description |
|---|---|
| `tax_identity_recovery` | IRS identity theft resolution and transcript correction |
| `tax_debt_resolution` | Tax debt payment plans and settlement |
| `audit_response` | IRS audit response and documentation workflow |
| `foreign_compliance` | Foreign account reporting and tax treaty compliance |

---

## Testing

### Integration Tests (74 assertions)

```bash
node tests/integration.test.js
```

Validates the full pipeline end-to-end: event schema, classification accuracy, identity stitching, journey state transitions, NBA rule evaluation, routing decisions, and metrics aggregation.

### Performance Benchmarks (8 benchmarks)

```bash
node tests/benchmark.js
node tests/benchmark.js --report    # saves JSON report to data/benchmark-report.json
```

Benchmarks cover classification throughput, full pipeline throughput, concurrent event latency (p50/p95/p99), memory usage, warm startup time, and SQLite write throughput.

### Traffic Simulator

```bash
# Default: 30 events, sequential
node tests/simulate-traffic.js

# Heavy load: 200 events, 10 concurrent batches, with report
node tests/simulate-traffic.js --count 200 --concurrency 10 --report

# Help
node tests/simulate-traffic.js --help
```

| Flag | Default | Description |
|---|---|---|
| `--count N` | `30` | Number of random events to generate |
| `--concurrency N` | `1` | Parallel event batches |
| `--report` | off | Save report to `data/traffic-report.json` |

The simulator requires a running server. Set `CX_API_URL` to target a different host.

---

## Performance

Representative benchmark results on Apple Silicon (M-series, Node.js 20+):

| Benchmark | Threshold | Typical Result |
|---|---|---|
| Warm startup (classifier init) | < 3s | ~21ms |
| Classification throughput | > 40/sec | ~7,000/sec (TF-IDF fallback) |
| Full pipeline throughput | > 50/sec | ~7,000/sec |
| Concurrent latency p50 | < 50ms | ~0.4ms |
| Concurrent latency p95 | < 200ms | ~1.2ms |
| Concurrent latency p99 | < 500ms | ~2.5ms |
| Memory (RSS after 1000 events) | < 200MB | ~81MB |
| SQLite write throughput | > 1,000/sec | ~549,000/sec |

When Ollama is available for semantic embeddings, classification throughput is network-bound at approximately 50-80 classifications per second (17ms round-trip per embedding call). TF-IDF fallback is CPU-bound and orders of magnitude faster.

---

## Project Structure

```
nexus-cx/
  server.js                        Express + WebSocket API server
  package.json                     Dependencies and npm scripts
  .env.example                     Environment variable reference
  CLAUDE.md                        Project context for Claude Code
  ui/
    index.html                     Single-page ops dashboard
  src/
    config.js                      Centralized configuration
    logger.js                      Structured JSON logger (Pino)
    pipeline.js                    Pipeline orchestrator wiring all 5 layers
    events/
      schema.js                    Canonical event schema and validation
      fabric.js                    Event Fabric (Kinesis simulation, ring buffers)
      channels/                    Channel-specific event adapters
    classifier/
      taxonomy.js                  19 domains, 82 intents, 629 exemplars
      classifier.js                Semantic embedding + TF-IDF classifier
      reranker.js                  MLP reranker for multi-intent filtering
      reranker-weights.json        Cached trained weights
      llm-client.js                Ollama client (embeddings + chat)
      circuit-breaker.js           Circuit breaker for Ollama calls
    identity/
      resolver.js                  Cross-channel identity stitching
    journey/
      definitions.js               13 journey type definitions (states, transitions, SLAs)
      state-machine.js             Journey FSM engine with stuck detection
    nba/
      rules.js                     10 deterministic business rules
      engine.js                    NBA evaluation, ML scoring, routing
    analytics/
      metrics.js                   Real-time KPIs and time-series aggregation
    store/
      database.js                  SQLite initialization, migrations, WAL config
      memory-store.js              In-memory store (DynamoDB partition/sort key simulation)
      persisted-store.js           Write-through SQLite-backed store
  tests/
    integration.test.js            74 integration assertions
    benchmark.js                   8 performance benchmarks
    simulate-traffic.js            Multi-channel traffic simulator
  cloudformation/
    main.yaml                      Root stack (nested stack orchestration)
    event-fabric.yaml              Kinesis, Lambda, EventBridge
    processing.yaml                ECS/Fargate services for classification and identity
    data-stores.yaml               DynamoDB, S3, ElastiCache
    orchestration.yaml             Step Functions, SQS, SNS
    dashboard.yaml                 CloudFront, S3 static hosting, Cognito
    api-layer.yaml                 API Gateway, Lambda, WAF
  plugin/
    plugin.json                    Claude Code plugin manifest
    skills/                        5 skills (analyze, deploy, journey, simulate, taxonomy)
    agents/                        2 agents (cx-monitor, cx-scenario-runner)
    commands/                      3 commands (cx-dashboard, cx-start, cx-status)
  data/
    cx-platform.db                 SQLite database (auto-created)
```

---

## CloudFormation

The `cloudformation/` directory contains 7 AWS deployment templates for production deployment:

| Template | Resources |
|---|---|
| `main.yaml` | Root stack orchestrating all nested stacks |
| `event-fabric.yaml` | Kinesis Data Streams, Lambda consumers, EventBridge rules |
| `processing.yaml` | ECS/Fargate services for classifier, identity, journey, and NBA |
| `data-stores.yaml` | DynamoDB tables, S3 buckets, ElastiCache clusters |
| `orchestration.yaml` | Step Functions workflows, SQS queues, SNS topics |
| `dashboard.yaml` | CloudFront distribution, S3 static hosting, Cognito auth |
| `api-layer.yaml` | API Gateway, Lambda integrations, WAF rules |

---

## Technology Stack

| Technology | Role |
|---|---|
| Node.js 20+ | Runtime |
| Express 4 | HTTP API server |
| ws | WebSocket server for real-time streaming |
| better-sqlite3 | SQLite persistence with WAL mode |
| Pino | Structured JSON logging |
| Ollama | Local LLM inference (no cloud APIs) |
| nomic-embed-text | 768-dimensional semantic embeddings |
| nemotron-mini | LLM enrichment (sentiment, emotion, effort, topics) |
| Helmet | Security headers |
| express-rate-limit | API rate limiting |
| uuid | Request and event ID generation |
| cors | Cross-origin resource sharing |

---

## License

MIT
