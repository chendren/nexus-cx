# Nexus CX Intelligence Platform

## Overview
Five-layer, local-first platform for real-time customer experience intelligence across all channels. Implements the Event Fabric pattern: every interaction flows through a unified streaming backbone for semantic classification, unified identity resolution, journey state tracking, next-best-action orchestration, and live operational analytics.

All state persists to SQLite (WAL mode) with write-through caching. Ollama provides local AI inference for 768-dim semantic embeddings and post-interaction analytics. No cloud services required.

## Architecture
```
Channel Layer --> Event Fabric (Kinesis/local) --> [Classifier | Identity | Journey | NBA | Analytics] --> Action Delivery
                                                                                                      --> Dashboard (WS)
```

## Local Development
- **Start**: `node server.js` (port 3143, `CX_PORT` overrides)
- **Test**: `node tests/integration.test.js` (80 integration tests)
- **Bench**: `node tests/benchmark.js --report` (8 performance benchmarks)
- **Simulate**: `node tests/simulate-traffic.js` (requires running server)
  - `--count N` events, `--concurrency N` parallel, `--report` to save JSON
- **Dashboard**: http://localhost:3143/
- **Health**: `curl localhost:3143/api/health/ready`

## Key Directories
- `src/events/` — canonical event schema + event fabric (Kinesis simulation)
- `src/classifier/` — semantic embedding classifier (nomic-embed-text) + TF-IDF fallback + MLP reranker + LLM analytics
- `src/identity/` — cross-channel identity resolution with alias stitching and unified customer profiles
- `src/journey/` — finite state machine journey engine + journey intelligence and stuck-state detection
- `src/nba/` — next-best-action engine with routing priority, deflection, and ML simulation
- `src/analytics/` — real-time metrics aggregation + KPI dashboard calculations
- `src/store/` — persistence layer: SQLite database, MemoryStore (DynamoDB simulation), PersistedStore (write-through cache)
- `src/pipeline.js` — orchestrator wiring all processors together
- `server.js` — Express + WebSocket API server with middleware stack (helmet, rate-limit, request IDs)
- `ui/` — single-page ops dashboard with sparklines, exports, health monitoring
- `cloudformation/` — 7 AWS deployment templates
- `plugin/` — Claude Code plugin (5 skills, 2 agents, 3 commands)
- `tests/` — integration tests, benchmarks, traffic simulator

## API Endpoints
- `POST /api/events` — ingest event: `{ customerId, channel, content }` (validates input, 422 on bad data)
- `POST /api/classify` — classify text: `{ text }`
- `GET /api/dashboard` — real-time metrics + journey intelligence + latency + circuit breaker state
- `GET /api/dashboard/kpis` — KPI snapshot
- `GET /api/dashboard/operations` — operational summary + journey intelligence
- `GET /api/health/ready` — deep health check (pipeline, classifier, Ollama, SQLite, memory)
- `GET /api/customers` / `GET /api/customers/:id` — profiles + journeys
- `GET /api/journeys` — stats + intelligence; `/api/journeys/transitions` — recent
- `GET /api/journeys/intelligence` — common paths, stuck journeys, cross-channel handoffs, top triggers
- `GET /api/nba/stats` / `/api/nba/actions` — NBA data
- `GET /api/metrics/history` — historical metrics from SQLite
- `GET /api/metrics/timeseries` — bucketed time series from memory
- `GET /api/events/export?format=csv|json` — export event archive
- `GET /api/dead-letters` — failed events
- `GET /api/status` — full platform status with latency stats
- WebSocket on same port for real-time streaming (metrics every 2s, live events, LLM enrichment)

## Taxonomy Domains (19 domains, 82 intents, 629 exemplars)
**Commercial CX**: billing (4) | technical_support (4) | account (4) | sales (4) | general (4)
**Treasury Core**: tax_payments (4) | savings_bonds (4) | treasury_payments (4) | debt_collection (4) | currency_compliance (4)
**Treasury Expanded**: tax_filing (5) | tax_identity (5) | tax_compliance (5) | treasury_accounts (4) | foreign_tax (5) | financial_crimes (5) | government_payments (5) | estate_gift_tax (4) | business_tax (4)

## Journey Types (13)
**Commercial**: billing_dispute | technical_troubleshooting | new_account_setup | churn_prevention | service_upgrade
**Treasury Core**: treasury_check_replacement | bond_redemption | debt_offset_dispute | treasury_account_recovery
**Treasury Expanded**: tax_identity_recovery | tax_debt_resolution | audit_response | foreign_compliance

## Code Conventions
- No regex — string methods only (enforced by hook)
- Singleton pattern for services (getClassifier(), getResolver(), etc.)
- Events are mutable — processors enrich them in-place through the pipeline
- In-memory stores simulate DynamoDB with partition/sort key patterns
- Write-through cache: reads from memory, writes to memory + SQLite
- Preserve local-first behavior: model AWS-ready concepts, but keep features runnable and testable offline
- Structured JSON logging via pino (no console.log in production paths)
