---
name: cx-dashboard
description: "Show real-time CX dashboard metrics"
allowed-tools: [Bash]
---

# CX Dashboard Metrics

Fetch and display real-time dashboard metrics from the Nexus CX Intelligence Platform.

## Steps

1. **Check server is running**:
   ```bash
   curl -s http://localhost:3143/api/health | jq .
   ```
   If not responding, tell the user to start the platform first with `/cx-platform:cx-start`.

2. **Fetch dashboard data**:
   ```bash
   curl -s http://localhost:3143/api/dashboard | jq .
   ```

3. **Fetch supplementary data**:
   ```bash
   curl -s http://localhost:3143/api/journeys | jq .
   curl -s http://localhost:3143/api/nba/stats | jq .
   curl -s http://localhost:3143/api/customers | jq '.count'
   ```

## Output Format

Present as a formatted dashboard:

```
  +====================================================+
  |  CX INTELLIGENCE DASHBOARD                           |
  |  [timestamp]                                         |
  +====================================================+
  |                                                      |
  |  EVENTS                                              |
  |  Total Processed: [count]                            |
  |  Events/min:      [rate]                             |
  |                                                      |
  +----------------------------------------------------+
  |                                                      |
  |  CHANNEL DISTRIBUTION                                |
  |  voice:    [===...] [count] ([pct]%)                 |
  |  chat:     [===...] [count] ([pct]%)                 |
  |  web:      [===...] [count] ([pct]%)                 |
  |  mobile:   [===...] [count] ([pct]%)                 |
  |  email:    [===...] [count] ([pct]%)                 |
  |  sms:      [===...] [count] ([pct]%)                 |
  |  whatsapp: [===...] [count] ([pct]%)                 |
  |                                                      |
  +----------------------------------------------------+
  |                                                      |
  |  CLASSIFICATION BREAKDOWN                            |
  |  billing:           [count] ([pct]%)                 |
  |  technical_support:  [count] ([pct]%)                |
  |  account:           [count] ([pct]%)                 |
  |  sales:             [count] ([pct]%)                 |
  |  general:           [count] ([pct]%)                 |
  |                                                      |
  +----------------------------------------------------+
  |                                                      |
  |  JOURNEYS                                            |
  |  Active: [count]  Completed: [count]  Abandoned: [n] |
  |  By Type:                                            |
  |    billing_dispute:          [active] / [total]      |
  |    technical_troubleshooting: [active] / [total]     |
  |    new_account_setup:        [active] / [total]      |
  |    churn_prevention:         [active] / [total]      |
  |    service_upgrade:          [active] / [total]      |
  |                                                      |
  +----------------------------------------------------+
  |                                                      |
  |  NBA ACTIONS                                         |
  |  Total Triggered: [count]                            |
  |  By Urgency:                                         |
  |    high: [count]  medium: [count]  low: [count]      |
  |                                                      |
  +----------------------------------------------------+
  |                                                      |
  |  CUSTOMERS                                           |
  |  Total Resolved: [count]                             |
  |                                                      |
  +====================================================+
```

Use bar chart visualization with `=` characters for channel distribution, scaled to the terminal width. Include percentages and raw counts.

If there is no data yet (zero events), note that the platform is running but no events have been ingested, and suggest using `/cx-platform:cx-simulate` or the cx-simulate skill to generate test data.
