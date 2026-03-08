---
name: cx-journey
description: "View and analyze customer journey states, transitions, and patterns. Use when the user asks about customer journeys, journey tracking, cross-channel behavior, or journey analytics."
---

# Journey Intelligence

Query and analyze customer journeys tracked by the Nexus CX Intelligence Platform's state machine engine.

## API Endpoints

### Journey Statistics

```bash
curl -s http://localhost:3143/api/journeys | jq .
```

Returns aggregate stats: total journeys, active vs. terminal, by type, by state.

### Customer's Journeys

```bash
curl -s http://localhost:3143/api/journeys/customer/CUSTOMER_ID | jq .
```

Returns all journeys (active and completed) for a specific customer. Each journey includes:
- `type` — Journey type (billing_dispute, technical_troubleshooting, etc.)
- `state` — Current state in the state machine
- `transitions` — History of all state changes with timestamps
- `channels` — Which channels the customer used during this journey
- `created` / `updated` — Timestamps

### Recent Transitions

```bash
curl -s http://localhost:3143/api/journeys/transitions?limit=50 | jq .
```

Returns the most recent state transitions across all journeys. Useful for monitoring real-time journey activity.

### Journey Type Definitions

```bash
curl -s http://localhost:3143/api/journeys/definitions | jq .
```

Returns the state machine definitions for all journey types, including states, valid transitions, and timeout rules.

## Journey Types

The platform tracks 5 journey types, each as a finite state machine:

### 1. Billing Dispute Resolution (`billing_dispute`)
- **Triggers**: billing.refund_request, billing.billing_inquiry
- **States**: initiated -> under_review -> refund_processing -> resolved
- **Escalation path**: Any state -> escalated (via complaint)
- **Terminal**: resolved, abandoned

### 2. Technical Troubleshooting (`technical_troubleshooting`)
- **Triggers**: technical_support.connectivity, device_issue, app_error
- **States**: reported -> diagnosing -> waiting_for_fix -> follow_up -> resolved
- **Escalation path**: diagnosing/waiting -> escalated (via complaint)
- **Terminal**: resolved, abandoned

### 3. New Account Setup (`new_account_setup`)
- **Triggers**: account.account_creation, sales.new_service
- **States**: started -> comparing_plans -> profile_created -> payment_setup -> active
- **Terminal**: active, abandoned

### 4. Churn Prevention (`churn_prevention`)
- **Triggers**: account.cancellation
- **States**: cancel_requested -> retention_offer -> final_attempt -> retained/cancelled
- **Escalation path**: cancel_requested -> escalated (via complaint)
- **Terminal**: retained, cancelled

### 5. Service Upgrade (`service_upgrade`)
- **Triggers**: sales.upgrade, sales.product_inquiry, sales.pricing
- **States**: exploring -> comparing -> processing -> completed
- **Payment fallback**: processing -> payment_issue
- **Terminal**: completed, abandoned

## Interpreting Journeys

### State Transitions
Each transition records:
- The **intent** that caused the transition (e.g., billing.refund_request)
- The **channel** the interaction came through
- The **from** and **to** states
- The **timestamp**

### Identifying Stuck Journeys
A journey may be "stuck" if:
- It has been in a non-terminal state for longer than the timeout period defined in the state machine
- There have been no transitions for an extended period
- The customer has had repeated interactions without progressing

Look for journeys where the latest transition timestamp is old relative to the timeout rules:
- billing_dispute: timeouts range from 720 to 4320 minutes
- technical_troubleshooting: timeouts range from 720 to 4320 minutes
- Most timeouts default to 1440 minutes (24 hours) for initial states

### Cross-Channel Patterns
Examine the `channels` array on each journey to identify:
- **Channel hopping** — Customer switches channels mid-journey (e.g., web -> chat -> voice)
- **Escalation signals** — Moving from self-service channels (web, mobile) to assisted channels (chat, voice)
- **Resolution channels** — Which channels most often lead to terminal states

## Analysis Tips

1. **High-value patterns**: Churn prevention journeys that reach `retention_offer` state have the highest business impact
2. **Friction indicators**: Journeys that cycle back to earlier states (e.g., diagnosing -> follow_up -> diagnosing) indicate unresolved issues
3. **Cross-channel correlation**: Same customer appearing on multiple channels within a short window often indicates frustration
4. **Abandoned journey rate**: High abandonment in early states suggests process friction or poor initial engagement
