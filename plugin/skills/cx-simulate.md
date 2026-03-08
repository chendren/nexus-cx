---
name: cx-simulate
description: "Simulate customer channel activity to test the CX platform. Use when the user asks to simulate traffic, test the platform, generate test data, or run scenarios."
---

# CX Traffic Simulation

Simulate customer interactions to test the Nexus CX Intelligence Platform's event processing, classification, journey tracking, and NBA engines.

## Single Event Ingestion

```bash
curl -s -X POST http://localhost:3143/api/events \
  -H 'Content-Type: application/json' \
  -d '{
    "customerId": "cust-demo-001",
    "channel": "voice",
    "eventType": "interaction",
    "content": "I was charged twice on my last bill and I want a refund"
  }' | jq .
```

### Event Fields

| Field | Required | Description | Values |
|-------|----------|-------------|--------|
| `customerId` | No | Customer ID (auto-generated if omitted) | Any string, e.g., "cust-demo-001" |
| `sessionId` | No | Session ID (auto-generated if omitted) | UUID string |
| `channel` | No | Interaction channel (defaults to "web") | voice, chat, web, mobile, email, sms, whatsapp |
| `eventType` | No | Event type (defaults to "interaction") | interaction, navigation, system, lifecycle |
| `content` | No | The customer's message or interaction text | Free text |
| `metadata` | No | Additional key-value metadata | Object |
| `context` | No | Context fields (agent_id, queue, etc.) | Object |

## Batch Event Ingestion

Send multiple events at once:

```bash
curl -s -X POST http://localhost:3143/api/events/batch \
  -H 'Content-Type: application/json' \
  -d '{
    "events": [
      {"customerId": "cust-batch-001", "channel": "web", "content": "looking at pricing plans"},
      {"customerId": "cust-batch-001", "channel": "chat", "content": "can you help me choose a plan"},
      {"customerId": "cust-batch-002", "channel": "voice", "content": "my internet has been down all day"}
    ]
  }' | jq .
```

## Sample Scenarios

### Scenario 1: Billing Dispute Flow
Simulates a customer disputing a charge across channels.

```bash
# Step 1: Customer calls about wrong charge (triggers billing_dispute journey)
curl -s -X POST http://localhost:3143/api/events -H 'Content-Type: application/json' \
  -d '{"customerId":"cust-dispute-001","channel":"voice","content":"I was charged incorrectly on my last bill, this is not right"}'

# Step 2: Customer follows up via chat asking about their bill
curl -s -X POST http://localhost:3143/api/events -H 'Content-Type: application/json' \
  -d '{"customerId":"cust-dispute-001","channel":"chat","content":"I need to understand the charges on my statement"}'

# Step 3: Customer requests a refund
curl -s -X POST http://localhost:3143/api/events -H 'Content-Type: application/json' \
  -d '{"customerId":"cust-dispute-001","channel":"chat","content":"please refund my last payment, I was overcharged"}'

# Step 4: Customer confirms resolution
curl -s -X POST http://localhost:3143/api/events -H 'Content-Type: application/json' \
  -d '{"customerId":"cust-dispute-001","channel":"email","content":"thank you for resolving this quickly, great service"}'
```

### Scenario 2: Technical Support Escalation
Simulates a customer with a connectivity issue that escalates.

```bash
# Step 1: Customer reports connectivity issue on web
curl -s -X POST http://localhost:3143/api/events -H 'Content-Type: application/json' \
  -d '{"customerId":"cust-tech-001","channel":"web","content":"my internet is not working and keeps dropping"}'

# Step 2: Customer calls in frustrated
curl -s -X POST http://localhost:3143/api/events -H 'Content-Type: application/json' \
  -d '{"customerId":"cust-tech-001","channel":"voice","content":"I already reported this online, the connection is still very slow"}'

# Step 3: Customer escalates with complaint
curl -s -X POST http://localhost:3143/api/events -H 'Content-Type: application/json' \
  -d '{"customerId":"cust-tech-001","channel":"voice","content":"this is unacceptable, I want to speak to a manager"}'

# Step 4: Resolution
curl -s -X POST http://localhost:3143/api/events -H 'Content-Type: application/json' \
  -d '{"customerId":"cust-tech-001","channel":"voice","content":"thank you, the issue is fixed now, great help"}'
```

### Scenario 3: Churn Prevention
Simulates a customer requesting cancellation and being retained.

```bash
# Step 1: Customer requests cancellation
curl -s -X POST http://localhost:3143/api/events -H 'Content-Type: application/json' \
  -d '{"customerId":"cust-churn-001","channel":"voice","content":"I want to cancel my account and stop my service"}'

# Step 2: Customer asks about pricing (retention opportunity)
curl -s -X POST http://localhost:3143/api/events -H 'Content-Type: application/json' \
  -d '{"customerId":"cust-churn-001","channel":"voice","content":"what are your current prices, are there any discounts"}'

# Step 3: Customer accepts upgrade offer
curl -s -X POST http://localhost:3143/api/events -H 'Content-Type: application/json' \
  -d '{"customerId":"cust-churn-001","channel":"voice","content":"I want to upgrade my service to the premium plan"}'
```

### Scenario 4: Cross-Channel New Account Setup
Simulates a prospect moving through signup across channels.

```bash
# Step 1: Browsing products on web
curl -s -X POST http://localhost:3143/api/events -H 'Content-Type: application/json' \
  -d '{"customerId":"cust-new-001","channel":"web","content":"what products do you offer, tell me about your services"}'

# Step 2: Comparing pricing on mobile
curl -s -X POST http://localhost:3143/api/events -H 'Content-Type: application/json' \
  -d '{"customerId":"cust-new-001","channel":"mobile","content":"how much does the premium plan cost, any promotions"}'

# Step 3: Signs up via chat
curl -s -X POST http://localhost:3143/api/events -H 'Content-Type: application/json' \
  -d '{"customerId":"cust-new-001","channel":"chat","content":"I want to create a new account and sign up for the premium plan"}'

# Step 4: Payment setup
curl -s -X POST http://localhost:3143/api/events -H 'Content-Type: application/json' \
  -d '{"customerId":"cust-new-001","channel":"web","content":"my credit card payment is not going through, getting an error"}'
```

### Scenario 5: Multi-Customer Burst
Simulates a burst of activity from multiple customers:

```bash
curl -s -X POST http://localhost:3143/api/events/batch -H 'Content-Type: application/json' \
  -d '{
    "events": [
      {"customerId":"cust-burst-001","channel":"voice","content":"my payment was declined"},
      {"customerId":"cust-burst-002","channel":"chat","content":"the app keeps crashing on my phone"},
      {"customerId":"cust-burst-003","channel":"email","content":"I want to cancel everything"},
      {"customerId":"cust-burst-004","channel":"web","content":"interested in upgrading my plan"},
      {"customerId":"cust-burst-005","channel":"sms","content":"when is my bill due"},
      {"customerId":"cust-burst-006","channel":"whatsapp","content":"I forgot my password and cannot log in"},
      {"customerId":"cust-burst-007","channel":"voice","content":"is there a service outage in my area"},
      {"customerId":"cust-burst-008","channel":"chat","content":"your service is terrible, worst experience"},
      {"customerId":"cust-burst-009","channel":"mobile","content":"I want to add another line to my account"},
      {"customerId":"cust-burst-010","channel":"web","content":"great service, the agent was very helpful"}
    ]
  }'
```

## Verifying Results

After simulating events, verify processing:

```bash
# Dashboard metrics (event counts, channel distribution, classification breakdown)
curl -s http://localhost:3143/api/dashboard | jq .

# Customer profiles (resolved identities)
curl -s http://localhost:3143/api/customers | jq .

# Specific customer's profile + journeys
curl -s http://localhost:3143/api/customers/cust-dispute-001 | jq .

# Journey statistics
curl -s http://localhost:3143/api/journeys | jq .

# Recent transitions
curl -s http://localhost:3143/api/journeys/transitions?limit=20 | jq .

# NBA action log
curl -s http://localhost:3143/api/nba/actions?limit=20 | jq .
```

## Tips

- Use consistent `customerId` values across events to simulate the same customer interacting on different channels
- Space events a second or two apart for more realistic timing when testing journey transitions
- The 7 available channels are: voice, chat, web, mobile, email, sms, whatsapp
- Event types are: interaction (default), navigation, system, lifecycle
- If you omit `customerId`, the platform auto-generates one — useful for anonymous traffic simulation
