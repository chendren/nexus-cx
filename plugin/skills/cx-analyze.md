---
name: cx-analyze
description: "Analyze customer interactions, classify intent against the CX taxonomy, and provide CX intelligence insights. Use when the user asks to analyze customer text, classify an interaction, or understand CX patterns."
---

# CX Interaction Analysis

Analyze customer interaction text using the Nexus CX Intelligence Platform's classification engine.

## Classify Customer Text

Send text to the deterministic classifier (TF-IDF + cosine similarity against intent centroids):

```bash
curl -s -X POST http://localhost:3143/api/classify \
  -H 'Content-Type: application/json' \
  -d '{"text": "CUSTOMER_TEXT_HERE"}' | jq .
```

### Classification Response Structure

The response contains:

| Field | Description |
|-------|-------------|
| `primary.domain` | Top-level domain (billing, technical_support, account, sales, general) |
| `primary.intent` | Specific intent within the domain (e.g., refund_request, connectivity) |
| `primary.confidence` | Cosine similarity score (0.0 - 1.0) |
| `alternatives` | Array of next-best matches with their scores |

### Confidence Thresholds

| Range | Interpretation | Action |
|-------|---------------|--------|
| 0.70+ | High confidence | Trust the classification |
| 0.50 - 0.69 | Moderate confidence | Classification is likely correct but review alternatives |
| 0.30 - 0.49 | Low confidence | Consider LLM fallback or manual review |
| Below 0.30 | Very low | The text likely does not match any known intent; suggest taxonomy expansion |

When confidence is below 0.50, present the top 3 alternatives to help the user understand ambiguity.

## Check Customer Context

Look up a customer's existing profile and journey history for richer analysis:

```bash
# Get customer profile + active journeys
curl -s http://localhost:3143/api/customers/CUSTOMER_ID | jq .
```

This returns the customer's resolved identity, channel history, and any active journeys. Use this context to:
- Identify repeat contacts about the same issue
- Detect cross-channel patterns (e.g., web browse then call)
- Understand where the customer is in their journey

## The 5-Domain Taxonomy

The platform uses a static taxonomy derived from historical CX interaction mining:

### 1. Billing
- **payment_issue** — Payment failures, declined cards, autopay problems
- **billing_inquiry** — Questions about charges, bill amounts, due dates
- **refund_request** — Refund demands, incorrect charges, credit requests
- **plan_change** — Plan upgrades, downgrades, tier switches

### 2. Technical Support
- **connectivity** — Internet/network issues, dropped connections, slow speeds
- **device_issue** — Hardware problems, battery, screen, overheating
- **app_error** — App crashes, login failures, software bugs
- **service_outage** — System-wide outages, service disruptions

### 3. Account Management
- **account_creation** — New account signups, registration
- **account_modification** — Profile updates, address/email/phone changes
- **password_reset** — Login issues, locked accounts, credential resets
- **cancellation** — Account closure, subscription termination

### 4. Sales
- **product_inquiry** — Product/service information requests
- **upgrade** — Service upgrade requests, premium options
- **new_service** — Additional services, line additions, bundling
- **pricing** — Cost questions, discounts, promotions

### 5. General
- **general_inquiry** — Generic help requests, information needs
- **feedback** — Suggestions, service reviews
- **complaint** — Escalation requests, dissatisfaction
- **compliment** — Positive feedback, gratitude

## Analysis Workflow

1. **Classify** the text using /api/classify
2. **Check confidence** — if below threshold, note the ambiguity
3. **Look up customer** if a customer ID is available
4. **Check active journeys** to see if this interaction is part of an existing journey
5. **Summarize** findings: intent, confidence, customer context, journey state, and any recommended actions

## View Full Taxonomy

```bash
curl -s http://localhost:3143/api/taxonomy | jq .
```

This returns the complete taxonomy structure with all domains, intents, and exemplar phrases.
