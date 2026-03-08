---
name: cx-taxonomy
description: "Manage the CX intent taxonomy — view, analyze, or extend the classification taxonomy. Use when the user asks about intents, taxonomy, classification categories, or wants to add new intents."
---

# CX Taxonomy Management

Manage the intent taxonomy that powers the Nexus CX Intelligence Platform's deterministic classifier.

## View Current Taxonomy

```bash
curl -s http://localhost:3143/api/taxonomy | jq .
```

This returns the full taxonomy structure: domains, intents, and exemplar phrases for each intent.

## Taxonomy Structure

The taxonomy is organized as a 3-level hierarchy:

```
Domain (5)
  └── Intent (20 total)
       └── Exemplars (5-8 phrases per intent)
```

### Domains and Intents

| Domain | Intent Key | Label | Exemplar Count |
|--------|-----------|-------|----------------|
| **billing** | payment_issue | Payment Issue | 8 |
| | billing_inquiry | Billing Inquiry | 7 |
| | refund_request | Refund Request | 7 |
| | plan_change | Plan Change | 7 |
| **technical_support** | connectivity | Connectivity Issue | 8 |
| | device_issue | Device Issue | 7 |
| | app_error | Application Error | 7 |
| | service_outage | Service Outage | 7 |
| **account** | account_creation | Account Creation | 6 |
| | account_modification | Account Modification | 6 |
| | password_reset | Password Reset | 6 |
| | cancellation | Account Cancellation | 7 |
| **sales** | product_inquiry | Product Inquiry | 6 |
| | upgrade | Upgrade Request | 6 |
| | new_service | New Service | 6 |
| | pricing | Pricing Inquiry | 7 |
| **general** | general_inquiry | General Inquiry | 6 |
| | feedback | Feedback | 6 |
| | complaint | Complaint | 7 |
| | compliment | Compliment | 6 |

Total: 5 domains, 20 intents, ~130 exemplar phrases.

## How the Classifier Works

The classification engine uses a deterministic TF-IDF + cosine similarity approach:

1. **Indexing**: At startup, all exemplar phrases are tokenized and converted to TF-IDF vectors. A centroid vector is computed for each intent by averaging its exemplar vectors.

2. **Classification**: When new text arrives:
   - The text is tokenized and converted to a TF-IDF vector using the same vocabulary
   - Cosine similarity is computed between the input vector and every intent centroid
   - The intent with the highest similarity score is the primary classification
   - Alternative matches are ranked by descending similarity

3. **No external dependencies**: The classifier runs entirely in-process with no API calls, ML model downloads, or GPU requirements. Classification is sub-millisecond.

4. **Limitations**: The deterministic classifier works well for text that closely matches the exemplar patterns. For novel phrasings, ambiguous text, or multi-intent messages, an LLM fallback would provide better accuracy. The platform architecture supports this as a future enhancement.

## Adding New Intents

To extend the taxonomy, edit the source file:

**File**: `src/classifier/taxonomy.js`

### Add an Intent to an Existing Domain

Add a new intent object under the appropriate domain's `intents` map:

```javascript
// In the 'billing' domain, add a 'payment_method' intent:
payment_method: {
  label: 'Payment Method',
  exemplars: [
    'how do I add a new credit card',
    'update my payment method',
    'change my bank account for autopay',
    'add a different payment option',
    'switch to paying with a debit card',
    'remove my old credit card from the account'
  ]
}
```

### Add a New Domain

Add a new top-level key to the TAXONOMY object:

```javascript
loyalty: {
  label: 'Loyalty & Rewards',
  intents: {
    points_inquiry: {
      label: 'Points Inquiry',
      exemplars: [
        'how many reward points do I have',
        'check my loyalty balance',
        'when do my points expire',
        'how do I earn more points'
      ]
    },
    redemption: {
      label: 'Reward Redemption',
      exemplars: [
        'I want to redeem my points',
        'use my rewards for a discount',
        'apply loyalty credits to my bill',
        'what can I redeem my points for'
      ]
    }
  }
}
```

### Exemplar Guidelines

When writing exemplars:
- Use **natural customer language**, not formal descriptions
- Include **5-8 exemplars** per intent for good centroid coverage
- Vary the phrasing — use different words that express the same intent
- Include both **direct** ("I want a refund") and **indirect** ("I was charged incorrectly") phrasings
- Keep exemplars focused on a **single intent** — avoid multi-intent phrases
- Use **lowercase** — the classifier normalizes case during tokenization

### After Modifying the Taxonomy

1. **Restart the server** to rebuild TF-IDF index:
   ```bash
   # Stop existing server
   lsof -t -i:3143 | xargs kill 2>/dev/null
   # Start fresh
   cd ~/omnichannel-cx-platform && node server.js &
   ```

2. **Test the new intent**:
   ```bash
   curl -s -X POST http://localhost:3143/api/classify \
     -H 'Content-Type: application/json' \
     -d '{"text": "TEST_PHRASE_HERE"}' | jq .
   ```

3. **Validate against existing intents** — make sure new exemplars do not cause misclassification of existing intents by testing several known phrases.

## Taxonomy Validation

Test classification accuracy by running a set of known phrases:

```bash
# Test billing intents
curl -s -X POST http://localhost:3143/api/classify -H 'Content-Type: application/json' \
  -d '{"text":"my payment was declined"}' | jq '.primary'

# Test technical support intents
curl -s -X POST http://localhost:3143/api/classify -H 'Content-Type: application/json' \
  -d '{"text":"wifi keeps dropping all the time"}' | jq '.primary'

# Test cancellation (triggers churn_prevention journey)
curl -s -X POST http://localhost:3143/api/classify -H 'Content-Type: application/json' \
  -d '{"text":"I want to cancel my account"}' | jq '.primary'
```

Check that:
- Each test phrase maps to the expected intent
- Confidence scores are above 0.50 for clear phrases
- Ambiguous phrases surface reasonable alternatives
