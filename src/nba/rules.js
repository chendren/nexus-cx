/**
 * @module nexus-cx/nba/rules
 * @description Deterministic business rules for the Next-Best-Action engine.
 *
 * Each rule defines a condition function evaluated against the event context
 * (customer profile, journey state, classification, interaction history) and
 * an action payload to execute when the condition is satisfied.
 *
 * Rules always fire when conditions match — they are auditable, explainable,
 * and take priority over ML-scored recommendations. In production, these would
 * be managed in a rules engine (e.g., DynamoDB-backed configuration store).
 *
 * Rule structure:
 *   - `id` {string} — unique rule identifier (e.g., 'CHURN_RISK_HIGH')
 *   - `name` {string} — human-readable rule name
 *   - `priority` {number} — execution priority (higher = evaluated first)
 *   - `condition` {Function} — predicate receiving `{ event, profile, journey, classification }`
 *   - `action` {Object} — action payload with type, urgency, content, and params
 *
 * @see {@link module:nexus-cx/nba/engine} NBA engine (consumer)
 */

/**
 * Ordered array of deterministic business rules evaluated by the NBA engine.
 * Rules are sorted by priority (highest first) and all matching rules fire
 * simultaneously — the engine deduplicates and ranks the resulting actions.
 *
 * Priority bands:
 *   90-100: Critical retention and complaint handling
 *   74-88:  SLA recovery, VIP routing, cross-channel frustration, repeated issues
 *   50-60:  Proactive opportunities (upgrades, onboarding, deflection)
 *   30-40:  Low-urgency self-service and feedback follow-ups
 *
 * @type {Array<{ id: string, name: string, priority: number, condition: Function, action: { type: string, urgency: string, content: string, params: Object } }>}
 */
const RULES = [
  // ─── Critical Retention (priority 95-100) ─────────────────────────────
  {
    id: 'CHURN_RISK_HIGH',
    name: 'High Churn Risk Intervention',
    priority: 100,
    condition: (ctx) =>
      ctx.journey?.journey_type === 'churn_prevention' &&
      ctx.journey?.current_state === 'cancel_requested',
    action: {
      type: 'retention_offer',
      urgency: 'critical',
      content: 'Offer 25% discount for 6 months to retain the customer',
      params: { discount_pct: 25, duration_months: 6 }
    }
  },
  {
    id: 'CHURN_FINAL',
    name: 'Final Churn Prevention Attempt',
    priority: 95,
    condition: (ctx) =>
      ctx.journey?.journey_type === 'churn_prevention' &&
      ctx.journey?.current_state === 'final_attempt',
    action: {
      type: 'escalate_retention',
      urgency: 'critical',
      content: 'Escalate to retention specialist with authority for 50% discount or free upgrade',
      params: { max_discount_pct: 50, allow_free_upgrade: true }
    }
  },
  // ─── VIP and Complaint Handling (priority 85-92) ────────────────────────
  {
    id: 'VIP_PRIORITY_ROUTING',
    name: 'VIP Priority Routing',
    priority: 92,
    condition: (ctx) =>
      ctx.routing?.priority >= 8 &&
      ['premium', 'gold', 'enterprise'].includes(ctx.profile?.attributes?.customer_tier),
    action: {
      type: 'priority_route',
      urgency: 'high',
      content: 'Route to premium support queue with best available agent',
      params: { route_senior: true, premium_queue: true }
    }
  },
  // ─── SLA and Escalation Recovery (priority 74-88) ───────────────────────
  {
    id: 'SLA_AT_RISK',
    name: 'SLA At Risk Recovery',
    priority: 88,
    condition: (ctx) =>
      ctx.routing?.sla_status === 'at_risk' || ctx.routing?.sla_status === 'breached',
    action: {
      type: 'queue_rebalance',
      urgency: 'high',
      content: 'SLA is at risk — boost queue priority and rebalance workload',
      params: { rebalance: true, queue_boost: 2 }
    }
  },
  {
    id: 'BILLING_DISPUTE_STUCK',
    name: 'Billing Dispute Stuck',
    priority: 80,
    condition: (ctx) =>
      ctx.journey?.journey_type === 'billing_dispute' &&
      (ctx.journey?.is_stuck || ctx.journey?.sla_status === 'breached'),
    action: {
      type: 'escalate_supervisor',
      urgency: 'high',
      content: 'Billing dispute is stuck — escalate to a billing supervisor',
      params: { escalation_level: 'supervisor' }
    }
  },
  {
    id: 'REPEATED_TECH_ISSUE',
    name: 'Repeated Technical Issue',
    priority: 75,
    condition: (ctx) =>
      ctx.classification?.primary?.domain === 'technical_support' &&
      ctx.profile?.interaction_count > 3 &&
      ctx.journey?.journey_type === 'technical_troubleshooting' &&
      ctx.journey?.context?.total_transitions > 2,
    action: {
      type: 'proactive_outreach',
      urgency: 'high',
      content: 'Customer has repeated technical issues — assign dedicated tech specialist',
      params: { assign_specialist: true, follow_up_hours: 24 }
    }
  },
  {
    id: 'HIGH_EFFORT_RECOVERY',
    name: 'High Effort Recovery',
    priority: 74,
    condition: (ctx) =>
      ctx.analysis?.customerEffortScore >= 4 &&
      ['negative', 'frustrated', 'angry'].includes(ctx.analysis?.sentiment),
    action: {
      type: 'effort_recovery',
      urgency: 'high',
      content: 'High-effort experience detected — acknowledge effort and offer concierge follow-up',
      params: { proactive_follow_up: true, waive_fee_review: true }
    }
  },
  // ─── Proactive Opportunities (priority 50-60) ──────────────────────────
  {
    id: 'UPGRADE_OPPORTUNITY',
    name: 'Upgrade Opportunity',
    priority: 60,
    condition: (ctx) =>
      ctx.classification?.primary?.domain === 'sales' &&
      (ctx.classification?.primary?.intent === 'upgrade' ||
       ctx.classification?.primary?.intent === 'product_inquiry') &&
      ctx.profile?.attributes?.segment !== 'enterprise',
    action: {
      type: 'upsell_offer',
      urgency: 'medium',
      content: 'Present premium tier comparison showing value proposition',
      params: { show_comparison: true, offer_trial: true }
    }
  },
  {
    id: 'NEW_CUSTOMER_ONBOARDING',
    name: 'New Customer Welcome',
    priority: 50,
    condition: (ctx) =>
      ctx.journey?.journey_type === 'new_account_setup' &&
      ctx.journey?.current_state === 'started' &&
      ctx.profile?.interaction_count <= 2,
    action: {
      type: 'onboarding_guide',
      urgency: 'medium',
      content: 'Welcome new customer with guided onboarding flow and setup assistance',
      params: { send_welcome_email: true, assign_onboarding_rep: false }
    }
  },
  {
    id: 'COMPLAINT_HANDLING',
    name: 'Active Complaint Handler',
    priority: 90,
    condition: (ctx) =>
      ctx.classification?.primary?.fullKey === 'general.complaint',
    action: {
      type: 'complaint_protocol',
      urgency: 'high',
      content: 'Activate complaint handling protocol — empathize, acknowledge, and offer resolution',
      params: { log_complaint: true, notify_manager: true, sla_hours: 4 }
    }
  },
  {
    id: 'CROSS_CHANNEL_FRUSTRATION',
    name: 'Cross-Channel Frustration Detection',
    priority: 85,
    condition: (ctx) =>
      ctx.profile?.channels_seen?.length >= 3 &&
      ctx.profile?.interaction_count > 5 &&
      ctx.journey?.status === 'active',
    action: {
      type: 'vip_handling',
      urgency: 'high',
      content: 'Customer has contacted through 3+ channels — route to a senior omnichannel specialist',
      params: { route_senior: true, no_queue: true }
    }
  },
  {
    id: 'DIGITAL_DEFLECTION',
    name: 'Digital Deflection Opportunity',
    priority: 55,
    condition: (ctx) =>
      ctx.routing?.should_deflect === true &&
      ctx.classification?.primary?.fullKey !== 'account.password_reset',
    action: {
      type: 'digital_deflection',
      urgency: 'medium',
      content: 'Offer a guided self-service flow before routing to a live agent',
      params: { offer_help_center: true, offer_virtual_assistant: true }
    }
  },
  // ─── Self-Service and Feedback (priority 30-40) ─────────────────────────
  {
    id: 'PASSWORD_RESET_ASSIST',
    name: 'Password Reset Self-Service',
    priority: 40,
    condition: (ctx) =>
      ctx.classification?.primary?.fullKey === 'account.password_reset',
    action: {
      type: 'self_service_redirect',
      urgency: 'low',
      content: 'Guide customer to self-service password reset portal',
      params: { redirect_url: '/reset-password', send_reset_email: true }
    }
  },
  {
    id: 'POSITIVE_FEEDBACK_FOLLOWUP',
    name: 'Positive Feedback Follow-up',
    priority: 30,
    condition: (ctx) =>
      ctx.classification?.primary?.fullKey === 'general.compliment',
    action: {
      type: 'satisfaction_survey',
      urgency: 'low',
      content: 'Customer is happy — send satisfaction survey and NPS request',
      params: { send_survey: true, request_review: true }
    }
  }
];

module.exports = { RULES };
