/**
 * @module nexus-cx/nba/engine
 * @description Next-Best-Action (NBA) recommendation engine for real-time action orchestration.
 *
 * Implements a three-tier evaluation approach:
 *   1. **Deterministic rules**: business rules that always fire when conditions match
 *      (e.g., high churn risk → retention offer). Auditable and explainable.
 *   2. **Scored recommendations**: simulated ML-based scoring using customer context,
 *      journey state, and interaction history to rank candidate actions
 *   3. **Channel-specific routing**: directs actions to the appropriate delivery channel
 *      (voice prompt, chat message, email, SMS) with priority-based ordering
 *
 * Action types: retention_offer, escalation, self_service_nudge, proactive_outreach,
 * cross_sell, survey_trigger, knowledge_article, callback_schedule
 *
 * Persistence: action log is write-through to `nba_actions` SQLite table when
 * `PERSIST=true`. Statistics (action counts, conversion rates) are recalculated
 * from the hydrated action log on startup.
 *
 * @requires ./rules
 * @requires uuid
 * @requires ../config
 * @requires ../logger
 *
 * @see {@link module:nexus-cx/nba/rules} Deterministic business rules
 * @see {@link module:nexus-cx/pipeline} Pipeline orchestrator (consumer)
 */
const { RULES } = require('./rules');
const { v4: uuidv4 } = require('uuid');
const config = require('../config');
const { createLogger } = require('../logger');

const log = createLogger('nba');

class NBAEngine {
  constructor() {
    this.actionLog = [];
    this.stats = {
      totalEvaluations: 0,
      actionsTriggered: 0,
      deflectionsTriggered: 0,
      byActionType: {},
      byChannel: {},
      byUrgency: {},
      priorityDistribution: {}
    };
    this._actionStmt = null;
  }

  hydrate() {
    if (!config.PERSIST) return;
    try {
      const { getDatabase } = require('../store/database');
      const db = getDatabase();
      const rows = db.prepare(
        'SELECT data FROM nba_actions ORDER BY id DESC LIMIT ?'
      ).all(config.NBA_ACTION_LOG_MAX);

      for (let i = rows.length - 1; i >= 0; i--) {
        try {
          const action = JSON.parse(rows[i].data);
          this.actionLog.push(action);
          this.stats.actionsTriggered++;
          if (action.type) {
            this.stats.byActionType[action.type] = (this.stats.byActionType[action.type] || 0) + 1;
          }
        } catch {}
      }
      log.info({ loaded: this.actionLog.length }, 'NBA action log hydrated');
    } catch {}
  }

  _persistAction(action) {
    if (!config.PERSIST) return;
    try {
      if (!this._actionStmt) {
        const { getDatabase } = require('../store/database');
        this._actionStmt = getDatabase().prepare(
          `INSERT INTO nba_actions (action_id, action_type, urgency, data, timestamp)
           VALUES (@action_id, @action_type, @urgency, @data, @timestamp)`
        );
      }
      this._actionStmt.run({
        action_id: action.action_id || '',
        action_type: action.type || '',
        urgency: action.urgency || '',
        data: JSON.stringify(action),
        timestamp: action.timestamp || new Date().toISOString()
      });
    } catch {}
  }

  /**
   * Evaluate all rules and return ranked actions for the given context.
   * @param {Object} ctx - { event, classification, profile, journey, analysis }
   */
  evaluate(ctx) {
    this.stats.totalEvaluations++;

    const channel = ctx.event?.channel || 'web';
    const routing = this._buildRoutingContext(ctx);
    const enrichedContext = {
      ...ctx,
      analysis: ctx.analysis || ctx.event?.analysis || null,
      routing
    };

    const matchedActions = [];
    const ruleErrors = [];

    for (const rule of RULES) {
      try {
        if (rule.condition(enrichedContext)) {
          matchedActions.push({
            rule_id: rule.id,
            rule_name: rule.name,
            priority: rule.priority,
            ...rule.action,
            source: 'deterministic'
          });
        }
      } catch (error) {
        ruleErrors.push({ rule_id: rule.id, error: error.message });
        log.warn({ ruleId: rule.id, err: error }, 'Rule evaluation failed');
      }
    }

    const mlScores = this._simulateMLScoring(enrichedContext);
    for (const score of mlScores) {
      matchedActions.push({
        ...score,
        source: 'ml_scored'
      });
    }

    const rankedActions = this._deduplicateActions(
      matchedActions.sort((a, b) => (b.priority || 0) - (a.priority || 0))
    ).slice(0, 3);

    const routedActions = rankedActions.map(action => ({
      action_id: uuidv4(),
      ...action,
      delivery: this._routeToChannel(action, channel, routing),
      decision_factors: this._buildDecisionFactors(enrichedContext, action),
      timestamp: new Date().toISOString()
    }));

    for (const action of routedActions) {
      this.actionLog.push(action);
      this._persistAction(action);
      this.stats.actionsTriggered++;
      this.stats.byActionType[action.type] = (this.stats.byActionType[action.type] || 0) + 1;
      this.stats.byChannel[channel] = (this.stats.byChannel[channel] || 0) + 1;
      this.stats.byUrgency[action.urgency] = (this.stats.byUrgency[action.urgency] || 0) + 1;
      if (['self_service_redirect', 'digital_deflection'].includes(action.type)) {
        this.stats.deflectionsTriggered++;
      }
    }

    this.stats.priorityDistribution[routing.priority] = (this.stats.priorityDistribution[routing.priority] || 0) + 1;

    return {
      actions: routedActions,
      totalRulesMatched: matchedActions.length,
      channel,
      routing,
      ruleErrors,
      decisionSummary: {
        topActionType: routedActions[0]?.type || null,
        shouldDeflect: routing.should_deflect,
        contactValue: routing.contact_value,
        slaStatus: routing.sla_status
      }
    };
  }

  _buildRoutingContext(ctx) {
    const event = ctx.event || {};
    const channel = event.channel || 'web';
    const classification = ctx.classification || event.classification || {};
    const analysis = ctx.analysis || event.analysis || {};
    const profile = ctx.profile || {};
    const journey = ctx.journey || {};
    const metadata = event.payload?.metadata || {};
    const configuredWait = Number(event.context?.queue_wait_minutes || 0);

    let priority = basePriorityByChannel(channel);

    const tier = profile.attributes?.customer_tier || metadata.customer_tier || 'standard';
    if (tier === 'premium' || tier === 'enterprise') priority += 3;
    else if (tier === 'gold') priority += 2;

    const urgency = analysis.urgency || 'medium';
    if (urgency === 'critical') priority += 3;
    else if (urgency === 'high') priority += 2;
    else if (urgency === 'medium') priority += 1;

    const domain = classification?.primary?.domain;
    if (['billing', 'technical_support', 'account'].includes(domain)) priority += 1;
    if (classification?.primary?.fullKey === 'account.cancellation') priority += 2;

    const sentiment = analysis.sentiment || 'neutral';
    if (sentiment === 'angry') priority += 3;
    else if (sentiment === 'frustrated' || sentiment === 'negative') priority += 2;
    else if (sentiment === 'positive') priority -= 1;

    const estimatedWaitTime = configuredWait > 0 ? configuredWait : this._estimateWaitTime(channel, priority);
    if (estimatedWaitTime > 10) priority += 1;
    if (estimatedWaitTime > 20) priority += 1;

    const skillMatchScore = this._estimateSkillMatch(ctx);
    if (skillMatchScore < 50) priority += 2;
    else if (skillMatchScore < 75) priority += 1;

    if (journey?.status === 'active' && (journey?.cross_channel_handoffs || 0) > 0) {
      priority += 1;
    }

    priority = clamp(priority, 0, 10);

    const availableAgents = this._estimateAvailableAgents(channel, priority);
    const contactValue = this._estimateContactValue(ctx, priority);
    const slaStatus = this._computeSlaStatus(priority, estimatedWaitTime);
    const shouldDeflect = this._shouldDeflect(ctx, { priority, contactValue, sentiment, urgency });

    return {
      priority,
      available_agents: availableAgents,
      estimated_wait_time: estimatedWaitTime,
      skill_match_score: skillMatchScore,
      contact_value: contactValue,
      sla_status: slaStatus,
      should_deflect: shouldDeflect,
      recommended_queue: this._selectQueue(domain, priority, tier),
      channel
    };
  }

  /**
   * Simulate ML model scoring (in production: SageMaker endpoint)
   */
  _simulateMLScoring(ctx) {
    const scores = [];

    if (ctx.profile && ctx.profile.interaction_count > 5) {
      const churnScore = Math.min(0.95, (ctx.profile.interaction_count - 3) * 0.1);
      if (churnScore > 0.5) {
        scores.push({
          type: 'proactive_retention',
          urgency: churnScore > 0.75 ? 'high' : 'medium',
          priority: Math.round(churnScore * 55),
          content: 'ML model predicts elevated churn risk (' + Math.round(churnScore * 100) + '%)',
          params: { churn_score: round(churnScore), contact_value: ctx.routing.contact_value },
          ml_model: 'churn_propensity_v2'
        });
      }
    }

    if (ctx.classification?.primary?.domain === 'sales') {
      scores.push({
        type: 'upsell_recommendation',
        urgency: 'low',
        priority: 25 + Math.round(ctx.routing.contact_value / 10),
        content: 'Customer is showing purchase intent — recommend premium features',
        params: {
          conversion_probability: 0.35,
          recommended_queue: ctx.routing.recommended_queue
        },
        ml_model: 'upsell_propensity_v2'
      });
    }

    if (ctx.analysis?.sentiment === 'angry' && ctx.routing.priority >= 8) {
      scores.push({
        type: 'manager_callback',
        urgency: 'high',
        priority: 78,
        content: 'High-risk interaction detected — schedule rapid manager callback',
        params: { callback_within_minutes: 15 },
        ml_model: 'friction_recovery_v1'
      });
    }

    return scores;
  }

  _deduplicateActions(actions) {
    const seen = new Set();
    const deduped = [];

    for (const action of actions) {
      const key = action.type + ':' + (action.rule_id || action.ml_model || action.content);
      if (seen.has(key)) continue;
      seen.add(key);
      deduped.push(action);
    }

    return deduped;
  }

  _estimateWaitTime(channel, priority) {
    const baseline = {
      voice: 8,
      chat: 4,
      web: 2,
      mobile: 3,
      email: 25,
      sms: 5,
      whatsapp: 6
    };

    const rawWait = (baseline[channel] || 4) - Math.min(priority, 5) * 0.4;
    return round(Math.max(1, rawWait));
  }

  _estimateSkillMatch(ctx) {
    const requiredSkills = Array.isArray(ctx.event?.context?.required_skills)
      ? ctx.event.context.required_skills.length
      : 0;
    const domain = ctx.classification?.primary?.domain;

    let score = 85;
    if (requiredSkills >= 3) score -= 20;
    else if (requiredSkills === 2) score -= 10;

    if (domain === 'technical_support') score -= 10;
    if (ctx.analysis?.urgency === 'critical') score -= 10;
    if (ctx.profile?.channels_seen?.length >= 3) score -= 5;

    return clamp(score, 35, 98);
  }

  _estimateAvailableAgents(channel, priority) {
    const baseline = {
      voice: 6,
      chat: 10,
      web: 20,
      mobile: 18,
      email: 14,
      sms: 12,
      whatsapp: 8
    };

    const available = (baseline[channel] || 8) - Math.max(0, priority - 5);
    return Math.max(1, available);
  }

  _estimateContactValue(ctx, priority) {
    let value = 35 + priority * 4;
    const tier = ctx.profile?.attributes?.customer_tier || ctx.event?.payload?.metadata?.customer_tier || 'standard';

    if (tier === 'premium' || tier === 'enterprise') value += 20;
    else if (tier === 'gold') value += 10;

    if (ctx.classification?.primary?.domain === 'sales') value += 15;
    if (ctx.classification?.primary?.fullKey === 'account.cancellation') value += 20;
    if (ctx.journey?.journey_type === 'churn_prevention') value += 15;
    if (ctx.analysis?.sentiment === 'angry') value += 10;

    return clamp(value, 1, 100);
  }

  _computeSlaStatus(priority, estimatedWaitTime) {
    if (priority >= 8 && estimatedWaitTime > 6) return 'breached';
    if (priority >= 7 && estimatedWaitTime > 4) return 'at_risk';
    if (priority >= 5 && estimatedWaitTime > 8) return 'at_risk';
    return 'compliant';
  }

  _shouldDeflect(ctx, { priority, contactValue, sentiment, urgency }) {
    const intent = ctx.classification?.primary?.fullKey;
    const channel = ctx.event?.channel;
    const allowedChannels = ['web', 'mobile', 'chat', 'sms', 'whatsapp'];
    const deflectableIntents = new Set([
      'account.password_reset',
      'billing.billing_inquiry',
      'technical_support.connectivity',
      'technical_support.app_error'
    ]);

    if (!allowedChannels.includes(channel)) return false;
    if (!deflectableIntents.has(intent)) return false;
    if (priority >= 7 || contactValue >= 80) return false;
    if (urgency === 'high' || urgency === 'critical') return false;
    if (['negative', 'frustrated', 'angry'].includes(sentiment)) return false;
    if (ctx.journey?.journey_type === 'churn_prevention') return false;

    return true;
  }

  _selectQueue(domain, priority, tier) {
    if (tier === 'premium' || tier === 'enterprise') return 'premium-support';
    if (priority >= 8) return 'priority-escalations';
    if (domain === 'technical_support') return 'technical-support';
    if (domain === 'billing') return 'billing-care';
    if (domain === 'sales') return 'sales-specialists';
    return 'general-support';
  }

  _buildDecisionFactors(ctx, action) {
    return {
      intent: ctx.classification?.primary?.fullKey || null,
      journey_type: ctx.journey?.journey_type || null,
      sentiment: ctx.analysis?.sentiment || null,
      urgency: ctx.analysis?.urgency || null,
      routing_priority: ctx.routing.priority,
      sla_status: ctx.routing.sla_status,
      contact_value: ctx.routing.contact_value,
      source: action.source
    };
  }

  /**
   * Route action to channel-specific delivery mechanism
   */
  _routeToChannel(action, channel, routing) {
    const deliveryMap = {
      voice: {
        mechanism: 'agent_assist',
        description: 'Push to Amazon Q in Connect agent assist panel',
        format: 'suggested_response'
      },
      chat: {
        mechanism: 'chat_suggestion',
        description: 'Display as suggested response in agent CCP',
        format: 'rich_card'
      },
      web: {
        mechanism: 'websocket_push',
        description: 'Real-time push via WebSocket to web app',
        format: 'in_app_notification'
      },
      mobile: {
        mechanism: 'push_notification',
        description: 'Mobile push notification or in-app message',
        format: 'push'
      },
      email: {
        mechanism: 'ses_outbound',
        description: 'Send via Amazon SES or Connect outbound',
        format: 'email_template'
      },
      sms: {
        mechanism: 'sns_sms',
        description: 'Send via Amazon SNS SMS',
        format: 'short_text'
      },
      whatsapp: {
        mechanism: 'whatsapp_api',
        description: 'Send via WhatsApp Business API',
        format: 'rich_message'
      }
    };

    const base = deliveryMap[channel] || deliveryMap.web;

    return {
      ...base,
      queue: routing.recommended_queue,
      priority: routing.priority
    };
  }

  getActionLog(limit = 50) {
    return this.actionLog.slice(-limit);
  }

  getStats() {
    return { ...this.stats };
  }
}

function basePriorityByChannel(channel) {
  const priorities = {
    voice: 4,
    chat: 3,
    web: 2,
    mobile: 2,
    email: 1,
    sms: 2,
    whatsapp: 2
  };
  return priorities[channel] || 2;
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function round(value) {
  return Math.round(value * 100) / 100;
}

// Singleton
let instance = null;
function getNBAEngine() {
  if (!instance) instance = new NBAEngine();
  return instance;
}

module.exports = { NBAEngine, getNBAEngine };
