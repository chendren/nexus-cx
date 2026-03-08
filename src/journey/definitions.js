/**
 * @module nexus-cx/journey/definitions
 * @description Finite state machine definitions for all supported customer journey types.
 *
 * Each journey definition specifies:
 *   - `states` {string[]} — all valid states in the journey lifecycle
 *   - `initialState` {string} — entry state when the journey is created
 *   - `transitions` {Object} — mapping from each state to its allowed next states
 *   - `terminalStates` {string[]} — states that mark journey completion
 *   - `triggerIntents` {string[]} — classifier intents that initiate this journey type
 *
 * Supports 13 journey types:
 *   **Commercial CX** (5): billing_dispute, technical_troubleshooting, new_account_setup,
 *     churn_prevention, service_upgrade
 *   **Treasury Core** (4): treasury_check_replacement, bond_redemption, debt_offset_dispute,
 *     treasury_account_recovery
 *   **Treasury Expanded** (4): tax_identity_recovery, tax_debt_resolution, audit_response,
 *     foreign_compliance
 *
 * Also exports `findMatchingJourneys(classification)` which maps classified intents
 * to applicable journey types, and `getJourneyDefinition(type)` for state graph lookup.
 *
 * @see {@link module:nexus-cx/journey/state-machine} Journey engine (consumer)
 */

const JOURNEY_DEFINITIONS = {
  billing_dispute: {
    label: 'Billing Dispute Resolution',
    description: 'Customer disputes a charge on their account',
    triggerIntents: ['billing.refund_request', 'billing.billing_inquiry'],
    initialState: 'initiated',
    terminalStates: ['resolved', 'abandoned'],
    states: {
      initiated: {
        label: 'Dispute Initiated',
        transitions: {
          'billing.billing_inquiry': 'under_review',
          'billing.refund_request': 'refund_processing',
          'general.complaint': 'escalated',
          _timeout: { state: 'abandoned', afterMinutes: 1440 }
        }
      },
      under_review: {
        label: 'Under Review',
        transitions: {
          'billing.refund_request': 'refund_processing',
          'general.complaint': 'escalated',
          'general.compliment': 'resolved',
          _timeout: { state: 'abandoned', afterMinutes: 4320 }
        }
      },
      refund_processing: {
        label: 'Refund Processing',
        transitions: {
          'general.compliment': 'resolved',
          'general.complaint': 'escalated',
          _timeout: { state: 'resolved', afterMinutes: 2880 }
        }
      },
      escalated: {
        label: 'Escalated to Supervisor',
        transitions: {
          'billing.refund_request': 'refund_processing',
          'general.compliment': 'resolved',
          _timeout: { state: 'resolved', afterMinutes: 1440 }
        }
      },
      resolved: { label: 'Resolved', transitions: {} },
      abandoned: { label: 'Abandoned', transitions: {} }
    }
  },

  technical_troubleshooting: {
    label: 'Technical Troubleshooting',
    description: 'Customer experiencing a technical issue',
    triggerIntents: ['technical_support.connectivity', 'technical_support.device_issue', 'technical_support.app_error'],
    initialState: 'reported',
    terminalStates: ['resolved', 'abandoned'],
    states: {
      reported: {
        label: 'Issue Reported',
        transitions: {
          'technical_support.connectivity': 'diagnosing',
          'technical_support.device_issue': 'diagnosing',
          'technical_support.app_error': 'diagnosing',
          'technical_support.service_outage': 'waiting_for_fix',
          _timeout: { state: 'abandoned', afterMinutes: 1440 }
        }
      },
      diagnosing: {
        label: 'Diagnosing Issue',
        transitions: {
          'technical_support.service_outage': 'waiting_for_fix',
          'general.complaint': 'escalated',
          'general.compliment': 'resolved',
          _timeout: { state: 'follow_up', afterMinutes: 720 }
        }
      },
      waiting_for_fix: {
        label: 'Waiting for Fix',
        transitions: {
          'general.compliment': 'resolved',
          'general.complaint': 'escalated',
          _timeout: { state: 'follow_up', afterMinutes: 1440 }
        }
      },
      follow_up: {
        label: 'Follow-up Needed',
        transitions: {
          'technical_support.connectivity': 'diagnosing',
          'technical_support.device_issue': 'diagnosing',
          'general.compliment': 'resolved',
          _timeout: { state: 'abandoned', afterMinutes: 4320 }
        }
      },
      escalated: {
        label: 'Escalated',
        transitions: {
          'general.compliment': 'resolved',
          _timeout: { state: 'resolved', afterMinutes: 2880 }
        }
      },
      resolved: { label: 'Resolved', transitions: {} },
      abandoned: { label: 'Abandoned', transitions: {} }
    }
  },

  new_account_setup: {
    label: 'New Account Setup',
    description: 'Customer creating and setting up a new account',
    triggerIntents: ['account.account_creation', 'sales.new_service'],
    initialState: 'started',
    terminalStates: ['active', 'abandoned'],
    states: {
      started: {
        label: 'Setup Started',
        transitions: {
          'account.account_creation': 'profile_created',
          'sales.pricing': 'comparing_plans',
          'sales.product_inquiry': 'comparing_plans',
          _timeout: { state: 'abandoned', afterMinutes: 1440 }
        }
      },
      comparing_plans: {
        label: 'Comparing Plans',
        transitions: {
          'account.account_creation': 'profile_created',
          'sales.upgrade': 'profile_created',
          'billing.plan_change': 'profile_created',
          _timeout: { state: 'abandoned', afterMinutes: 4320 }
        }
      },
      profile_created: {
        label: 'Profile Created',
        transitions: {
          'billing.payment_issue': 'payment_setup',
          'billing.billing_inquiry': 'payment_setup',
          'general.compliment': 'active',
          _timeout: { state: 'active', afterMinutes: 1440 }
        }
      },
      payment_setup: {
        label: 'Payment Setup',
        transitions: {
          'general.compliment': 'active',
          'billing.payment_issue': 'payment_setup',
          _timeout: { state: 'active', afterMinutes: 720 }
        }
      },
      active: { label: 'Account Active', transitions: {} },
      abandoned: { label: 'Abandoned', transitions: {} }
    }
  },

  churn_prevention: {
    label: 'Churn Prevention',
    description: 'Customer showing signs of leaving or requesting cancellation',
    triggerIntents: ['account.cancellation'],
    initialState: 'cancel_requested',
    terminalStates: ['retained', 'cancelled'],
    states: {
      cancel_requested: {
        label: 'Cancellation Requested',
        transitions: {
          'sales.pricing': 'retention_offer',
          'sales.product_inquiry': 'retention_offer',
          'general.complaint': 'escalated',
          'general.compliment': 'retained',
          _timeout: { state: 'cancelled', afterMinutes: 1440 }
        }
      },
      retention_offer: {
        label: 'Retention Offer Made',
        transitions: {
          'billing.plan_change': 'retained',
          'sales.upgrade': 'retained',
          'account.cancellation': 'final_attempt',
          'general.compliment': 'retained',
          _timeout: { state: 'cancelled', afterMinutes: 2880 }
        }
      },
      final_attempt: {
        label: 'Final Retention Attempt',
        transitions: {
          'general.compliment': 'retained',
          'billing.plan_change': 'retained',
          _timeout: { state: 'cancelled', afterMinutes: 720 }
        }
      },
      escalated: {
        label: 'Escalated to Retention Team',
        transitions: {
          'general.compliment': 'retained',
          'billing.plan_change': 'retained',
          'account.cancellation': 'cancelled',
          _timeout: { state: 'cancelled', afterMinutes: 1440 }
        }
      },
      retained: { label: 'Customer Retained', transitions: {} },
      cancelled: { label: 'Account Cancelled', transitions: {} }
    }
  },

  service_upgrade: {
    label: 'Service Upgrade',
    description: 'Customer exploring or completing a service upgrade',
    triggerIntents: ['sales.upgrade', 'sales.product_inquiry', 'sales.pricing'],
    initialState: 'exploring',
    terminalStates: ['completed', 'abandoned'],
    states: {
      exploring: {
        label: 'Exploring Options',
        transitions: {
          'sales.pricing': 'comparing',
          'sales.product_inquiry': 'comparing',
          'sales.upgrade': 'processing',
          _timeout: { state: 'abandoned', afterMinutes: 4320 }
        }
      },
      comparing: {
        label: 'Comparing Plans',
        transitions: {
          'sales.upgrade': 'processing',
          'billing.plan_change': 'processing',
          _timeout: { state: 'abandoned', afterMinutes: 4320 }
        }
      },
      processing: {
        label: 'Processing Upgrade',
        transitions: {
          'billing.payment_issue': 'payment_issue',
          'general.compliment': 'completed',
          _timeout: { state: 'completed', afterMinutes: 720 }
        }
      },
      payment_issue: {
        label: 'Payment Issue',
        transitions: {
          'billing.payment_issue': 'payment_issue',
          'general.compliment': 'completed',
          _timeout: { state: 'abandoned', afterMinutes: 1440 }
        }
      },
      completed: { label: 'Upgrade Completed', transitions: {} },
      abandoned: { label: 'Abandoned', transitions: {} }
    }
  },

  // ═══════════════════════════════════════════════════
  // US Treasury Journey Types
  // ═══════════════════════════════════════════════════

  treasury_check_replacement: {
    label: 'Treasury Check Replacement',
    description: 'Citizen reporting a lost, stolen, or undelivered Treasury check',
    triggerIntents: ['treasury_payments.check_replacement', 'treasury_payments.unclaimed_money'],
    initialState: 'reported',
    terminalStates: ['resolved', 'abandoned'],
    states: {
      reported: {
        label: 'Check Issue Reported',
        transitions: {
          'treasury_payments.direct_deposit': 'payment_redirect',
          'treasury_payments.check_replacement': 'investigating',
          'debt_collection.treasury_offset': 'offset_review',
          'general.complaint': 'escalated',
          _timeout: { state: 'investigating', afterMinutes: 1440 }
        }
      },
      investigating: {
        label: 'Under Investigation',
        transitions: {
          'treasury_payments.check_replacement': 'reissuing',
          'treasury_payments.direct_deposit': 'payment_redirect',
          'general.complaint': 'escalated',
          _timeout: { state: 'reissuing', afterMinutes: 4320 }
        }
      },
      reissuing: {
        label: 'Check Being Reissued',
        transitions: {
          'general.compliment': 'resolved',
          'treasury_payments.check_replacement': 'reissuing',
          'general.complaint': 'escalated',
          _timeout: { state: 'resolved', afterMinutes: 10080 }
        }
      },
      payment_redirect: {
        label: 'Redirecting to Direct Deposit',
        transitions: {
          'treasury_payments.direct_deposit': 'payment_redirect',
          'general.compliment': 'resolved',
          _timeout: { state: 'resolved', afterMinutes: 2880 }
        }
      },
      offset_review: {
        label: 'Offset Review',
        transitions: {
          'debt_collection.offset_dispute': 'escalated',
          'treasury_payments.check_replacement': 'reissuing',
          _timeout: { state: 'resolved', afterMinutes: 4320 }
        }
      },
      escalated: {
        label: 'Escalated to Supervisor',
        transitions: {
          'general.compliment': 'resolved',
          _timeout: { state: 'resolved', afterMinutes: 2880 }
        }
      },
      resolved: { label: 'Resolved', transitions: {} },
      abandoned: { label: 'Abandoned', transitions: {} }
    }
  },

  bond_redemption: {
    label: 'Savings Bond Redemption',
    description: 'Citizen redeeming or inheriting savings bonds',
    triggerIntents: ['savings_bonds.bond_redemption', 'savings_bonds.bond_inheritance'],
    initialState: 'initiated',
    terminalStates: ['completed', 'abandoned'],
    states: {
      initiated: {
        label: 'Redemption Initiated',
        transitions: {
          'savings_bonds.bond_redemption': 'verification',
          'savings_bonds.bond_inheritance': 'estate_processing',
          'savings_bonds.bond_purchase': 'reinvestment',
          _timeout: { state: 'abandoned', afterMinutes: 4320 }
        }
      },
      verification: {
        label: 'Bond Verification',
        transitions: {
          'savings_bonds.bond_redemption': 'processing',
          'treasury_payments.direct_deposit': 'processing',
          'general.complaint': 'escalated',
          _timeout: { state: 'processing', afterMinutes: 2880 }
        }
      },
      estate_processing: {
        label: 'Estate Documentation Review',
        transitions: {
          'savings_bonds.bond_inheritance': 'estate_processing',
          'savings_bonds.bond_redemption': 'processing',
          'general.complaint': 'escalated',
          _timeout: { state: 'abandoned', afterMinutes: 10080 }
        }
      },
      processing: {
        label: 'Redemption Processing',
        transitions: {
          'treasury_payments.direct_deposit': 'payment_pending',
          'general.compliment': 'completed',
          _timeout: { state: 'payment_pending', afterMinutes: 2880 }
        }
      },
      payment_pending: {
        label: 'Payment Pending',
        transitions: {
          'general.compliment': 'completed',
          'treasury_payments.check_replacement': 'payment_issue',
          _timeout: { state: 'completed', afterMinutes: 4320 }
        }
      },
      payment_issue: {
        label: 'Payment Issue',
        transitions: {
          'treasury_payments.direct_deposit': 'payment_pending',
          'general.complaint': 'escalated',
          _timeout: { state: 'escalated', afterMinutes: 2880 }
        }
      },
      reinvestment: {
        label: 'Reinvestment Options',
        transitions: {
          'savings_bonds.bond_purchase': 'completed',
          'savings_bonds.marketable_securities': 'completed',
          _timeout: { state: 'completed', afterMinutes: 4320 }
        }
      },
      escalated: {
        label: 'Escalated',
        transitions: {
          'general.compliment': 'completed',
          _timeout: { state: 'completed', afterMinutes: 2880 }
        }
      },
      completed: { label: 'Completed', transitions: {} },
      abandoned: { label: 'Abandoned', transitions: {} }
    }
  },

  debt_offset_dispute: {
    label: 'Debt Offset Dispute',
    description: 'Citizen disputing a Treasury offset on their federal payment',
    triggerIntents: ['debt_collection.offset_dispute', 'debt_collection.treasury_offset'],
    initialState: 'dispute_filed',
    terminalStates: ['resolved', 'abandoned'],
    states: {
      dispute_filed: {
        label: 'Dispute Filed',
        transitions: {
          'debt_collection.offset_dispute': 'under_review',
          'debt_collection.cross_servicing': 'debt_verification',
          'debt_collection.wage_garnishment': 'garnishment_review',
          'general.complaint': 'escalated',
          _timeout: { state: 'under_review', afterMinutes: 2880 }
        }
      },
      under_review: {
        label: 'Under Review',
        transitions: {
          'debt_collection.offset_dispute': 'under_review',
          'debt_collection.treasury_offset': 'under_review',
          'general.complaint': 'escalated',
          _timeout: { state: 'resolution', afterMinutes: 7200 }
        }
      },
      debt_verification: {
        label: 'Debt Verification',
        transitions: {
          'debt_collection.cross_servicing': 'payment_arrangement',
          'debt_collection.offset_dispute': 'under_review',
          _timeout: { state: 'under_review', afterMinutes: 4320 }
        }
      },
      payment_arrangement: {
        label: 'Payment Arrangement',
        transitions: {
          'debt_collection.cross_servicing': 'payment_arrangement',
          'general.compliment': 'resolved',
          _timeout: { state: 'resolved', afterMinutes: 4320 }
        }
      },
      garnishment_review: {
        label: 'Wage Garnishment Review',
        transitions: {
          'debt_collection.wage_garnishment': 'garnishment_review',
          'debt_collection.offset_dispute': 'under_review',
          'general.complaint': 'escalated',
          _timeout: { state: 'resolution', afterMinutes: 4320 }
        }
      },
      escalated: {
        label: 'Escalated',
        transitions: {
          'debt_collection.offset_dispute': 'under_review',
          'general.compliment': 'resolved',
          _timeout: { state: 'resolution', afterMinutes: 2880 }
        }
      },
      resolution: {
        label: 'Resolution Determination',
        transitions: {
          'general.compliment': 'resolved',
          'general.complaint': 'escalated',
          'debt_collection.cross_servicing': 'payment_arrangement',
          _timeout: { state: 'resolved', afterMinutes: 2880 }
        }
      },
      resolved: { label: 'Resolved', transitions: {} },
      abandoned: { label: 'Abandoned', transitions: {} }
    }
  },

  treasury_account_recovery: {
    label: 'TreasuryDirect Account Recovery',
    description: 'Citizen locked out of or having issues with their TreasuryDirect account',
    triggerIntents: ['treasury_payments.direct_deposit', 'treasury_payments.direct_express'],
    initialState: 'reported',
    terminalStates: ['access_restored', 'abandoned'],
    states: {
      reported: {
        label: 'Issue Reported',
        transitions: {
          'treasury_payments.direct_deposit': 'identity_verification',
          'treasury_payments.direct_express': 'card_replacement',
          'general.complaint': 'escalated',
          _timeout: { state: 'identity_verification', afterMinutes: 1440 }
        }
      },
      identity_verification: {
        label: 'Identity Verification',
        transitions: {
          'treasury_payments.direct_deposit': 'account_update',
          'general.complaint': 'escalated',
          _timeout: { state: 'abandoned', afterMinutes: 4320 }
        }
      },
      card_replacement: {
        label: 'Card Replacement',
        transitions: {
          'treasury_payments.direct_express': 'card_replacement',
          'treasury_payments.direct_deposit': 'account_update',
          'general.compliment': 'access_restored',
          _timeout: { state: 'access_restored', afterMinutes: 7200 }
        }
      },
      account_update: {
        label: 'Account Being Updated',
        transitions: {
          'general.compliment': 'access_restored',
          'treasury_payments.direct_deposit': 'account_update',
          'general.complaint': 'escalated',
          _timeout: { state: 'access_restored', afterMinutes: 2880 }
        }
      },
      escalated: {
        label: 'Escalated',
        transitions: {
          'general.compliment': 'access_restored',
          _timeout: { state: 'access_restored', afterMinutes: 2880 }
        }
      },
      access_restored: { label: 'Access Restored', transitions: {} },
      abandoned: { label: 'Abandoned', transitions: {} }
    }
  },

  // ═══════════════════════════════════════════════════
  // Expanded Treasury Journey Types
  // ═══════════════════════════════════════════════════

  tax_identity_recovery: {
    label: 'Tax Identity Theft Recovery',
    description: 'Citizen recovering from tax-related identity theft',
    triggerIntents: ['tax_identity.tax_identity_theft', 'tax_identity.tax_scam_reporting'],
    initialState: 'reported',
    terminalStates: ['resolved', 'abandoned'],
    states: {
      reported: {
        label: 'Identity Theft Reported',
        transitions: {
          'tax_identity.tax_identity_theft': 'investigation',
          'tax_identity.taxpayer_advocate': 'advocate_assigned',
          'tax_filing.tax_transcript': 'investigation',
          'general.complaint': 'escalated',
          _timeout: { state: 'investigation', afterMinutes: 2880 }
        }
      },
      investigation: {
        label: 'Under Investigation',
        transitions: {
          'tax_identity.tax_identity_theft': 'investigation',
          'tax_identity.taxpayer_advocate': 'advocate_assigned',
          'tax_payments.refund_status': 'refund_hold',
          'general.complaint': 'escalated',
          _timeout: { state: 'refund_hold', afterMinutes: 10080 }
        }
      },
      advocate_assigned: {
        label: 'Taxpayer Advocate Assigned',
        transitions: {
          'tax_identity.taxpayer_advocate': 'advocate_assigned',
          'general.compliment': 'resolved',
          _timeout: { state: 'resolved', afterMinutes: 20160 }
        }
      },
      refund_hold: {
        label: 'Refund on Hold Pending Resolution',
        transitions: {
          'tax_payments.refund_status': 'refund_hold',
          'general.compliment': 'resolved',
          'general.complaint': 'escalated',
          _timeout: { state: 'resolved', afterMinutes: 14400 }
        }
      },
      escalated: {
        label: 'Escalated',
        transitions: {
          'tax_identity.taxpayer_advocate': 'advocate_assigned',
          'general.compliment': 'resolved',
          _timeout: { state: 'resolved', afterMinutes: 4320 }
        }
      },
      resolved: { label: 'Resolved', transitions: {} },
      abandoned: { label: 'Abandoned', transitions: {} }
    }
  },

  tax_debt_resolution: {
    label: 'Tax Debt Resolution',
    description: 'Citizen resolving outstanding tax debt through payment plans or settlement',
    triggerIntents: ['tax_filing.installment_agreement', 'tax_filing.offer_compromise', 'tax_compliance.back_taxes'],
    initialState: 'initiated',
    terminalStates: ['resolved', 'abandoned'],
    states: {
      initiated: {
        label: 'Resolution Initiated',
        transitions: {
          'tax_filing.installment_agreement': 'payment_plan',
          'tax_filing.offer_compromise': 'oic_review',
          'tax_compliance.back_taxes': 'filing_catch_up',
          'tax_compliance.penalty_abatement': 'penalty_review',
          'tax_compliance.tax_levy': 'levy_hold',
          _timeout: { state: 'abandoned', afterMinutes: 7200 }
        }
      },
      filing_catch_up: {
        label: 'Filing Catch-Up',
        transitions: {
          'tax_filing.installment_agreement': 'payment_plan',
          'tax_filing.offer_compromise': 'oic_review',
          'tax_compliance.amended_return': 'filing_catch_up',
          'general.complaint': 'escalated',
          _timeout: { state: 'payment_plan', afterMinutes: 10080 }
        }
      },
      payment_plan: {
        label: 'Payment Plan Active',
        transitions: {
          'tax_filing.installment_agreement': 'payment_plan',
          'tax_compliance.penalty_abatement': 'penalty_review',
          'general.compliment': 'resolved',
          _timeout: { state: 'resolved', afterMinutes: 43200 }
        }
      },
      oic_review: {
        label: 'Offer in Compromise Under Review',
        transitions: {
          'tax_filing.offer_compromise': 'oic_review',
          'tax_identity.taxpayer_advocate': 'escalated',
          'general.complaint': 'escalated',
          _timeout: { state: 'resolved', afterMinutes: 20160 }
        }
      },
      penalty_review: {
        label: 'Penalty Abatement Review',
        transitions: {
          'tax_compliance.penalty_abatement': 'penalty_review',
          'tax_filing.installment_agreement': 'payment_plan',
          'general.compliment': 'resolved',
          _timeout: { state: 'payment_plan', afterMinutes: 4320 }
        }
      },
      levy_hold: {
        label: 'Levy Release Requested',
        transitions: {
          'tax_compliance.tax_levy': 'levy_hold',
          'tax_filing.installment_agreement': 'payment_plan',
          'general.complaint': 'escalated',
          _timeout: { state: 'payment_plan', afterMinutes: 2880 }
        }
      },
      escalated: {
        label: 'Escalated',
        transitions: {
          'tax_identity.taxpayer_advocate': 'escalated',
          'general.compliment': 'resolved',
          _timeout: { state: 'resolved', afterMinutes: 4320 }
        }
      },
      resolved: { label: 'Resolved', transitions: {} },
      abandoned: { label: 'Abandoned', transitions: {} }
    }
  },

  audit_response: {
    label: 'Audit Response',
    description: 'Citizen responding to an IRS audit or examination',
    triggerIntents: ['tax_compliance.audit_notice', 'tax_compliance.penalty_abatement'],
    initialState: 'notice_received',
    terminalStates: ['resolved', 'abandoned'],
    states: {
      notice_received: {
        label: 'Audit Notice Received',
        transitions: {
          'tax_compliance.audit_notice': 'documentation',
          'tax_filing.tax_transcript': 'documentation',
          'tax_identity.taxpayer_advocate': 'advocate_engaged',
          _timeout: { state: 'documentation', afterMinutes: 4320 }
        }
      },
      documentation: {
        label: 'Gathering Documentation',
        transitions: {
          'tax_filing.tax_transcript': 'documentation',
          'tax_compliance.amended_return': 'amended_filing',
          'tax_compliance.audit_notice': 'under_examination',
          'general.complaint': 'escalated',
          _timeout: { state: 'under_examination', afterMinutes: 10080 }
        }
      },
      under_examination: {
        label: 'Under Examination',
        transitions: {
          'tax_compliance.penalty_abatement': 'penalty_dispute',
          'tax_compliance.audit_notice': 'under_examination',
          'general.complaint': 'escalated',
          'general.compliment': 'resolved',
          _timeout: { state: 'resolved', afterMinutes: 20160 }
        }
      },
      amended_filing: {
        label: 'Amended Return Filed',
        transitions: {
          'tax_compliance.amended_return': 'amended_filing',
          'general.compliment': 'resolved',
          _timeout: { state: 'resolved', afterMinutes: 10080 }
        }
      },
      penalty_dispute: {
        label: 'Disputing Penalties',
        transitions: {
          'tax_compliance.penalty_abatement': 'penalty_dispute',
          'tax_identity.taxpayer_advocate': 'advocate_engaged',
          'general.compliment': 'resolved',
          _timeout: { state: 'resolved', afterMinutes: 7200 }
        }
      },
      advocate_engaged: {
        label: 'Taxpayer Advocate Engaged',
        transitions: {
          'tax_identity.taxpayer_advocate': 'advocate_engaged',
          'general.compliment': 'resolved',
          _timeout: { state: 'resolved', afterMinutes: 14400 }
        }
      },
      escalated: {
        label: 'Escalated',
        transitions: {
          'tax_identity.taxpayer_advocate': 'advocate_engaged',
          'general.compliment': 'resolved',
          _timeout: { state: 'resolved', afterMinutes: 4320 }
        }
      },
      resolved: { label: 'Resolved', transitions: {} },
      abandoned: { label: 'Abandoned', transitions: {} }
    }
  },

  foreign_compliance: {
    label: 'Foreign Account Compliance',
    description: 'Citizen or non-citizen coming into compliance with foreign account reporting',
    triggerIntents: ['foreign_tax.fbar_reporting', 'foreign_tax.fatca_compliance'],
    initialState: 'initiated',
    terminalStates: ['compliant', 'abandoned'],
    states: {
      initiated: {
        label: 'Compliance Process Started',
        transitions: {
          'foreign_tax.fbar_reporting': 'filing',
          'foreign_tax.fatca_compliance': 'filing',
          'foreign_tax.nonresident_tax': 'tax_return_review',
          _timeout: { state: 'filing', afterMinutes: 7200 }
        }
      },
      filing: {
        label: 'Filing Required Reports',
        transitions: {
          'foreign_tax.fbar_reporting': 'filing',
          'foreign_tax.fatca_compliance': 'filing',
          'tax_compliance.penalty_abatement': 'penalty_review',
          'general.compliment': 'compliant',
          _timeout: { state: 'compliant', afterMinutes: 14400 }
        }
      },
      tax_return_review: {
        label: 'Tax Return Under Review',
        transitions: {
          'foreign_tax.nonresident_tax': 'tax_return_review',
          'foreign_tax.treaty_benefits': 'treaty_claim',
          'foreign_tax.foreign_earned_income': 'tax_return_review',
          'general.compliment': 'compliant',
          _timeout: { state: 'compliant', afterMinutes: 10080 }
        }
      },
      treaty_claim: {
        label: 'Treaty Benefit Claim',
        transitions: {
          'foreign_tax.treaty_benefits': 'treaty_claim',
          'general.compliment': 'compliant',
          _timeout: { state: 'compliant', afterMinutes: 7200 }
        }
      },
      penalty_review: {
        label: 'Penalty Review for Late Filing',
        transitions: {
          'tax_compliance.penalty_abatement': 'penalty_review',
          'general.compliment': 'compliant',
          _timeout: { state: 'compliant', afterMinutes: 7200 }
        }
      },
      compliant: { label: 'Compliant', transitions: {} },
      abandoned: { label: 'Abandoned', transitions: {} }
    }
  }
};

function getJourneyDefinition(type) {
  return JOURNEY_DEFINITIONS[type] || null;
}

function findMatchingJourneys(intentKey) {
  const matches = [];
  for (const [type, def] of Object.entries(JOURNEY_DEFINITIONS)) {
    if (def.triggerIntents.includes(intentKey)) {
      matches.push({ type, definition: def });
    }
  }
  return matches;
}

module.exports = { JOURNEY_DEFINITIONS, getJourneyDefinition, findMatchingJourneys };
