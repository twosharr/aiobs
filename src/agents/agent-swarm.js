import { datadogConnector } from '../telemetry/datadog-connector.js';
import { dynatraceConnector } from '../telemetry/dynatrace-connector.js';
import { lineageTracker } from '../dag-engine/lineage-tracker.js';
import { lakehouse } from '../data-lake/lakehouse.js';
import { schemaRegistry } from '../schema-engine/schema-registry.js';

export class AgentSwarm {
  constructor() {
    this.logs = [];
    this.activeIncidents = [];
    this.remediationHistory = [];
  }

  logAgentAction(agentName, role, action, details, status = 'SUCCESS') {
    const entry = {
      id: `act_${Date.now()}_${Math.floor(Math.random() * 1000)}`,
      timestamp: new Date().toISOString(),
      agent: agentName,
      role,
      action,
      details,
      status
    };
    this.logs.unshift(entry);
    if (this.logs.length > 100) this.logs.pop();
    return entry;
  }

  /**
   * AGENT 1: Sentinel (Data Quality & Anomaly Detection Agent)
   */
  async runSentinel(pipelineResult, activeChaos) {
    const anomalies = [];

    // Evaluate CRM results
    if (pipelineResult.crm.drift.hasDrift) {
      const isBreaking = pipelineResult.crm.drift.isBreaking;
      anomalies.push({
        source: 'crm_customers',
        type: 'SCHEMA_DRIFT',
        severity: isBreaking ? 'CRITICAL' : 'WARNING',
        message: isBreaking ? 'Breaking schema drift: missing required columns or type mismatch' : 'Soft schema drift: added columns detected'
      });
    }

    if (pipelineResult.crm.validation.score < 85) {
      anomalies.push({
        source: 'crm_customers',
        type: 'QUALITY_DEGRADATION',
        severity: 'HIGH',
        score: pipelineResult.crm.validation.score,
        badCount: pipelineResult.crm.validation.badCount,
        message: `Data quality score dropped to ${pipelineResult.crm.validation.score}% (${pipelineResult.crm.validation.badCount} records failed contract)`
      });
    }

    // Evaluate Txns results
    if (pipelineResult.txns.drift.hasDrift) {
      anomalies.push({
        source: 'payment_gateway',
        type: 'SCHEMA_DRIFT',
        severity: pipelineResult.txns.drift.isBreaking ? 'CRITICAL' : 'WARNING',
        message: 'Schema drift in payment gateway: unexpected payload structure or currency symbol in numeric amount'
      });
    }

    if (pipelineResult.txns.validation.score < 80) {
      anomalies.push({
        source: 'payment_gateway',
        type: 'QUALITY_DEGRADATION',
        severity: 'CRITICAL',
        score: pipelineResult.txns.validation.score,
        badCount: pipelineResult.txns.validation.badCount,
        message: `Payment gateway data quality collapsed to ${pipelineResult.txns.validation.score}% due to null surge or duplicate transaction keys`
      });
    }

    if (activeChaos === 'COST_SPIKE') {
      anomalies.push({
        source: 'silver_clean_transactions',
        type: 'COST_ANOMALY',
        severity: 'HIGH',
        cost: 4.85,
        message: 'Snowflake query cost jumped 14x above baseline due to full warehouse scan'
      });
    }

    if (activeChaos === 'STALENESS_LAG') {
      anomalies.push({
        source: 'bronze_raw_transactions',
        type: 'FRESHNESS_SLA_BREACH',
        severity: 'HIGH',
        lagSeconds: 14400,
        message: 'Data freshness SLA breached: Kafka ingestion stream delayed by >4 hours'
      });
    }

    this.logAgentAction(
      'Sentinel',
      'Data Quality & Anomaly Detector',
      'Scanned pipeline telemetry batch',
      `Identified ${anomalies.length} anomaly conditions across ingested datasets.`
    );

    // Telemetry Sync to Datadog
    datadogConnector.emitMetrics([
      { metric: 'data.observability.crm.quality', value: pipelineResult.crm.validation.score, pipeline: 'crm_pipeline' },
      { metric: 'data.observability.txns.quality', value: pipelineResult.txns.validation.score, pipeline: 'payment_pipeline' },
      { metric: 'data.observability.anomalies.count', value: anomalies.length }
    ]);

    if (anomalies.length > 0) {
      datadogConnector.emitEvent(
        `Anomaly Detected: ${anomalies[0].type}`,
        anomalies.map(a => a.message).join(' | '),
        anomalies.some(a => a.severity === 'CRITICAL') ? 'error' : 'warning',
        [`anomaly_type:${anomalies[0].type}`]
      );
    }

    return anomalies;
  }

  /**
   * AGENT 2: Inspector (DAG Topology & Davis Root Cause Analysis Agent)
   */
  async runInspector(anomalies, pipelineResult) {
    if (!anomalies || anomalies.length === 0) return null;

    const critical = anomalies.find(a => a.severity === 'CRITICAL') || anomalies[0];
    const failedNode = critical.source === 'payment_gateway' ? 'silver_txns' : 'silver_customers';

    // Calculate blast radius via Lineage Tracker
    const impact = lineageTracker.getDownstreamImpact(failedNode);
    const upstream = lineageTracker.getUpstreamAncestors(failedNode);

    const rootCauseReport = {
      incidentId: `INC-${Date.now()}`,
      suspectedRootNode: critical.source,
      failedStage: failedNode,
      anomalyType: critical.type,
      upstreamSources: upstream.map(u => u.name),
      downstreamBlastRadius: impact.impactedNodes.map(i => i.name),
      evidence: critical.message,
      confidence: 0.94
    };

    this.logAgentAction(
      'Inspector',
      'Root Cause Analysis & Lineage Tracer',
      'Traversed DAG causal dependency graph',
      `Pinpointed root cause to '${critical.source}'. Blast radius threatens ${impact.blastRadiusCount} downstream assets: [${impact.impactedNodes.map(i => i.name).join(', ')}].`
    );

    // Sync to Dynatrace Davis AI Engine
    dynatraceConnector.reportDavisProblem(
      `Davis AI Root Cause: ${critical.type} at ${critical.source}`,
      critical.source,
      impact.impactedNodes,
      `Causal path traced from ${critical.source} -> ${failedNode} -> downstream consumers [${impact.impactedNodes.map(i => i.id).join(', ')}].`,
      critical.severity === 'CRITICAL' ? 'AVAILABILITY' : 'PERFORMANCE'
    );

    return rootCauseReport;
  }

  /**
   * AGENT 3: Healer (Autonomous Auto-Remediation & Quarantine Agent)
   */
  async runHealer(rootCauseReport, pipelineResult) {
    if (!rootCauseReport) return null;

    const remedies = [];

    // Case 1: Schema Drift with Amount String Cast
    if (rootCauseReport.anomalyType === 'SCHEMA_DRIFT') {
      remedies.push({
        action: 'DYNAMIC_TYPE_COERCION',
        description: 'Auto-applied Silver transformation rule: stripped currency symbols and parsed string amounts to IEEE floats.',
        target: 'silver_clean_transactions',
        recoveredRecords: pipelineResult.txns.validation.validCount
      });
    }

    // Case 2: Contract Violation / Bad Records
    if (pipelineResult.txns.validation.badRecords.length > 0 || pipelineResult.crm.validation.badRecords.length > 0) {
      const quarantinedCount = pipelineResult.txns.validation.badRecords.length + pipelineResult.crm.validation.badRecords.length;
      remedies.push({
        action: 'CIRCUIT_BREAKER_QUARANTINE',
        description: `Quarantined ${quarantinedCount} poisoned records into Dead Letter Queue (DLQ) to protect downstream Executive Dashboard from corrupted data.`,
        target: 'Lakehouse Quarantine Storage',
        quarantinedCount
      });
    }

    // Case 3: Freshness / Latency Auto-Trigger
    if (rootCauseReport.anomalyType === 'FRESHNESS_SLA_BREACH') {
      remedies.push({
        action: 'MICRO_BATCH_BACKFILL_TRIGGER',
        description: 'Dispatched autonomous micro-batch ingestion task to catch up lagging Kafka offsets.',
        target: 'bronze_raw_transactions'
      });
    }

    // Resolve Dynatrace Davis Problem
    const openProblems = dynatraceConnector.davisProblems.filter(p => p.status === 'OPEN');
    if (openProblems.length > 0) {
      dynatraceConnector.resolveDavisProblem(openProblems[0].problemId, 'Autonomous Healer executed quarantine and dynamic schema casting playbook.');
    }

    this.logAgentAction(
      'Healer',
      'Autonomous Auto-Remediation Engine',
      'Executed autonomous healing playbook',
      remedies.map(r => r.description).join(' ')
    );

    this.remediationHistory.unshift({
      timestamp: new Date().toISOString(),
      incidentId: rootCauseReport.incidentId,
      remedies
    });

    return remedies;
  }

  /**
   * AGENT 4: FinOps (Cloud Cost & SLA Optimizer)
   */
  async runFinOps(activeChaos) {
    let report = null;
    if (activeChaos === 'COST_SPIKE') {
      report = {
        status: 'ANOMALY_FLAGGED',
        queryId: 'SNOWFLAKE_QRY_998124',
        pipeline: 'silver_clean_transactions',
        estimatedCost: '$4.85 / batch (normal: $0.35)',
        costIncreaseFactor: '13.8x',
        recommendation: 'Agent FinOps detected missing clustering key on `transaction_id`. Generated auto-partitioning optimization PR #42.'
      };

      this.logAgentAction(
        'FinOps',
        'Data Cloud Cost Optimizer',
        'Detected Snowflake query credit surge',
        report.recommendation
      );
    } else {
      report = {
        status: 'WITHIN_BUDGET',
        hourlyBurnRate: '$1.42',
        budgetCap: '$15.00/hr',
        recommendation: 'Resource allocation optimal.'
      };
    }
    return report;
  }

  /**
   * AGENT 5: Copilot (Natural Language Data Platform Assistant)
   */
  async askCopilot(query, currentContext) {
    const qLower = query.toLowerCase();
    let answer = '';
    let category = 'GENERAL';

    if (qLower.includes('why') && (qLower.includes('fail') || qLower.includes('broken') || qLower.includes('degraded'))) {
      category = 'INCIDENT_EXPLANATION';
      const lastIncident = this.logs.find(l => l.agent === 'Inspector') || null;
      if (lastIncident) {
        answer = `🔎 **Investigation Summary**: The pipeline degraded because the upstream source sent corrupted/drifted data.\n\n` +
          `• **Root Cause**: ${lastIncident.details}\n` +
          `• **Impact**: Downstream tables 'gold_financial_reconciliation' and 'Executive Revenue Dashboard' were protected because Healer quarantined bad rows.\n` +
          `• **Dynatrace & Datadog Status**: Davis AI flagged the causal root and Datadog recorded the quality metric drop.`;
      } else {
        answer = `All current pipeline runs are **HEALTHY** (Data Quality Score > 98%). No failure events currently active.`;
      }
    } else if (qLower.includes('quarantine') || qLower.includes('bad data') || qLower.includes('dlq')) {
      category = 'DATA_LAKE_QUARANTINE';
      const qOverview = lakehouse.getOverview().quarantine;
      answer = `📦 **Quarantine (DLQ) Status**:\n\n` +
        `• Total Quarantined Batches: ${qOverview.totalFiles}\n` +
        `• Policy: Rejected records violating Data Contracts (e.g. null emails, duplicate transactions) are isolated to prevent bad reports.\n` +
        `• Storage location: \`lakehouse/quarantine/\`.`;
    } else if (qLower.includes('schema') || qLower.includes('drift')) {
      category = 'SCHEMA_MANAGEMENT';
      const lastDrift = schemaRegistry.driftEvents[0];
      if (lastDrift) {
        answer = `📐 **Schema Evolution Status**:\n\n` +
          `• Last Drift Source: ${lastDrift.sourceName}\n` +
          `• Breaking Change: ${lastDrift.isBreaking ? 'YES (Handled by Healer Agent)' : 'NO'}\n` +
          `• Added Columns: ${JSON.stringify(lastDrift.diff.addedColumns)}\n` +
          `• Type Mismatches: ${JSON.stringify(lastDrift.diff.typeMismatches)}`;
      } else {
        answer = `Schemas are aligned with registered baselines. No unhandled drifts.`;
      }
    } else if (qLower.includes('dynatrace') || qLower.includes('datadog')) {
      category = 'TELEMETRY_STATUS';
      const ddSnap = datadogConnector.getDashboardSnapshot();
      const dtSnap = dynatraceConnector.getDashboardSnapshot();
      answer = `📡 **Dual-Telemetry Exporters Active**:\n\n` +
        `• **Datadog**: ${ddSnap.metricsBufferCount} metric datapoints, ${ddSnap.spansBufferCount} OTel spans emitted.\n` +
        `• **Dynatrace**: ${dtSnap.smartscapeEntityCount} Smartscape entities monitored, Davis AI causal analysis active.`;
    } else {
      category = 'GENERAL_COPILOT';
      answer = `Hello! I am your **Autonomous Data Platform Observability Copilot**.\n\n` +
        `You can ask me:\n` +
        `1. *"Why did the pipeline fail last run?"*\n` +
        `2. *"Show me the quarantine and DLQ status"*\n` +
        `3. *"Did any schema drift occur?"*\n` +
        `4. *"What is the status of Dynatrace and Datadog telemetry?"*`;
    }

    this.logAgentAction('Copilot', 'Natural Language Assistant', 'Answered engineer query', `Question: "${query}"`);
    return { query, answer, category, timestamp: new Date().toISOString() };
  }
}

export const agentSwarm = new AgentSwarm();
