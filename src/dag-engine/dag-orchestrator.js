import { lakehouse } from '../data-lake/lakehouse.js';
import { dataGenerator } from '../data-lake/data-generator.js';
import { schemaRegistry } from '../schema-engine/schema-registry.js';
import { dataContractsValidator } from '../schema-engine/data-contracts.js';

export class DAGOrchestrator {
  constructor() {
    this.dagDefinition = {
      nodes: [
        { id: 'src_crm', name: 'CRM Postgres (Customers)', layer: 'source', system: 'PostgreSQL' },
        { id: 'src_kafka_txns', name: 'Payment Gateway (Kafka)', layer: 'source', system: 'Apache Kafka' },
        { id: 'src_rates_api', name: 'FX Rates (Public API)', layer: 'source', system: 'REST API' },

        { id: 'bronze_customers', name: 'bronze_raw_customers', layer: 'bronze', system: 'S3 / Lakehouse' },
        { id: 'bronze_txns', name: 'bronze_raw_transactions', layer: 'bronze', system: 'S3 / Lakehouse' },

        { id: 'silver_customers', name: 'silver_clean_customers', layer: 'silver', system: 'dbt / Snowflake' },
        { id: 'silver_txns', name: 'silver_clean_transactions', layer: 'silver', system: 'dbt / Snowflake' },
        { id: 'silver_fx', name: 'silver_normalized_fx', layer: 'silver', system: 'dbt / Snowflake' },

        { id: 'gold_customer360', name: 'gold_customer_360', layer: 'gold', system: 'Snowflake Mart' },
        { id: 'gold_recon', name: 'gold_financial_reconciliation', layer: 'gold', system: 'Snowflake Mart' },

        { id: 'bi_revenue', name: 'Executive Revenue Dashboard', layer: 'consumption', system: 'Tableau / PowerBI' },
        { id: 'ml_fraud', name: 'Real-time Fraud ML Model', layer: 'consumption', system: 'Databricks MLflow' }
      ],
      edges: [
        { from: 'src_crm', to: 'bronze_customers' },
        { from: 'src_kafka_txns', to: 'bronze_txns' },
        { from: 'src_rates_api', to: 'silver_fx' },

        { from: 'bronze_customers', to: 'silver_customers' },
        { from: 'bronze_txns', to: 'silver_txns' },

        { from: 'silver_customers', to: 'gold_customer360' },
        { from: 'silver_txns', to: 'gold_customer360' },
        { from: 'silver_txns', to: 'gold_recon' },
        { from: 'silver_fx', to: 'gold_recon' },

        { from: 'gold_customer360', to: 'ml_fraud' },
        { from: 'gold_recon', to: 'bi_revenue' }
      ]
    };

    // Live execution state for each node
    this.nodeStates = {};
    this.resetNodeStates();
  }

  resetNodeStates() {
    this.dagDefinition.nodes.forEach(n => {
      this.nodeStates[n.id] = {
        id: n.id,
        name: n.name,
        layer: n.layer,
        status: 'HEALTHY', // HEALTHY, DEGRADED, FAILED, RUNNING
        lastRunAt: new Date().toISOString(),
        durationMs: Math.floor(Math.random() * 800) + 200,
        rowCount: Math.floor(Math.random() * 1500) + 500,
        qualityScore: 98.5,
        freshnessSeconds: 120,
        costEstimateUsd: parseFloat((Math.random() * 0.45 + 0.05).toFixed(3)),
        activeAnomalies: []
      };
    });
  }

  getDAGState() {
    return {
      nodes: this.dagDefinition.nodes.map(n => ({
        ...n,
        telemetry: this.nodeStates[n.id]
      })),
      edges: this.dagDefinition.edges
    };
  }

  /**
   * Run an end-to-end pipeline execution with active telemetry
   */
  async executePipelineRun(chaosOverride = null) {
    const runId = `run_${Date.now()}`;
    const runLogs = [];

    // Step 1: Ingest CRM Customers
    const crmBatch = dataGenerator.generateSyntheticBatch('crm_customers', 20);
    const crmFingerprint = schemaRegistry.fingerprint(crmBatch.records);
    const crmDrift = schemaRegistry.evaluateDrift('crm_customers', crmFingerprint);
    const crmBronze = lakehouse.writeBronze('crm_customers', crmBatch.batchId, crmBatch.records, { drift: crmDrift });

    // Step 2: Ingest Payment Gateway Transactions
    const txnBatch = dataGenerator.generateSyntheticBatch('payment_gateway', 30);
    const txnFingerprint = schemaRegistry.fingerprint(txnBatch.records);
    const txnDrift = schemaRegistry.evaluateDrift('payment_gateway', txnFingerprint);
    const txnBronze = lakehouse.writeBronze('payment_gateway', txnBatch.batchId, txnBatch.records, { drift: txnDrift });

    // Step 3: Validate and Cleanse (Silver)
    const crmValidation = dataContractsValidator.validate('crm_customers', crmBatch.records);
    const txnValidation = dataContractsValidator.validate('payment_gateway', txnBatch.records);

    // Save Silver and Quarantine
    if (crmValidation.badRecords.length > 0) {
      lakehouse.writeQuarantine('crm_customers', crmBatch.batchId, crmValidation.badRecords, 'Data Contract Violation');
    }
    const crmSilver = lakehouse.writeSilver('silver_customers', crmBatch.batchId, crmValidation.validRecords.map(r => schemaRegistry.normalizeRecord('crm_customers', r)), { score: crmValidation.score });

    if (txnValidation.badRecords.length > 0) {
      lakehouse.writeQuarantine('payment_gateway', txnBatch.batchId, txnValidation.badRecords, 'Data Contract Violation');
    }
    const txnSilver = lakehouse.writeSilver('silver_transactions', txnBatch.batchId, txnValidation.validRecords.map(r => schemaRegistry.normalizeRecord('payment_gateway', r)), { score: txnValidation.score });

    // Step 4: Gold Aggregations
    const goldRecon = lakehouse.writeGold('gold_financial_recon', txnBatch.batchId, [
      { metric: 'total_clean_transactions', value: txnValidation.validRecords.length },
      { metric: 'quarantined_count', value: txnValidation.badRecords.length },
      { metric: 'data_quality_pct', value: txnValidation.score }
    ]);

    // Update Node States based on execution
    this.nodeStates['bronze_customers'].rowCount = crmBatch.records.length;
    this.nodeStates['bronze_customers'].qualityScore = crmDrift.isBreaking ? 60 : 100;
    this.nodeStates['bronze_customers'].status = crmDrift.isBreaking ? 'DEGRADED' : 'HEALTHY';

    this.nodeStates['bronze_txns'].rowCount = txnBatch.records.length;
    this.nodeStates['bronze_txns'].qualityScore = txnDrift.isBreaking ? 50 : 100;
    this.nodeStates['bronze_txns'].status = txnDrift.isBreaking ? 'DEGRADED' : 'HEALTHY';

    this.nodeStates['silver_customers'].qualityScore = crmValidation.score;
    this.nodeStates['silver_customers'].status = crmValidation.score < 80 ? 'DEGRADED' : 'HEALTHY';

    this.nodeStates['silver_txns'].qualityScore = txnValidation.score;
    this.nodeStates['silver_txns'].status = txnValidation.score < 75 ? (txnValidation.score < 50 ? 'FAILED' : 'DEGRADED') : 'HEALTHY';

    // Downstream impact on Gold and Consumption
    const isTxnCompromised = this.nodeStates['silver_txns'].status !== 'HEALTHY';
    this.nodeStates['gold_recon'].status = isTxnCompromised ? 'DEGRADED' : 'HEALTHY';
    this.nodeStates['bi_revenue'].status = isTxnCompromised ? 'FAILED' : 'HEALTHY';

    if (dataGenerator.chaosConfig.activeScenario === 'COST_SPIKE') {
      this.nodeStates['silver_txns'].costEstimateUsd = 4.85; // 10x cost spike
    } else {
      this.nodeStates['silver_txns'].costEstimateUsd = 0.35;
    }

    if (dataGenerator.chaosConfig.activeScenario === 'STALENESS_LAG') {
      this.nodeStates['bronze_txns'].freshnessSeconds = 14400; // 4 hours lag
      this.nodeStates['bronze_txns'].status = 'DEGRADED';
    }

    return {
      runId,
      timestamp: new Date().toISOString(),
      crm: { batchId: crmBatch.batchId, drift: crmDrift, validation: crmValidation },
      txns: { batchId: txnBatch.batchId, drift: txnDrift, validation: txnValidation },
      nodeStates: this.nodeStates
    };
  }
}

export const dagOrchestrator = new DAGOrchestrator();
