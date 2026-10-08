import express from 'express';
import cors from 'cors';
import path from 'path';
import { fileURLToPath } from 'url';
import { dagOrchestrator } from './dag-engine/dag-orchestrator.js';
import { lakehouse } from './data-lake/lakehouse.js';
import { dataGenerator } from './data-lake/data-generator.js';
import { schemaRegistry } from './schema-engine/schema-registry.js';
import { datadogConnector } from './telemetry/datadog-connector.js';
import { dynatraceConnector } from './telemetry/dynatrace-connector.js';
import { agentSwarm } from './agents/agent-swarm.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, '../public')));

/**
 * 1. DAG State & Telemetry
 */
app.get('/api/dag', (req, res) => {
  res.json(dagOrchestrator.getDAGState());
});

/**
 * 2. Lakehouse Tiers Overview
 */
app.get('/api/lakehouse', (req, res) => {
  res.json({
    overview: lakehouse.getOverview(),
    chaosState: dataGenerator.chaosConfig
  });
});

/**
 * 3. Schema Registry & Drift Events
 */
app.get('/api/schema', (req, res) => {
  res.json({
    baselines: schemaRegistry.baselines,
    driftEvents: schemaRegistry.driftEvents
  });
});

/**
 * 4. Datadog Telemetry
 */
app.get('/api/telemetry/datadog', (req, res) => {
  res.json(datadogConnector.getDashboardSnapshot());
});

/**
 * 5. Dynatrace Telemetry & Davis AI
 */
app.get('/api/telemetry/dynatrace', (req, res) => {
  res.json(dynatraceConnector.getDashboardSnapshot());
});

/**
 * 6. Agent Swarm Action Logs
 */
app.get('/api/agents/logs', (req, res) => {
  res.json({
    logs: agentSwarm.logs,
    remediations: agentSwarm.remediationHistory
  });
});

/**
 * 7. Execute Pipeline Run + Multi-Agent Observability Loop
 */
app.post('/api/pipeline/run', async (req, res) => {
  try {
    const activeChaos = dataGenerator.chaosConfig.activeScenario;
    const runResult = await dagOrchestrator.executePipelineRun(activeChaos);

    // Agent Loop Trigger:
    // Step 1: Sentinel checks for anomalies
    const anomalies = await agentSwarm.runSentinel(runResult, activeChaos);

    // Step 2: Inspector traces root cause & blast radius
    const rootCause = await agentSwarm.runInspector(anomalies, runResult);

    // Step 3: Healer executes autonomous remediation
    const remedies = await agentSwarm.runHealer(rootCause, runResult);

    // Step 4: FinOps checks cloud cost & SLA
    const finOps = await agentSwarm.runFinOps(activeChaos);

    res.json({
      success: true,
      pipelineRun: runResult,
      agenticAnalysis: {
        anomaliesCount: anomalies.length,
        anomalies,
        rootCause,
        remedies,
        finOps
      }
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/**
 * 8. Chaos & Internet Data Injector
 */
app.post('/api/chaos/inject', async (req, res) => {
  const { scenario, severity } = req.body;
  if (!scenario || scenario === 'CLEAR') {
    const result = dataGenerator.clearChaos();
    dagOrchestrator.resetNodeStates();
    return res.json(result);
  }

  const result = dataGenerator.setChaos(scenario, severity || 0.6);
  res.json(result);
});

/**
 * 9. Live Public Internet Data Ingestion
 */
app.post('/api/ingest/internet', async (req, res) => {
  const { sourceType } = req.body;
  const result = await dataGenerator.fetchInternetSource(sourceType || 'users');
  const batch = lakehouse.writeBronze('internet_public_api', `inet_${Date.now()}`, result.records, { source: result.source });
  res.json({ success: true, batchId: batch.batchId, recordCount: result.count, source: result.source });
});

/**
 * 10. Copilot Natural Language Query
 */
app.post('/api/copilot/chat', async (req, res) => {
  const { message } = req.body;
  if (!message) return res.status(400).json({ error: 'Message required' });

  const response = await agentSwarm.askCopilot(message, {
    dagState: dagOrchestrator.getDAGState(),
    chaos: dataGenerator.chaosConfig
  });
  res.json(response);
});

// Start initial pipeline run on boot
setTimeout(async () => {
  await dagOrchestrator.executePipelineRun();
  console.log('Initial healthy pipeline execution completed.');
}, 500);

app.listen(PORT, () => {
  console.log(`Agentic Data Platform Observability running at http://localhost:${PORT}`);
});
