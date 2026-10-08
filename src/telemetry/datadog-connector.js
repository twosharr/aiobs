import { v4 as uuidv4 } from 'uuid';

export class DatadogConnector {
  constructor(apiKey = 'simulated_dd_api_key_env') {
    this.apiKey = apiKey;
    this.recentMetrics = [];
    this.recentSpans = [];
    this.recentEvents = [];
  }

  /**
   * Export Data Observability metric points to Datadog
   */
  emitMetrics(metricsList) {
    const timestamp = Math.floor(Date.now() / 1000);
    const series = metricsList.map(m => ({
      metric: m.metric,
      points: [[timestamp, m.value]],
      type: m.type || 'gauge',
      tags: [
        'env:production',
        'platform:data_observability',
        `pipeline:${m.pipeline || 'default'}`,
        `layer:${m.layer || 'bronze'}`,
        ...(m.tags || [])
      ]
    }));

    this.recentMetrics.unshift(...series);
    if (this.recentMetrics.length > 100) this.recentMetrics.length = 100;

    return {
      status: 'success',
      datapointsExported: series.length,
      sample: series[0]
    };
  }

  /**
   * Emit OpenTelemetry / Datadog APM trace span for pipeline step
   */
  emitSpan(nodeId, durationMs, status, tags = {}) {
    const span = {
      trace_id: uuidv4().replace(/-/g, '').substring(0, 16),
      span_id: uuidv4().replace(/-/g, '').substring(0, 16),
      service: 'data-platform-engine',
      resource: nodeId,
      name: `pipeline.step.${nodeId}`,
      start: Date.now() - durationMs,
      duration: durationMs,
      error: status === 'FAILED' ? 1 : 0,
      meta: {
        'data.node_id': nodeId,
        'data.status': status,
        'data.quality_tier': tags.layer || 'unknown',
        'data.quality_score': String(tags.qualityScore || 100),
        'data.row_count': String(tags.rowCount || 0),
        'env': 'production'
      }
    };

    this.recentSpans.unshift(span);
    if (this.recentSpans.length > 50) this.recentSpans.length = 50;
    return span;
  }

  /**
   * Post Datadog Event / Alert Monitor
   */
  emitEvent(title, text, alertType = 'warning', tags = []) {
    const event = {
      title,
      text,
      alert_type: alertType, // 'info', 'warning', 'error', 'success'
      date_happened: Math.floor(Date.now() / 1000),
      priority: alertType === 'error' ? 'urgent' : 'normal',
      tags: ['source:agentic_observability', ...tags]
    };

    this.recentEvents.unshift(event);
    if (this.recentEvents.length > 50) this.recentEvents.length = 50;
    return event;
  }

  getDashboardSnapshot() {
    return {
      connectorStatus: 'CONNECTED (Telemetry Active)',
      metricsBufferCount: this.recentMetrics.length,
      spansBufferCount: this.recentSpans.length,
      eventsBufferCount: this.recentEvents.length,
      latestMetrics: this.recentMetrics.slice(0, 8),
      latestSpans: this.recentSpans.slice(0, 5),
      latestEvents: this.recentEvents.slice(0, 5)
    };
  }
}

export const datadogConnector = new DatadogConnector();
