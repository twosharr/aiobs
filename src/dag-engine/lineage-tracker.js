export class LineageTracker {
  constructor(dagDefinition) {
    this.dag = dagDefinition;
    this.forwardMap = {};
    this.reverseMap = {};
    this.buildGraph();
  }

  buildGraph() {
    this.dag.nodes.forEach(n => {
      this.forwardMap[n.id] = [];
      this.reverseMap[n.id] = [];
    });

    this.dag.edges.forEach(e => {
      this.forwardMap[e.from].push(e.to);
      this.reverseMap[e.to].push(e.from);
    });
  }

  /**
   * Downstream Blast Radius: Which assets are affected if nodeId breaks?
   */
  getDownstreamImpact(nodeId) {
    const visited = new Set();
    const queue = [...(this.forwardMap[nodeId] || [])];

    while (queue.length > 0) {
      const current = queue.shift();
      if (!visited.has(current)) {
        visited.add(current);
        (this.forwardMap[current] || []).forEach(next => queue.push(next));
      }
    }

    const impactedNodes = Array.from(visited).map(id => {
      const n = this.dag.nodes.find(node => node.id === id);
      return { id, name: n?.name || id, layer: n?.layer || 'unknown' };
    });

    return {
      sourceNode: nodeId,
      blastRadiusCount: impactedNodes.length,
      impactedNodes
    };
  }

  /**
   * Upstream Root Cause Ancestors: Where did data come from?
   */
  getUpstreamAncestors(nodeId) {
    const visited = new Set();
    const queue = [...(this.reverseMap[nodeId] || [])];

    while (queue.length > 0) {
      const current = queue.shift();
      if (!visited.has(current)) {
        visited.add(current);
        (this.reverseMap[current] || []).forEach(prev => queue.push(prev));
      }
    }

    return Array.from(visited).map(id => {
      const n = this.dag.nodes.find(node => node.id === id);
      return { id, name: n?.name || id, layer: n?.layer || 'unknown' };
    });
  }
}

export const lineageTracker = new LineageTracker(new (await import('./dag-orchestrator.js')).DAGOrchestrator().dagDefinition);
