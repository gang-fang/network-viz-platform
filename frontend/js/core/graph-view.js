/**
 * GraphView
 * Responsible for computing the "Visible Graph" based on the Core Graph and Expansion State.
 * Implements the Drill-down/Roll-up logic.
 */
class GraphView {
    /**
     * Compute the visible nodes and edges.
     * @param {Graph} coreGraph - The full graph of proteins.
     * @param {Set} expandedClusters - Set of expanded NH IDs.
     * @returns {Object} { nodes: [], edges: [] }
     */
    static compute(coreGraph, expandedClusters) {
        const visibleNodes = new Map();
        const visibleEdges = new Map();

        // Helper to find the representative ID for a node
        const getRepId = (nodeId) => {
            const node = coreGraph.nodes.get(nodeId);
            if (!node) return String(nodeId); // Should not happen

            const nhId = node.NH_ID;
            // If belongs to a cluster AND that cluster is NOT expanded -> Cluster is Rep
            if (nhId && !expandedClusters.has(nhId)) {
                return String(nhId);
            }
            // Otherwise -> Node itself is Rep
            return String(nodeId);
        };

        // 1. Determine Visible Nodes
        coreGraph.nodes.forEach(node => {
            const repId = getRepId(node.id);

            if (!visibleNodes.has(repId)) {
                if (repId === String(node.id)) {
                    // It's the protein itself
                    visibleNodes.set(repId, { ...node, id: String(node.id), _isCluster: false });
                } else {
                    // It's a Cluster (NH) node
                    visibleNodes.set(repId, {
                        id: repId,
                        kind: 'nh',
                        label: repId,
                        size: node.NH_Size || 10, // Use attribute from protein
                        _isCluster: true,
                        // Aggregate other attributes if needed
                    });
                }
            }
        });

        // 2. Aggregate Edges
        coreGraph.edges.forEach(edge => {
            const uRep = getRepId(edge.source);
            const vRep = getRepId(edge.target);

            if (uRep !== vRep) {
                // Canonical edge ID for the view
                const viewEdgeId = Graph.getEdgeId(uRep, vRep);

                if (!visibleEdges.has(viewEdgeId)) {
                    visibleEdges.set(viewEdgeId, {
                        id: viewEdgeId,
                        source: uRep,
                        target: vRep,
                        weight: edge.weight,
                        count: 1,
                        _isAggregated: (uRep !== String(edge.source) || vRep !== String(edge.target))
                    });
                } else {
                    // Accumulate weight
                    const existing = visibleEdges.get(viewEdgeId);
                    existing.weight += edge.weight;
                    existing.count += 1;
                }
            }
        });

        // 3. Normalize Weights
        // Formula: NormalizedWeight = Sum(weights) / (Size(u) * Size(v))
        visibleEdges.forEach(edge => {
            const uNode = visibleNodes.get(edge.source);
            const vNode = visibleNodes.get(edge.target);

            if (uNode && vNode) {
                const sizeU = uNode._isCluster ? (uNode.size || 1) : 1;
                const sizeV = vNode._isCluster ? (vNode.size || 1) : 1;

                // Store raw sum for debugging/tooltip
                edge._rawWeight = edge.weight;

                // Apply normalization
                edge.weight = edge.weight / (sizeU * sizeV);
            }
        });

        return {
            nodes: Array.from(visibleNodes.values()),
            edges: Array.from(visibleEdges.values())
        };
    }
}

// Export
if (typeof module !== 'undefined' && module.exports) {
    module.exports = GraphView;
} else {
    window.GraphView = GraphView;
}
