/**
 * Core Graph Data Structure
 * Stores the network topology and attributes in a framework-agnostic way.
 */
class Graph {
    constructor() {
        this.nodes = new Map(); // id -> { id, kind, ...attrs }
        this.edges = new Map(); // id -> { id, source, target, weight, ...attrs }
        this.adjacency = new Map(); // nodeId -> Set(edgeId)
    }

    /**
     * Add a node to the graph
     * @param {Object} node - Node object (must have id)
     */
    addNode(node) {
        if (!node.id) throw new Error("Node must have an id");
        if (!this.nodes.has(node.id)) {
            this.nodes.set(node.id, { ...node });
            this.adjacency.set(node.id, new Set());
        } else {
            // Merge attributes if node exists
            const existing = this.nodes.get(node.id);
            this.nodes.set(node.id, { ...existing, ...node });
        }
    }

    /**
     * Add an edge to the graph
     * @param {Object} edge - Edge object (must have source, target)
     */
    addEdge(edge) {
        const { source, target, id } = edge;
        if (!source || !target) throw new Error("Edge must have source and target");

        // Canonical ID if not provided
        const edgeId = id || Graph.getEdgeId(source, target);

        if (!this.edges.has(edgeId)) {
            this.edges.set(edgeId, { ...edge, id: edgeId });

            // Update adjacency
            if (this.adjacency.has(source)) this.adjacency.get(source).add(edgeId);
            if (this.adjacency.has(target)) this.adjacency.get(target).add(edgeId);
        } else {
            // Merge attributes
            const existing = this.edges.get(edgeId);
            this.edges.set(edgeId, { ...existing, ...edge });
        }
    }

    /**
     * Get canonical edge ID for undirected edge
     */
    static getEdgeId(u, v) {
        return [u, v].sort().join('|');
    }

    /**
     * Get all nodes as array
     */
    getNodes() {
        return Array.from(this.nodes.values());
    }

    /**
     * Get all edges as array
     */
    getEdges() {
        return Array.from(this.edges.values());
    }

    /**
     * Get neighbors of a node
     */
    getNeighbors(nodeId) {
        const edgeIds = this.adjacency.get(nodeId);
        if (!edgeIds) return [];

        const neighbors = new Set();
        edgeIds.forEach(edgeId => {
            const edge = this.edges.get(edgeId);
            if (edge.source === nodeId) neighbors.add(edge.target);
            else neighbors.add(edge.source);
        });
        return Array.from(neighbors);
    }

    /**
     * Clear the graph
     */
    clear() {
        this.nodes.clear();
        this.edges.clear();
        this.adjacency.clear();
    }

    /**
     * Get all members of a specific cluster (NH node)
     * @param {string} clusterId - The ID of the cluster (NH node)
     * @returns {Array<string>} - Array of node IDs that belong to this cluster
     */
    getClusterMembers(clusterId) {
        const members = [];
        const strClusterId = String(clusterId);

        this.nodes.forEach(node => {
            if (node.NH_ID && String(node.NH_ID) === strClusterId) {
                members.push(node.id);
            }
        });
        return members;
    }
}

// Export for module system if using CommonJS, or global if using vanilla script tags
if (typeof module !== 'undefined' && module.exports) {
    module.exports = Graph;
} else {
    window.Graph = Graph;
}
