/**
 * Application State Management
 * Centralizes the state of the application (graph, selection, UI state).
 */
class AppState {
    constructor() {
        this.graph = new Graph(); // The core graph model
        this.viewGraph = new Graph(); // The graph currently being visualized (subset/aggregated)

        this.expandedClusters = new Set(); // Set of expanded cluster IDs
        this.nodeColors = new Map(); // Map of NodeID -> Array<Color>
        this.highlightLayers = new Map(); // Map of LayerID -> { matches: Array, color: String }
        this.hiddenNodes = new Set(); // Set of hidden Node IDs
        this.currentNetwork = null; // Name of the currently loaded network

        // Selection State
        this.selectionMode = false;
        this.selectedNodes = new Set(); // Set of selected Node IDs

        this.eventBus = new EventTarget(); // Simple event bus
    }

    /**
     * Emit an event
     */
    emit(eventName, detail) {
        this.eventBus.dispatchEvent(new CustomEvent(eventName, { detail }));
    }

    /**
     * Subscribe to an event
     */
    on(eventName, callback) {
        this.eventBus.addEventListener(eventName, (e) => callback(e.detail));
    }

    /**
     * Set the core graph data
     */
    setGraphData(nodes, edges, networkName = null) {
        this.graph.clear();
        this.expandedClusters.clear(); // Reset expansion state
        this.hiddenNodes.clear(); // Reset hidden state
        this.currentNetwork = networkName;
        nodes.forEach(n => this.graph.addNode(n));
        edges.forEach(e => this.graph.addEdge(e));
        this.emit('graphUpdated', { nodes, edges, networkName });
    }

    /**
     * Update the view graph (currently visible nodes/edges)
     */
    setViewGraph(nodes, edges) {
        this.viewGraph.clear();
        nodes.forEach(n => this.viewGraph.addNode(n));
        edges.forEach(e => this.viewGraph.addEdge(e));
        // We don't necessarily need to emit here if D3Adapter is the one calling it,
        // but it's good practice for other listeners.
        // However, D3Adapter calls this inside updateVisualization, which is triggered by events.
        // Emitting here might cause loops if not careful.
        // For now, just update the data structure.
    }

    /**
     * Expand a cluster (NH node)
     */
    expandCluster(nhId) {
        if (this.expandedClusters.has(nhId)) return;
        this.expandedClusters.add(nhId);
        this.updateDerivedHighlights();
        this.emit('clusterExpanded', nhId);
    }

    /**
     * Collapse a cluster
     */
    collapseCluster(clusterId) {
        if (this.expandedClusters.has(clusterId)) {
            this.expandedClusters.delete(clusterId);
            this.updateDerivedHighlights();
            this.emit('clusterCollapsed', { clusterId });
        }
    }

    /**
     * Expand all clusters
     */
    expandAll() {
        let changed = false;
        this.graph.nodes.forEach(node => {
            if (node.NH_ID && !this.expandedClusters.has(node.NH_ID)) {
                this.expandedClusters.add(node.NH_ID);
                changed = true;
            }
        });
        if (changed) {
            this.updateDerivedHighlights();
            this.emit('graphUpdated');
        }
    }

    /**
     * Collapse all clusters
     */
    collapseAll() {
        if (this.expandedClusters.size > 0) {
            this.expandedClusters.clear();
            this.updateDerivedHighlights();
            this.emit('graphUpdated');
        }
    }

    /**
     * Add or update a highlight layer
     * @param {string} layerId - Unique ID for this layer (e.g. "species:9606")
     * @param {Array} matches - Array of {id, nh_id} objects
     * @param {string} color - Color for this layer
     */
    addHighlightLayer(layerId, matches, color) {
        this.highlightLayers.set(layerId, { matches, color });
        this.updateDerivedHighlights();
        this.emit('graphVisualsUpdated');
    }

    /**
     * Update the color of an existing highlight layer
     */
    updateHighlightLayerColor(layerId, color) {
        if (this.highlightLayers.has(layerId)) {
            const layer = this.highlightLayers.get(layerId);
            layer.color = color;
            this.updateDerivedHighlights();
            this.emit('graphVisualsUpdated');
        }
    }

    /**
     * Remove a highlight layer
     */
    removeHighlightLayer(layerId) {
        if (this.highlightLayers.has(layerId)) {
            this.highlightLayers.delete(layerId);
            this.updateDerivedHighlights();
            this.emit('graphVisualsUpdated');
        }
    }

    /**
     * Clear all highlight layers
     */
    clearHighlightLayers() {
        this.highlightLayers.clear();
        this.nodeColors.clear();
        this.emit('graphVisualsUpdated');
    }


    clearHighlights() {
        this.clearHighlightLayers();
    }

    updateDerivedHighlights() {
        this.nodeColors.clear();

        // Iterate over all active layers
        for (const [layerId, layer] of this.highlightLayers) {
            const { matches, color } = layer;

            matches.forEach(m => {
                let targetId = m.id;

                // Logic: If protein has a parent NH cluster AND that cluster is collapsed -> Highlight Cluster
                if (m.nh_id && !this.expandedClusters.has(m.nh_id)) {
                    targetId = m.nh_id;
                }

                // Add color to the target node's color list
                if (!this.nodeColors.has(targetId)) {
                    this.nodeColors.set(targetId, []);
                }

                const colors = this.nodeColors.get(targetId);
                // Avoid duplicate colors for the same node from the same layer? 
                // The user wants "split into n+1 slices" where n is number of species (layers).
                // So if multiple proteins from same species map to this node, we still only want ONE slice for that species.
                // So we check if this color is already in the list? 
                // Or better, we check if we already processed this layer for this targetId.
                // But `colors` is just an array of strings.
                // Simple heuristic: If the color is not already in the array, add it.
                // This assumes each layer has a unique color, or at least we want unique colors in the pie.
                if (!colors.includes(color)) {
                    colors.push(color);
                }
            });
        }
    }

    hideNodes(ids) {
        let changed = false;
        ids.forEach(id => {
            if (!this.hiddenNodes.has(id)) {
                this.hiddenNodes.add(id);
                changed = true;
            }
        });
        if (changed) this.emit('graphUpdated');
    }

    showNodes(ids) {
        let changed = false;
        ids.forEach(id => {
            if (this.hiddenNodes.has(id)) {
                this.hiddenNodes.delete(id);
                changed = true;
            }
        });
        if (changed) this.emit('graphUpdated');
    }

    // Selection Mode Methods

    setSelectionMode(enabled) {
        if (this.selectionMode === enabled) return;
        this.selectionMode = enabled;

        if (enabled) {
            // Auto-select highlighted nodes
            this.autoSelectHighlightedNodes();
        } else {
            // Clear selection when exiting mode? 
            // User didn't specify, but usually good practice.
            // Or keep it? The requirement says "revert to original appearance".
            // Let's clear it to be safe and avoid confusion.
            this.clearSelection();
        }

        this.emit('selectionModeChanged', enabled);
    }

    autoSelectHighlightedNodes() {
        // Find all nodes that have highlights (nodeColors has entry)
        const toSelect = [];
        this.nodeColors.forEach((colors, nodeId) => {
            if (colors && colors.length > 0) {
                toSelect.push(nodeId);
            }
        });
        this.selectNodes(toSelect);
    }

    toggleNodeSelection(nodeId) {
        if (this.selectedNodes.has(nodeId)) {
            this.selectedNodes.delete(nodeId);
        } else {
            this.selectedNodes.add(nodeId);
        }
        this.emit('selectionUpdated', Array.from(this.selectedNodes));
    }

    selectNodes(nodeIds) {
        let changed = false;
        nodeIds.forEach(id => {
            if (!this.selectedNodes.has(id)) {
                this.selectedNodes.add(id);
                changed = true;
            }
        });
        if (changed) {
            this.emit('selectionUpdated', Array.from(this.selectedNodes));
        }
    }

    clearSelection() {
        if (this.selectedNodes.size > 0) {
            this.selectedNodes.clear();
            this.emit('selectionUpdated', []);
        }
    }
}

// Export
if (typeof module !== 'undefined' && module.exports) {
    module.exports = AppState;
} else {
    window.AppState = AppState;
}
