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
        this.hiddenNodes = new Set(); // Set of hidden core protein node IDs
        this.hiddenEdges = new Set(); // Set of hidden core edge IDs
        this.hiddenEdgeWeightRanges = []; // Compact threshold rules: [{ min, max }]
        this.currentNetwork = null; // Name of the currently loaded network
        this.editRevision = 0; // Incremented whenever edit operations change visible proteins
        this.topologyRevision = 0; // Incremented whenever visible graph topology needs recomputing

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
        this.hiddenEdges.clear(); // Reset hidden edge state
        this.hiddenEdgeWeightRanges = [];
        this.editRevision = 0;
        this.topologyRevision += 1;
        this.currentNetwork = networkName;
        nodes.forEach(n => this.graph.addNode(n));
        edges.forEach(e => this.graph.addEdge(e));
        this.clearSelection();
        this.emit('editUpdated', this.getEditStats());
        this.emit('graphUpdated', {
            nodes,
            edges,
            networkName,
            layoutReset: true,
            fitView: true,
            resetViewport: true,
            reason: 'load',
            preservePins: false,
            topologyRevision: this.topologyRevision
        });
    }

    /**
     * Update the view graph (currently visible nodes/edges)
     */
    setViewGraph(nodes, edges) {
        this.viewGraph.clear();
        nodes.forEach(n => this.viewGraph.addNode(n));
        edges.forEach(e => this.viewGraph.addEdge(e));
        this.pruneSelectionToViewGraph();
        const stats = this.getEditStats();
        this.emit('viewGraphUpdated', stats);
        this.emit('editUpdated', stats);
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
        this.topologyRevision += 1;
        this.updateDerivedHighlights();
        this.emit('clusterExpanded', {
            clusterId: nhId,
            topologyRevision: this.topologyRevision
        });
    }

    /**
     * Collapse a cluster
     */
    collapseCluster(clusterId) {
        if (this.expandedClusters.has(clusterId)) {
            this.expandedClusters.delete(clusterId);
            this.topologyRevision += 1;
            this.updateDerivedHighlights();
            this.emit('clusterCollapsed', {
                clusterId,
                topologyRevision: this.topologyRevision
            });
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
            this.topologyRevision += 1;
            this.updateDerivedHighlights();
            this.emit('graphUpdated', {
                layoutReset: true,
                fitView: true,
                preservePins: false,
                resetViewport: true,
                reason: 'expandAll',
                topologyRevision: this.topologyRevision
            });
        }
    }

    /**
     * Collapse all clusters
     */
    collapseAll() {
        if (this.expandedClusters.size > 0) {
            this.expandedClusters.clear();
            this.topologyRevision += 1;
            this.updateDerivedHighlights();
            this.emit('graphUpdated', {
                layoutReset: true,
                fitView: true,
                preservePins: false,
                resetViewport: true,
                reason: 'collapseAll',
                topologyRevision: this.topologyRevision
            });
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
                const matchId = String(m.id);
                const node = this.graph.nodes.get(matchId);
                if (!node || this.hiddenNodes.has(matchId)) return;

                let targetId = matchId;

                // Logic: If protein has a parent NH cluster AND that cluster is collapsed -> Highlight Cluster
                const nhId = node.NH_ID || m.nh_id;
                if (nhId && !this.expandedClusters.has(nhId)) {
                    targetId = String(nhId);
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
        return this.hideProteinIds(this.resolveProteinIds(ids), 'hideNodes');
    }

    showNodes(ids) {
        return this.showProteinIds(this.resolveProteinIds(ids), 'showNodes');
    }

    hideProteinIds(ids, reason = 'hideNodes') {
        let changed = false;
        ids.forEach(id => {
            const nodeId = String(id);
            if (this.graph.nodes.has(nodeId) && !this.hiddenNodes.has(nodeId)) {
                this.hiddenNodes.add(nodeId);
                changed = true;
            }
        });
        if (changed) this.applyEditChange(reason);
        return changed;
    }

    showProteinIds(ids, reason = 'showNodes') {
        let changed = false;
        ids.forEach(id => {
            const nodeId = String(id);
            if (this.hiddenNodes.has(nodeId)) {
                this.hiddenNodes.delete(nodeId);
                changed = true;
            }
        });
        if (changed) this.applyEditChange(reason);
        return changed;
    }

    showAllNodes() {
        if (this.hiddenNodes.size === 0) return false;
        this.hiddenNodes.clear();
        this.applyEditChange('showAllNodes');
        return true;
    }

    hideEdgeIds(ids, reason = 'hideEdges') {
        let changed = false;
        (ids || []).forEach(id => {
            const edgeId = this.normalizeEdgeId(id);
            if (this.graph.edges.has(edgeId) && !this.hiddenEdges.has(edgeId)) {
                this.hiddenEdges.add(edgeId);
                changed = true;
            }
        });
        if (changed) this.applyEditChange(reason);
        return changed;
    }

    showEdgeIds(ids, reason = 'showEdges', { preserveRanges = false } = {}) {
        let changed = false;
        (ids || []).forEach(id => {
            const edgeId = this.normalizeEdgeId(id);
            if (this.hiddenEdges.has(edgeId)) {
                this.hiddenEdges.delete(edgeId);
                changed = true;
            }
        });
        if (changed) {
            if (!preserveRanges) this.hiddenEdgeWeightRanges = [];
            this.applyEditChange(reason);
        }
        return changed;
    }

    normalizeEdgeId(id) {
        const raw = String(id).trim();
        const parts = raw.split('|');
        if (parts.length === 2 && parts[0].trim() && parts[1].trim()) {
            return Graph.getEdgeId(parts[0].trim(), parts[1].trim());
        }
        return raw;
    }

    validateEdgeThreshold(threshold) {
        const cutoff = Number(threshold);
        if (!Number.isFinite(cutoff) || cutoff <= 0 || cutoff >= 1) {
            throw new Error('SJI threshold must be greater than 0 and less than 1.');
        }
        return cutoff;
    }

    getEdgeIdsBelowWeight(threshold) {
        const cutoff = this.validateEdgeThreshold(threshold);

        const ids = [];
        this.graph.edges.forEach(edge => {
            const weight = Number(edge.weight);
            if (Number.isFinite(weight) && weight < cutoff) {
                ids.push(String(edge.id));
            }
        });
        return ids;
    }

    getHiddenEdgeIdsAboveWeight(threshold) {
        const cutoff = this.validateEdgeThreshold(threshold);

        const ids = [];
        this.graph.edges.forEach(edge => {
            const weight = Number(edge.weight);
            if (!Number.isFinite(weight) || weight <= cutoff) {
                return;
            }

            const edgeId = String(edge.id);
            if (this.hiddenEdges.has(edgeId) && this.isWeightHiddenByRanges(weight)) {
                ids.push(edgeId);
            }
        });
        return ids;
    }

    hideEdgesByWeightBelow(threshold) {
        const cutoff = this.validateEdgeThreshold(threshold);
        const edgeIds = this.getEdgeIdsBelowWeight(threshold);
        const before = this.hiddenEdges.size;
        this.addHiddenEdgeWeightRange(0, cutoff);
        this.hideEdgeIds(edgeIds, 'hideEdgesByWeight');
        return {
            matchedCount: edgeIds.length,
            changedCount: this.hiddenEdges.size - before
        };
    }

    showEdgesByWeightAbove(threshold) {
        const cutoff = this.validateEdgeThreshold(threshold);
        const edgeIds = this.getHiddenEdgeIdsAboveWeight(threshold);
        const before = this.hiddenEdges.size;
        this.removeHiddenEdgeWeightRange(cutoff, 1);
        this.showEdgeIds(edgeIds, 'showEdgesByWeightAbove', { preserveRanges: true });
        return {
            matchedCount: edgeIds.length,
            changedCount: before - this.hiddenEdges.size
        };
    }

    showAllEdges() {
        if (this.hiddenEdges.size === 0 && this.hiddenEdgeWeightRanges.length === 0) return false;
        this.hiddenEdges.clear();
        this.hiddenEdgeWeightRanges = [];
        this.applyEditChange('showAllEdges');
        return true;
    }

    showAllEdits() {
        if (this.hiddenNodes.size === 0 && this.hiddenEdges.size === 0 && this.hiddenEdgeWeightRanges.length === 0) return false;
        this.hiddenNodes.clear();
        this.hiddenEdges.clear();
        this.hiddenEdgeWeightRanges = [];
        this.applyEditChange('showAllEdits');
        return true;
    }

    addHiddenEdgeWeightRange(min, max) {
        const next = [...this.hiddenEdgeWeightRanges, { min, max }]
            .sort((a, b) => a.min - b.min || a.max - b.max);
        const merged = [];

        next.forEach(range => {
            if (merged.length === 0 || range.min > merged[merged.length - 1].max) {
                merged.push({ ...range });
                return;
            }
            merged[merged.length - 1].max = Math.max(merged[merged.length - 1].max, range.max);
        });

        this.hiddenEdgeWeightRanges = merged;
    }

    removeHiddenEdgeWeightRange(min, max) {
        const remaining = [];

        this.hiddenEdgeWeightRanges.forEach(range => {
            if (range.max <= min || range.min >= max) {
                remaining.push(range);
                return;
            }
            if (range.min < min) {
                remaining.push({ min: range.min, max: min });
            }
            if (range.max > max) {
                remaining.push({ min: max, max: range.max });
            }
        });

        this.hiddenEdgeWeightRanges = remaining;
    }

    isWeightHiddenByRanges(weight) {
        const numericWeight = Number(weight);
        return Number.isFinite(numericWeight)
            && this.hiddenEdgeWeightRanges.some(range => numericWeight >= range.min && numericWeight < range.max);
    }

    getEdgeIdsForHiddenWeightRanges() {
        const ids = [];
        this.graph.edges.forEach(edge => {
            if (this.isWeightHiddenByRanges(edge.weight)) {
                ids.push(String(edge.id));
            }
        });
        return ids;
    }

    setsEqual(left, right) {
        if (left.size !== right.size) return false;
        for (const item of left) {
            if (!right.has(item)) return false;
        }
        return true;
    }

    applyEditChange(reason) {
        this.editRevision += 1;
        this.topologyRevision += 1;
        this.pruneSelectionForHiddenProteins();
        this.updateDerivedHighlights();
        this.emit('editUpdated', this.getEditStats());
        this.emit('graphUpdated', {
            layoutReset: true,
            reason,
            editRevision: this.editRevision,
            topologyRevision: this.topologyRevision
        });
    }

    resolveProteinIds(ids) {
        const resolved = new Set();
        (ids || []).forEach(rawId => {
            const id = String(rawId).trim();
            if (!id) return;

            if (this.graph.nodes.has(id)) {
                resolved.add(id);
                return;
            }

            this.graph.getClusterMembers(id).forEach(memberId => {
                resolved.add(String(memberId));
            });
        });
        return Array.from(resolved);
    }

    getVisibleProteinIds() {
        const ids = [];
        this.graph.nodes.forEach((node, id) => {
            const nodeId = String(id);
            if (!this.hiddenNodes.has(nodeId)) ids.push(nodeId);
        });
        return ids;
    }

    getHiddenProteinIds() {
        return Array.from(this.hiddenNodes);
    }

    getHiddenEdgeIds() {
        return Array.from(this.hiddenEdges);
    }

    getHiddenEdgeWeightRanges() {
        return this.hiddenEdgeWeightRanges.map(range => ({ ...range }));
    }

    getHiddenEdgeEditPayload() {
        if (this.hiddenEdges.size === 0) {
            return { hiddenEdgeIds: [], hiddenEdgeWeightRanges: [] };
        }

        const rangeEdgeIds = new Set(this.getEdgeIdsForHiddenWeightRanges());
        if (this.hiddenEdgeWeightRanges.length > 0) {
            return {
                hiddenEdgeIds: this.getHiddenEdgeIds().filter(id => !rangeEdgeIds.has(id)),
                hiddenEdgeWeightRanges: this.getHiddenEdgeWeightRanges()
            };
        }

        return {
            hiddenEdgeIds: this.getHiddenEdgeIds(),
            hiddenEdgeWeightRanges: []
        };
    }

    getVisibleClusterMembers(clusterId) {
        return this.graph.getClusterMembers(clusterId)
            .map(String)
            .filter(id => !this.hiddenNodes.has(id));
    }

    getHighlightedNodeIds() {
        return Array.from(this.nodeColors.keys()).filter(nodeId => {
            const id = String(nodeId);
            if (this.hiddenNodes.has(id)) return false;
            return this.viewGraph.nodes.has(id);
        });
    }

    getEditStats() {
        const totalProteinCount = this.graph.nodes.size;
        const hiddenProteinCount = this.hiddenNodes.size;
        return {
            totalProteinCount,
            hiddenProteinCount,
            visibleProteinCount: Math.max(0, totalProteinCount - hiddenProteinCount),
            totalEdgeCount: this.graph.edges.size,
            hiddenEdgeCount: this.hiddenEdges.size,
            viewNodeCount: this.viewGraph.nodes.size,
            viewEdgeCount: this.viewGraph.edges.size,
            editRevision: this.editRevision
        };
    }

    pruneSelectionForHiddenProteins() {
        let changed = false;
        this.selectedNodes.forEach(id => {
            const nodeId = String(id);
            if (this.hiddenNodes.has(nodeId)) {
                this.selectedNodes.delete(id);
                changed = true;
            }
        });
        if (changed) this.emit('selectionUpdated', Array.from(this.selectedNodes));
    }

    pruneSelectionToViewGraph() {
        if (this.selectedNodes.size === 0) return;

        let changed = false;
        this.selectedNodes.forEach(id => {
            if (!this.viewGraph.nodes.has(id)) {
                this.selectedNodes.delete(id);
                changed = true;
            }
        });
        if (changed) this.emit('selectionUpdated', Array.from(this.selectedNodes));
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
