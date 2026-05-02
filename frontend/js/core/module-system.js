/**
 * Module System
 * Handles loading and lifecycle of plugins.
 */
class ModuleSystem {
    constructor(appState) {
        this.appState = appState;
        this.modules = new Map();
    }

    /**
     * Register a module
     * @param {Object} module - The module instance
     */
    register(module) {
        if (!module.id) throw new Error("Module must have an id");

        this.modules.set(module.id, module);

        // Initialize module with context
        if (module.init) {
            const context = this.createContext(module.id);
            module.init(context);
        }
    }

    /**
     * Create a restricted context for the module
     */
    createContext(moduleId) {
        return {
            // Allow modules to listen to events
            on: (event, callback) => this.appState.on(event, callback),

            // Allow modules to inspect the graph (read-only mostly, or via specific methods)
            getGraph: () => this.appState.graph,
            getViewGraph: () => this.appState.viewGraph,
            getCurrentNetwork: () => this.appState.currentNetwork,
            getGraphRevision: () => this.appState.topologyRevision,
            getVisibleProteinIds: () => this.appState.getVisibleProteinIds(),
            getHiddenProteinIds: () => this.appState.getHiddenProteinIds(),
            getHiddenEdgeIds: () => this.appState.getHiddenEdgeIds(),
            getHiddenEdgeWeightRanges: () => this.appState.getHiddenEdgeWeightRanges(),
            getHiddenEdgeEditPayload: () => this.appState.getHiddenEdgeEditPayload(),
            getVisibleClusterMembers: (clusterId) => this.appState.getVisibleClusterMembers(clusterId),
            getHighlightedNodeIds: () => this.appState.getHighlightedNodeIds(),
            getEditStats: () => this.appState.getEditStats(),
            getSelectedNodeIds: () => Array.from(this.appState.selectedNodes),
            getSelectedNodeCount: () => this.appState.selectedNodes.size,
            isSelectionModeEnabled: () => this.appState.selectionMode,
            setSelectionMode: (enabled) => this.appState.setSelectionMode(enabled),
            clearSelection: () => this.appState.clearSelection(),
            getSpeciesNames: () => ModuleSystem.getSpeciesNames(),
            getSpeciesTree: () => ModuleSystem.getSpeciesTree(),

            // Allow modules to add attributes to nodes/edges
            updateNodeAttribute: (nodeId, key, value) => {
                const node = this.appState.graph.nodes.get(nodeId);
                if (node) {
                    node[key] = value;
                    this.appState.emit('nodeAttributeUpdated', { nodeId, key, value, source: moduleId });
                }
            },

            // Allow modules to add controls to the left panel
            addPanelControl: (element) => {
                const controlPanel = document.getElementById('control-panel');
                if (controlPanel) {
                    let moduleContainer = document.getElementById('module-controls-container');
                    if (!moduleContainer) {
                        moduleContainer = document.createElement('div');
                        moduleContainer.id = 'module-controls-container';
                        controlPanel.appendChild(moduleContainer);
                    }
                    moduleContainer.appendChild(element);
                }
            },

            // Expose State Actions
            addHighlightLayer: (layerId, matches, color) => this.appState.addHighlightLayer(layerId, matches, color),
            updateHighlightLayerColor: (layerId, color) => this.appState.updateHighlightLayerColor(layerId, color),
            removeHighlightLayer: (layerId) => this.appState.removeHighlightLayer(layerId),
            clearHighlightLayers: () => this.appState.clearHighlightLayers(),
            hideNodes: (ids) => this.appState.hideNodes(ids),
            showNodes: (ids) => this.appState.showNodes(ids),
            showAllNodes: () => this.appState.showAllNodes(),
            showAllEdges: () => this.appState.showAllEdges(),
            showAllEdits: () => this.appState.showAllEdits(),
            hideEdgesByWeightBelow: (threshold) => this.appState.hideEdgesByWeightBelow(threshold),
            showEdgesByWeightAbove: (threshold) => this.appState.showEdgesByWeightAbove(threshold)
        };
    }
}

ModuleSystem.speciesNamesPromise = null;
ModuleSystem.getSpeciesNames = function () {
    if (!ModuleSystem.speciesNamesPromise) {
        ModuleSystem.speciesNamesPromise = fetch('/api/species-names').then(res => {
            if (!res.ok) throw new Error("Failed to fetch species names");
            return res.json();
        });
    }
    return ModuleSystem.speciesNamesPromise;
};

ModuleSystem.speciesTreePromise = null;
ModuleSystem.getSpeciesTree = function () {
    if (!ModuleSystem.speciesTreePromise) {
        ModuleSystem.speciesTreePromise = fetch('/api/species-tree').then(async res => {
            if (res.status === 404) return null;
            if (!res.ok) throw new Error(`Failed to fetch species tree (${res.status})`);
            const payload = await res.json();
            return payload.tree || null;
        });
    }
    return ModuleSystem.speciesTreePromise;
};

// Export
if (typeof module !== 'undefined' && module.exports) {
    module.exports = ModuleSystem;
} else {
    window.ModuleSystem = ModuleSystem;
}
