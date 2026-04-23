/**
 * Main Application Entry Point (New Architecture)
 */
async function initApp() {
    // 1. Initialize State
    const appState = new AppState();
    window.appState = appState; // For debugging

    // 2. Initialize Module System
    const moduleSystem = new ModuleSystem(appState);

    // Register modules based on configuration
    if (window.ModuleConfig && window.ModuleConfig.activeModules) {
        window.ModuleConfig.activeModules.forEach(moduleId => {
            // Convention: Module object is available on window as PascalCase(moduleId) + "Module"
            // e.g., "species-selector" -> window.SpeciesSelectorModule
            // OR we can just look for the object if we know the naming convention.

            // Helper to convert kebab-case to PascalCase
            const toPascalCase = (str) => str.replace(/(^\w|-\w)/g, (text) => text.replace(/-/, "").toUpperCase());
            const moduleName = toPascalCase(moduleId) + "Module";

            const module = window[moduleName];
            if (module) {
                moduleSystem.register(module);
            } else {
                console.warn(`Module ${moduleId} (expected ${moduleName}) not found. Make sure script is loaded.`);
            }
        });
    } else {
        console.warn("No module configuration found.");
    }

    // 3. Initialize Visualization Adapter
    const d3Adapter = new D3Adapter(appState, 'cy');
    window.d3Adapter = d3Adapter; // Expose for debugging

    // 4. UI Controls
    const networkSelect = document.getElementById('network-select');
    const refreshBtn = document.getElementById('refresh-btn');
    const loadBtn = document.getElementById('load-btn');
    const infoDiv = document.getElementById('info');

    // Zoom Controls
    const bindButton = (id, callback) => {
        const btn = document.getElementById(id);
        if (btn) {
            btn.addEventListener('click', callback);
        } else {
            console.warn(`Button ${id} not found.`);
        }
    };

    bindButton('zoom-in', () => d3Adapter.zoomIn());
    bindButton('zoom-out', () => d3Adapter.zoomOut());
    bindButton('zoom-reset', () => d3Adapter.resetZoom());
    bindButton('expand-all-btn', () => appState.expandAll());
    bindButton('collapse-all-btn', () => appState.collapseAll());

    // Function to fetch networks
    async function loadNetworkList() {
        try {
            if (!networkSelect) {
                console.error("Network select element not found!");
                return;
            }
            networkSelect.innerHTML = '<option value="">Loading...</option>';
            const res = await fetch('/api/networks');
            if (!res.ok) throw new Error(`API Error: ${res.status}`);

            const networks = await res.json();

            if (!Array.isArray(networks) || networks.length === 0) {
                console.warn("Network list is empty or invalid.");
                networkSelect.innerHTML = '<option value="">No networks found</option>';
                return;
            }

            networkSelect.innerHTML = '<option value="">Select a network...</option>';
            networks.forEach(net => {
                const option = document.createElement('option');
                option.value = net;
                option.textContent = net;
                networkSelect.appendChild(option);
            });
        } catch (err) {
            console.error("Failed to fetch networks list:", err);
            if (networkSelect) networkSelect.innerHTML = '<option value="">Error loading list</option>';
        }
    }

    // Initial load
    loadNetworkList();

    // Refresh button
    if (refreshBtn) {
        refreshBtn.addEventListener('click', loadNetworkList);
    } else {
        console.error("Refresh button not found!");
    }

    // Enable button when network selected
    if (networkSelect) {
        networkSelect.addEventListener('change', () => {
            if (loadBtn) loadBtn.disabled = !networkSelect.value;
        });
    }

    // Load Data
    if (loadBtn) {
        loadBtn.addEventListener('click', async () => {
            const filename = networkSelect.value;
            if (!filename) return;

            try {
                loadBtn.disabled = true;
                loadBtn.textContent = "Loading...";
                infoDiv.textContent = `Fetching ${filename}...`;

                const response = await fetch(`/api/networks/${filename}`);
                const data = await response.json();

                if (data.error) throw new Error(data.error);

                // Convert to internal Graph format
                const nodes = [];
                const edges = [];

                data.elements.nodes.forEach(n => {
                    nodes.push({ id: n.data.id, ...n.data });
                });

                data.elements.edges.forEach(e => {
                    edges.push({
                        id: e.data.id,
                        source: e.data.source,
                        target: e.data.target,
                        weight: e.data.weight,
                        ...e.data
                    });
                });

                infoDiv.textContent = `Loaded: ${nodes.length} nodes, ${edges.length} edges`;
                appState.setGraphData(nodes, edges, filename);

            } catch (err) {
                console.error("Failed to load network:", err);
                infoDiv.textContent = "Error: " + err.message;
                alert("Failed to load network: " + err.message);
            } finally {
                loadBtn.disabled = false;
                loadBtn.textContent = "Load Network";
            }
        });
    }



}

if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initApp);
} else {
    initApp();
}
