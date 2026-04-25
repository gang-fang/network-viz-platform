/**
 * Module Configuration
 * Lists the modules that should be loaded by the application.
 * The order here determines the initialization order.
 */
const ModuleConfig = {
    activeModules: [
        'search-highlight',
        'species-selector',
        'network-editor',
        'clear-highlights',
        'uniprot-tooltip',
        'export-panel'

    ]
};

// Export for browser environment
if (typeof window !== 'undefined') {
    window.ModuleConfig = ModuleConfig;
}

// Export for Node.js environment (if needed for tests)
if (typeof module !== 'undefined' && module.exports) {
    module.exports = ModuleConfig;
}
