/**
 * Module Configuration
 * Lists the modules that should be loaded by the application.
 * The order here determines the initialization order.
 */
const ModuleConfig = {
    activeModules: [
        'search-highlight',
        'species-selector',
        'sji-edge-highlight',
        'network-editor',
        'clear-highlights',
        'uniprot-tooltip',
        'export-panel'
    ]
};

if (typeof window !== 'undefined') {
    window.ModuleConfig = ModuleConfig;
}
