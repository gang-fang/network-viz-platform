/**
 * Clear Highlights Module
 * Adds a standalone button to reset all highlights.
 */
const ClearHighlightsModule = {
    id: "clear-highlights",
    context: null,

    init(context) {
        this.context = context;
        const btn = document.createElement('button');
        btn.textContent = "Clear Highlights";
        btn.className = "control-button";
        btn.style.width = "100%";
        btn.style.marginTop = "10px";
        btn.onclick = () => {
            this.context.clearHighlightLayers();
        };

        context.addPanelControl(btn);
    }
};

// Export
if (typeof module !== 'undefined' && module.exports) {
    module.exports = ClearHighlightsModule;
} else {
    window.ClearHighlightsModule = ClearHighlightsModule;
}
