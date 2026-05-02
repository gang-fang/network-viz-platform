/**
 * Search and Highlight Module
 * Handles searching for proteins and managing highlight colors.
 */
const SearchHighlightModule = {
    id: "search-highlight",
    context: null,
    activeHighlightColor: '#e74c3c', // Default red
    currentLayerId: null,

    // UI Elements
    searchInput: null,
    colorPalette: null,
    colorCanvas: null,
    selectedColorPreview: null,
    colorValueText: null,

    init(context) {
        this.context = context;
        this.createUI();
    },

    createUI() {
        const container = document.createElement('div');
        container.className = "control-group";
        container.style.marginTop = "15px";
        const defaultSearchValue = this.getDefaultSearchValueFromUrl();

        // Label
        const label = document.createElement('label');
        label.htmlFor = "search-input";
        label.textContent = "Highlight Proteins (UniProt AC):";
        label.style.fontSize = "0.94rem";
        label.style.whiteSpace = "nowrap";
        container.appendChild(label);

        // Controls Wrapper
        const wrapper = document.createElement('div');
        wrapper.style.display = "flex";
        wrapper.style.gap = "5px";
        wrapper.style.alignItems = "center";
        wrapper.style.position = "relative";

        // 1. Color Palette Popup (Hidden initially)
        this.createColorPalette();
        wrapper.appendChild(this.colorPalette);

        // 2. Search Input
        this.searchInput = document.createElement('input');
        this.searchInput.type = "text";
        this.searchInput.id = "search-input";
        this.searchInput.placeholder = "e.g. P12345, Q67890";
        this.searchInput.className = "control-input";
        this.searchInput.style.marginBottom = "0";
        this.searchInput.style.flexGrow = "1";
        this.searchInput.value = defaultSearchValue;
        this.searchInput.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') {
                this.performSearch();
                this.openPalette();
            }
        });
        wrapper.appendChild(this.searchInput);

        // 3. Search Button (Magnifying Glass)
        const searchBtn = document.createElement('div');
        searchBtn.id = "search-btn";
        searchBtn.title = "Search";
        searchBtn.style.cursor = "pointer";
        searchBtn.style.fontSize = "1.2rem";
        searchBtn.textContent = "🔍";
        searchBtn.onclick = () => {
            this.performSearch();
            this.openPalette();
        };
        wrapper.appendChild(searchBtn);

        container.appendChild(wrapper);

        // Add to panel
        this.context.addPanelControl(container);

        // Close palette on outside click
        document.addEventListener('click', (e) => {
            if (this.colorPalette && !this.colorPalette.classList.contains('hidden') &&
                !this.colorPalette.contains(e.target) &&
                e.target !== searchBtn &&
                e.target !== this.searchInput) {
                this.colorPalette.classList.add('hidden');
            }
        });
    },

    getDefaultSearchValueFromUrl() {
        const seedsParam = new URLSearchParams(window.location.search).get('seeds');
        if (!seedsParam) {
            return '';
        }

        return seedsParam
            .split(',')
            .map(value => value.trim())
            .filter(Boolean)
            .join(', ');
    },

    createColorPalette() {
        this.colorPalette = document.createElement('div');
        this.colorPalette.id = "color-palette";
        this.colorPalette.className = "hidden";
        this.colorPalette.style.position = "absolute";
        this.colorPalette.style.top = "100%";
        this.colorPalette.style.left = "0";
        this.colorPalette.style.background = "#2c3e50";
        this.colorPalette.style.padding = "10px";
        this.colorPalette.style.borderRadius = "8px";
        this.colorPalette.style.boxShadow = "0 4px 12px rgba(0,0,0,0.5)";
        this.colorPalette.style.zIndex = "100";
        this.colorPalette.style.display = "flex";
        this.colorPalette.style.flexDirection = "column";
        this.colorPalette.style.gap = "10px";
        this.colorPalette.style.width = "220px";

        // Title
        const title = document.createElement('div');
        title.style.fontSize = "0.8rem";
        title.style.color = "#bdc3c7";
        title.style.marginBottom = "5px";
        title.textContent = "Pick a Highlight Color";
        this.colorPalette.appendChild(title);

        // Canvas
        this.colorCanvas = document.createElement('canvas');
        this.colorCanvas.id = "color-canvas";
        this.colorCanvas.width = 200;
        this.colorCanvas.height = 150;
        this.colorCanvas.style.cursor = "crosshair";
        this.colorCanvas.style.borderRadius = "4px";
        this.colorCanvas.style.border = "1px solid #34495e";
        this.colorCanvas.onclick = (e) => this.pickColor(e);
        this.colorCanvas.onmousemove = (e) => this.previewColor(e);
        this.colorPalette.appendChild(this.colorCanvas);

        // Preview Row
        const previewRow = document.createElement('div');
        previewRow.style.display = "flex";
        previewRow.style.justifyContent = "space-between";
        previewRow.style.alignItems = "center";

        this.selectedColorPreview = document.createElement('div');
        this.selectedColorPreview.id = "selected-color-preview";
        this.selectedColorPreview.style.width = "30px";
        this.selectedColorPreview.style.height = "30px";
        this.selectedColorPreview.style.borderRadius = "50%";
        this.selectedColorPreview.style.border = "2px solid #fff";
        this.selectedColorPreview.style.backgroundColor = this.activeHighlightColor;
        previewRow.appendChild(this.selectedColorPreview);

        this.colorValueText = document.createElement('div');
        this.colorValueText.id = "color-value-text";
        this.colorValueText.style.fontSize = "0.8rem";
        this.colorValueText.style.color = "#fff";
        this.colorValueText.textContent = this.activeHighlightColor;
        previewRow.appendChild(this.colorValueText);

        this.colorPalette.appendChild(previewRow);
    },

    initColorPickerCanvas() {
        const ctx = this.colorCanvas.getContext('2d');
        const width = this.colorCanvas.width;
        const height = this.colorCanvas.height;

        // Hue Gradient
        const gradient = ctx.createLinearGradient(0, 0, width, 0);
        gradient.addColorStop(0, "rgb(255, 0, 0)");
        gradient.addColorStop(0.15, "rgb(255, 0, 255)");
        gradient.addColorStop(0.33, "rgb(0, 0, 255)");
        gradient.addColorStop(0.49, "rgb(0, 255, 255)");
        gradient.addColorStop(0.67, "rgb(0, 255, 0)");
        gradient.addColorStop(0.84, "rgb(255, 255, 0)");
        gradient.addColorStop(1, "rgb(255, 0, 0)");

        ctx.fillStyle = gradient;
        ctx.fillRect(0, 0, width, height);

        // Saturation/Brightness Gradient
        const whiteGrad = ctx.createLinearGradient(0, 0, 0, height);
        whiteGrad.addColorStop(0, "rgba(255, 255, 255, 1)");
        whiteGrad.addColorStop(0.5, "rgba(255, 255, 255, 0)");
        whiteGrad.addColorStop(0.5, "rgba(0, 0, 0, 0)");
        whiteGrad.addColorStop(1, "rgba(0, 0, 0, 1)");

        ctx.fillStyle = whiteGrad;
        ctx.fillRect(0, 0, width, height);
    },

    openPalette() {
        this.colorPalette.classList.remove('hidden');
        this.initColorPickerCanvas();
    },

    previewColor(e) {
        const color = this.getColorFromEvent(e);
        this.selectedColorPreview.style.backgroundColor = color;
        this.colorValueText.textContent = color;

        // Real-time update if we have an active layer
        if (this.currentLayerId) {
            this.context.updateHighlightLayerColor(this.currentLayerId, color);
        }
    },

    pickColor(e) {
        const color = this.getColorFromEvent(e);
        this.activeHighlightColor = color;

        // Commit the color (already updated via preview, but ensure it's set)
        if (this.currentLayerId) {
            this.context.updateHighlightLayerColor(this.currentLayerId, color);
        } else if (this.searchInput.value.trim()) {
            // If no layer yet but we have search text, perform search now
            this.performSearch();
        }

        // Close palette
        this.colorPalette.classList.add('hidden');
    },

    getColorFromEvent(e) {
        const rect = this.colorCanvas.getBoundingClientRect();
        const x = e.clientX - rect.left;
        const y = e.clientY - rect.top;
        const ctx = this.colorCanvas.getContext('2d');
        const imageData = ctx.getImageData(x, y, 1, 1).data;
        return `rgb(${imageData[0]}, ${imageData[1]}, ${imageData[2]})`;
    },

    async performSearch() {
        const query = this.searchInput.value.trim();
        if (!query) return;

        const accessions = query.split(/[\s,;]+/).filter(s => s);
        if (accessions.length === 0) return;

        const filename = this.context.getCurrentNetwork();

        if (!filename) return;

        try {
            const res = await fetch('/api/networks/search', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ network: filename, accessions })
            });

            if (!res.ok) throw new Error("Search failed");

            const result = await res.json();

            const layerId = `search:${Date.now()}`;
            this.currentLayerId = layerId;
            this.context.addHighlightLayer(layerId, result.matches, this.activeHighlightColor);

        } catch (err) {
            console.error("Search error:", err);
            alert("Search failed: " + err.message);
        }
    }
};

// Export
if (typeof module !== 'undefined' && module.exports) {
    module.exports = SearchHighlightModule;
} else {
    window.SearchHighlightModule = SearchHighlightModule;
}
