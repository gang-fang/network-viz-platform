/**
 * SJI Edge Highlight Module
 * Highlights visible edges whose SJI weight is within a user-selected range.
 */
const SjiEdgeHighlightModule = {
    id: 'sji-edge-highlight',
    popupMargin: 24,
    context: null,
    popup: null,
    minInput: null,
    maxInput: null,
    selectedColor: '#e74c3c',
    edgeLayerId: 'sji-edge-highlight:main',
    hasInitializedRange: false,

    init(context) {
        this.context = context;
        this.context.on('graphUpdated', (detail) => {
            if (detail?.reason === 'load') {
                this.hasInitializedRange = false;
                if (this.popup && this.popup.style.display !== 'none') {
                    this._setDefaultRange();
                }
            }
        });

        const btn = document.createElement('button');
        btn.textContent = 'Highlight SJI Edges';
        btn.className = 'control-button';
        btn.style.width = '100%';
        btn.style.marginTop = '10px';
        btn.addEventListener('click', () => this.togglePopup());
        context.addPanelControl(btn);

        this._createPopup();
    },

    _createPopup() {
        this.popup = document.createElement('div');
        this.popup.className = 'floating-panel sji-edge-highlight-panel';
        this.popup.style.display = 'none';

        const header = document.createElement('div');
        header.className = 'panel-header';

        const title = document.createElement('h3');
        title.textContent = 'Highlight SJI Edges';
        header.appendChild(title);

        const closeBtn = document.createElement('button');
        closeBtn.className = 'close-button';
        closeBtn.textContent = 'x';
        closeBtn.addEventListener('click', () => this.togglePopup());
        header.appendChild(closeBtn);

        const content = document.createElement('div');
        content.className = 'panel-content sji-edge-highlight-content';

        const rangeRow = document.createElement('div');
        rangeRow.style.cssText = 'display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:12px;';
        this.minInput = this._createRangeInput('Min', '0');
        this.maxInput = this._createRangeInput('Max', '1');
        rangeRow.appendChild(this.minInput.wrapper);
        rangeRow.appendChild(this.maxInput.wrapper);
        content.appendChild(rangeRow);

        const actions = document.createElement('div');
        actions.className = 'sji-edge-highlight-actions';

        const colorRow = document.createElement('div');
        colorRow.style.cssText = 'display:flex;gap:5px;flex-wrap:wrap;';
        const COLORS = ['#e74c3c','#e91e63','#2ecc71','#f1c40f','#9b59b6','#e67e22','#1abc9c','#34495e'];
        COLORS.forEach(color => {
            const swatch = document.createElement('div');
            swatch.className = 'sji-edge-highlight-swatch';
            swatch.style.cssText =
                `width:20px;height:20px;border-radius:50%;background:${color};cursor:pointer;` +
                `border:${this.selectedColor === color ? '2px solid #000' : '1px solid #ccc'};`;
            swatch.addEventListener('click', () => {
                this.selectedColor = color;
                colorRow.querySelectorAll('.sji-edge-highlight-swatch').forEach(s => {
                    s.style.border = '1px solid #ccc';
                });
                swatch.style.border = '2px solid #000';
            });
            colorRow.appendChild(swatch);
        });
        actions.appendChild(colorRow);

        const highlightBtn = document.createElement('button');
        highlightBtn.className = 'control-button';
        highlightBtn.textContent = 'Highlight';
        highlightBtn.style.flexShrink = '0';
        highlightBtn.addEventListener('click', () => this._applyHighlight());
        actions.appendChild(highlightBtn);
        content.appendChild(actions);

        this.popup.appendChild(header);
        this.popup.appendChild(content);
        document.body.appendChild(this.popup);
        this._makeDraggable(this.popup);
    },

    _createRangeInput(labelText, defaultValue) {
        const wrapper = document.createElement('label');
        wrapper.style.cssText = 'display:flex;flex-direction:column;gap:4px;font-size:0.9rem;color:#202a33;';
        wrapper.textContent = labelText;

        const input = document.createElement('input');
        input.type = 'number';
        input.min = '0';
        input.max = '1';
        input.step = '0.01';
        input.value = defaultValue;
        input.className = 'control-input';
        input.style.marginBottom = '0';
        input.addEventListener('blur', () => this._resetFieldIfInvalid(input));

        wrapper.appendChild(input);
        return { wrapper, input };
    },

    togglePopup() {
        if (this.popup.style.display === 'none') {
            this.openDefaultPosition();
        } else {
            this.popup.style.display = 'none';
        }
    },

    openDefaultPosition() {
        if (!this.hasInitializedRange) this._setDefaultRange();
        this.popup.style.display = 'flex';
        this.popup.style.transform = 'none';

        const margin = this.popupMargin;
        const rect = this.popup.getBoundingClientRect();
        const left = Math.max(margin, window.innerWidth - rect.width - margin);
        const top = Math.max(margin, window.innerHeight - rect.height - margin);

        this.popup.style.left = left + 'px';
        this.popup.style.top = top + 'px';
    },

    _setDefaultRange() {
        this.minInput.input.value = '0';
        this.maxInput.input.value = this._formatNumber(this._getMaxSjiValue());
        this.hasInitializedRange = true;
    },

    _resetToFullRange() {
        this.minInput.input.value = '0';
        this.maxInput.input.value = '1';
    },

    _getMaxSjiValue() {
        const graph = this.context.getGraph();
        let max = 0;
        graph.edges.forEach(edge => {
            const value = Number(edge.weight);
            if (Number.isFinite(value)) max = Math.max(max, value);
        });
        return Math.min(1, Math.max(0, max));
    },

    _formatNumber(value) {
        return String(Number(value.toFixed(6)));
    },

    _getRange() {
        const min = Number(this.minInput.input.value);
        const max = Number(this.maxInput.input.value);
        if (!Number.isFinite(min) || !Number.isFinite(max) || min < 0 || max > 1 || min > max) {
            return null;
        }
        return { min, max };
    },

    _resetFieldIfInvalid(input) {
        const value = Number(input.value);
        if (!Number.isFinite(value) || value < 0 || value > 1) {
            this._resetToFullRange();
        }
    },

    _applyHighlight() {
        const range = this._getRange();
        if (!range) {
            this._resetToFullRange();
            return;
        }

        this.context.addEdgeHighlightLayer(this.edgeLayerId, range, this.selectedColor);
    },

    _makeDraggable(el) {
        const header = el.querySelector('.panel-header');
        let dragging = false, sx = 0, sy = 0, il = 0, it = 0;

        header.addEventListener('mousedown', e => {
            dragging = true;
            sx = e.clientX;
            sy = e.clientY;
            const r = el.getBoundingClientRect();
            il = r.left; it = r.top;
            el.style.transform = 'none';
            el.style.left = il + 'px';
            el.style.top = it + 'px';
            el.classList.add('dragging');
        });
        document.addEventListener('mousemove', e => {
            if (!dragging) return;
            el.style.left = (il + e.clientX - sx) + 'px';
            el.style.top = (it + e.clientY - sy) + 'px';
        });
        document.addEventListener('mouseup', () => {
            if (!dragging) return;
            dragging = false;
            el.classList.remove('dragging');
        });
    },
};

if (typeof module !== 'undefined' && module.exports) {
    module.exports = SjiEdgeHighlightModule;
} else {
    window.SjiEdgeHighlightModule = SjiEdgeHighlightModule;
}
