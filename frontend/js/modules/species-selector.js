/**
 * Species Selector Module
 *
 * Allows filtering and highlighting nodes based on NCBI_txID (species).
 *
 * When commontree.txt and NCBI_txID.csv are present in the configured data
 * directory the module renders an interactive taxonomic tree (SpeciesTreeView)
 * inside the popup panel.  If either file is missing it falls back to the
 * original flat checkbox list so the application stays fully functional.
 */
const SpeciesSelectorModule = {
    id: 'species-selector',
    popupMargin: 24,

    // ── State ─────────────────────────────────────────────────────────────────
    speciesMap:      new Map(),   // NCBI_txID → species name  (flat-list fallback)
    selectedSpecies: new Set(),   // currently selected NCBI_txIDs
    context:         null,

    // ── UI elements ───────────────────────────────────────────────────────────
    popup:              null,
    checkboxContainer:  null,   // hosts either SpeciesTreeView or flat checkboxes
    selectedColor:      '#e74c3c',

    // ── Tree-view state ───────────────────────────────────────────────────────
    treeView: null,    // SpeciesTreeView instance, or null when using flat list
    useTree:  false,   // true once the tree has been successfully loaded

    // ─────────────────────────────────────────────────────────────────────────
    init(context) {
        this.context = context;

        // Decide which UI to build (tree or flat list)
        this._initSpeciesData();

        // Button in the side panel
        const btn = document.createElement('button');
        btn.textContent = 'Highlight by Species';
        btn.className   = 'control-button';
        btn.style.width = '100%';
        btn.style.marginTop = '10px';
        btn.addEventListener('click', () => this.togglePopup());
        context.addPanelControl(btn);

        // Create the floating popup (hidden initially)
        this._createPopup();
    },

    // ─── Data loading ─────────────────────────────────────────────────────────

    async _initSpeciesData() {
        try {
            const tree = this.context.getSpeciesTree
                ? await this.context.getSpeciesTree()
                : null;

            if (tree) {
                await this._loadTreeView(tree);
            } else {
                await this._loadFlatList();
            }
        } catch (err) {
            console.warn('SpeciesSelector: species tree unavailable (' + err.message +
                         ') — falling back to flat species list.');
            await this._loadFlatList();
        }
    },

    /** Fetch and render the taxonomic tree. */
    async _loadTreeView(treeData) {
        let tree = treeData;
        if (!tree) {
            const res = await fetch('/api/species-tree');
            if (res.status === 404) {
                await this._loadFlatList();
                return;
            }
            if (!res.ok) throw new Error('GET /api/species-tree failed: ' + res.status);
            ({ tree } = await res.json());
        }

        this.useTree  = true;
        this.treeView = new SpeciesTreeView(this.checkboxContainer, {
            maxHeight:   '280px',
            showSearch:  true,
            showToolbar: false,   // Select All / Deselect All are in the popup header
            showBadges:  true,
            onChange: (selected) => {
                this.selectedSpecies.clear();
                selected.forEach(s => this.selectedSpecies.add(s.taxid));
            },
        });
        this.treeView.load(tree);
    },

    /** Original flat checkbox list (fallback when tree files are absent). */
    async _loadFlatList() {
        try {
            const data = this.context.getSpeciesNames
                ? await this.context.getSpeciesNames()
                : await fetch('/api/species-names').then(r => {
                    if (!r.ok) throw new Error('Failed to fetch species names');
                    return r.json();
                });

            data.forEach(item => {
                this.speciesMap.set(String(item.ncbi_txid), item.species_name);
            });
            this._populateCheckboxes();
        } catch (err) {
            console.error('SpeciesSelector flat-list error:', err);
        }
    },

    // ─── Popup creation ───────────────────────────────────────────────────────

    _createPopup() {
        this.popup = document.createElement('div');
        this.popup.className    = 'floating-panel hidden';
        this.popup.style.width  = '555px';
        this.popup.style.display = 'none';

        // ── Header ────────────────────────────────────────────────────────────
        const header = document.createElement('div');
        header.className = 'panel-header';
        header.innerHTML = '<h3>Select Species</h3>';

        const closeBtn = document.createElement('button');
        closeBtn.className   = 'close-button';
        closeBtn.textContent = '×';
        closeBtn.addEventListener('click', () => this.togglePopup());
        header.appendChild(closeBtn);

        // ── Content ───────────────────────────────────────────────────────────
        const content = document.createElement('div');
        content.className = 'panel-content';

        // Select All / Deselect All row
        const controls = document.createElement('div');
        controls.style.cssText = 'display:flex;gap:6px;margin-bottom:8px;';

        const selAll = document.createElement('button');
        selAll.className   = 'control-button';
        selAll.textContent = 'Select All';
        selAll.addEventListener('click', () => this._toggleAll(true));

        const deselAll = document.createElement('button');
        deselAll.className   = 'control-button';
        deselAll.textContent = 'Deselect All';
        deselAll.addEventListener('click', () => this._toggleAll(false));

        controls.appendChild(selAll);
        controls.appendChild(deselAll);
        content.appendChild(controls);

        // Species list container (tree or flat checkboxes land here)
        this.checkboxContainer = document.createElement('div');
        this.checkboxContainer.className = 'checkbox-group';
        content.appendChild(this.checkboxContainer);

        // ── Footer ────────────────────────────────────────────────────────────
        const footer = document.createElement('div');
        footer.className = 'panel-footer';
        footer.style.cssText = 'flex-direction:row;gap:10px;align-items:center;justify-content:space-between;';

        // Color swatches
        const colorRow = document.createElement('div');
        colorRow.style.cssText = 'display:flex;gap:5px;flex-wrap:wrap;';

        const COLORS = ['#e74c3c','#e91e63','#2ecc71','#f1c40f','#9b59b6','#e67e22','#1abc9c','#34495e'];
        COLORS.forEach(color => {
            const swatch = document.createElement('div');
            swatch.style.cssText =
                `width:20px;height:20px;border-radius:50%;background:${color};cursor:pointer;` +
                `border:${this.selectedColor === color ? '2px solid #000' : '1px solid #ccc'};`;
            swatch.addEventListener('click', () => {
                this.selectedColor = color;
                colorRow.querySelectorAll('div').forEach(s => {
                    s.style.border = '1px solid #ccc';
                });
                swatch.style.border = '2px solid #000';
            });
            colorRow.appendChild(swatch);
        });
        footer.appendChild(colorRow);

        // Highlight button
        const highlightBtn = document.createElement('button');
        highlightBtn.className   = 'control-button';
        highlightBtn.textContent = 'Highlight';
        highlightBtn.style.flexShrink = '0';
        highlightBtn.addEventListener('click', () => this._applyAction('highlight'));
        footer.appendChild(highlightBtn);

        this.popup.appendChild(header);
        this.popup.appendChild(content);
        this.popup.appendChild(footer);
        document.body.appendChild(this.popup);
        this._makeDraggable(this.popup);
    },

    // ─── Flat-list helpers (fallback only) ────────────────────────────────────

    _populateCheckboxes() {
        this.checkboxContainer.innerHTML = '';

        const sorted = Array.from(this.speciesMap.entries())
            .sort((a, b) => a[1].localeCompare(b[1]));

        sorted.forEach(([txid, name]) => {
            const div = document.createElement('div');
            div.className = 'checkbox-item';

            const cb = document.createElement('input');
            cb.type   = 'checkbox';
            cb.value  = txid;
            cb.id     = 'sps-' + txid;
            cb.addEventListener('change', e => {
                if (e.target.checked) this.selectedSpecies.add(txid);
                else                  this.selectedSpecies.delete(txid);
            });

            const lbl = document.createElement('label');
            lbl.htmlFor     = 'sps-' + txid;
            lbl.textContent = name;
            lbl.style.cssText = 'font-size:0.9rem;color:black;cursor:pointer;';

            div.appendChild(cb);
            div.appendChild(lbl);
            this.checkboxContainer.appendChild(div);
        });
    },

    // ─── Select / Deselect All ────────────────────────────────────────────────

    _toggleAll(select) {
        if (this.useTree && this.treeView) {
            if (select) this.treeView.selectAll();
            else        this.treeView.clearSelection();
            // selectedSpecies is updated via the tree's onChange callback
        } else {
            // Flat list
            this.checkboxContainer.querySelectorAll('input[type="checkbox"]').forEach(cb => {
                cb.checked = select;
                if (select) this.selectedSpecies.add(cb.value);
                else        this.selectedSpecies.delete(cb.value);
            });
        }
    },

    // ─── Popup visibility ─────────────────────────────────────────────────────

    togglePopup() {
        if (this.popup.style.display === 'none') {
            this.openDefaultPosition();
        } else {
            this.popup.style.display = 'none';
        }
    },

    openDefaultPosition() {
        this.popup.style.display   = 'block';
        this.popup.style.transform = 'none';

        const margin = this.popupMargin;
        const rect = this.popup.getBoundingClientRect();
        const left = Math.max(margin, window.innerWidth - rect.width - margin);
        const top = Math.max(margin, window.innerHeight - rect.height - margin);

        this.popup.style.left = left + 'px';
        this.popup.style.top  = top + 'px';
    },

    // ─── Highlight action ─────────────────────────────────────────────────────

    async _fetchNodesForSpecies(networkName, speciesIds) {
        const res = await fetch('/api/networks/search-species', {
            method:  'POST',
            headers: { 'Content-Type': 'application/json' },
            body:    JSON.stringify({ network: networkName, speciesIds: Array.from(speciesIds) }),
        });
        if (!res.ok) throw new Error('Failed to fetch nodes for species');
        return res.json();
    },

    async _applyAction(action) {
        if (this.selectedSpecies.size === 0) {
            alert('Please select at least one species.');
            return;
        }
        const networkName = this.context.getCurrentNetwork();
        if (!networkName) {
            alert('No network selected.');
            return;
        }
        try {
            const result = await this._fetchNodesForSpecies(networkName, this.selectedSpecies);
            if (action === 'highlight') {
                if (result.matches && result.matches.length > 0) {
                    const layerId = 'species:' + Date.now();
                    this.context.addHighlightLayer(layerId, result.matches, this.selectedColor);
                } else {
                    alert('No nodes found for the selected species.');
                }
            }
        } catch (err) {
            console.error('Species action failed:', err);
            alert('Failed to apply action: ' + err.message);
        }
    },

    // ─── Draggable panel ─────────────────────────────────────────────────────

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
            el.style.top  = it + 'px';
            el.classList.add('dragging');
        });
        document.addEventListener('mousemove', e => {
            if (!dragging) return;
            el.style.left = (il + e.clientX - sx) + 'px';
            el.style.top  = (it + e.clientY - sy) + 'px';
        });
        document.addEventListener('mouseup', () => {
            if (!dragging) return;
            dragging = false;
            el.classList.remove('dragging');
        });
    },

    // ── Keep old camelCase entry-points as aliases for any external callers ───
    toggleAll(select)    { return this._toggleAll(select); },
    applyAction(action)  { return this._applyAction(action); },
    fetchSpeciesData()   { return this._initSpeciesData(); },
    createPopup()        { return this._createPopup(); },
    makeDraggable(el)    { return this._makeDraggable(el); },
};

if (typeof module !== 'undefined' && module.exports) {
    module.exports = SpeciesSelectorModule;
} else {
    window.SpeciesSelectorModule = SpeciesSelectorModule;
}
