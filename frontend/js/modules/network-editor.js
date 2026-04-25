/**
 * Network Editor Module
 * Removes/restores proteins from the current view and saves the edited network.
 *
 * The Species section now renders a SpeciesTreeView when commontree.txt and
 * NCBI_txID.csv are present on the server, falling back to the original
 * filter-input + <select multiple> when they are absent.
 */
const NetworkEditorModule = {
    id: 'network-editor',
    popupMargin: 24,
    context: null,
    popup: null,
    statsEl: null,
    statusEl: null,

    // ── Species section — flat-list (fallback) elements ───────────────────────
    speciesFilter: null,
    speciesSelect: null,

    idInput: null,
    edgeThresholdInput: null,
    saveNameInput: null,
    speciesMap: new Map(),
    selectedSpecies: new Set(),
    speciesFilterTimer: null,
    speciesMatchesCache: new Map(),
    lastDefaultSaveName: '',
    lastNetworkName: null,
    saveNameTouched: false,

    // ── Tree-view state ───────────────────────────────────────────────────────
    treeView: null,   // SpeciesTreeView instance, or null in flat-list mode
    useTree:  false,
    _speciesContainer: null,   // the div that hosts whichever widget is active

    // ─────────────────────────────────────────────────────────────────────────
    init(context) {
        this.context = context;

        this.createPanelButton();
        this.createPopup();
        this.bindContextMenu();

        // Load species data (tree or flat list)
        this._initSpeciesData();

        context.on('editUpdated',      (stats) => this.updateStats(stats));
        context.on('viewGraphUpdated', (stats) => this.updateStats(stats));
        context.on('selectionUpdated', ()      => this.updateStats(this.context.getEditStats()));
        context.on('graphUpdated',     ()      => {
            this.syncDefaultSaveName();
            this.updateStats(this.context.getEditStats());
        });
    },

    createPanelButton() {
        const btn = document.createElement('button');
        btn.textContent      = 'Edit Network';
        btn.className        = 'control-button';
        btn.style.width      = '100%';
        btn.style.marginTop  = '10px';
        btn.addEventListener('click', () => this.togglePopup());
        this.context.addPanelControl(btn);
    },

    bindContextMenu() {
        const target = document.getElementById('cy');
        if (!target) return;
        target.addEventListener('contextmenu', event => {
            event.preventDefault();
            this.openAt(event.pageX, event.pageY);
        });
    },

    createPopup() {
        this.popup = document.createElement('div');
        this.popup.id        = 'network-editor-panel';
        this.popup.className = 'floating-panel network-editor-panel';
        this.popup.style.display = 'none';

        const header = document.createElement('div');
        header.className = 'panel-header';

        const title = document.createElement('h3');
        title.textContent = 'Edit Network';
        const closeBtn = document.createElement('button');
        closeBtn.className   = 'close-button';
        closeBtn.textContent = 'x';
        closeBtn.addEventListener('click', () => this.closePopup());
        header.appendChild(title);
        header.appendChild(closeBtn);

        const content = document.createElement('div');
        content.className = 'panel-content network-editor-content';

        this.statsEl = document.createElement('div');
        this.statsEl.className = 'network-editor-stats';
        content.appendChild(this.statsEl);

        content.appendChild(this.createSelectionSection());
        content.appendChild(this.createSpeciesSection());
        content.appendChild(this.createIdSection());
        content.appendChild(this.createEdgeSection());
        content.appendChild(this.createSaveSection());

        this.statusEl = document.createElement('div');
        this.statusEl.className = 'network-editor-status';
        content.appendChild(this.statusEl);

        this.popup.appendChild(header);
        this.popup.appendChild(content);
        document.body.appendChild(this.popup);
        this.makeDraggable(this.popup);
        this.updateStats(this.context.getEditStats());
    },

    createSection(titleText) {
        const section = document.createElement('section');
        section.className = 'network-editor-section';
        const title = document.createElement('h4');
        title.textContent = titleText;
        section.appendChild(title);
        return section;
    },

    createButton(text, onClick, extraClass = '') {
        const button = document.createElement('button');
        button.type      = 'button';
        button.textContent = text;
        button.className = ('control-button ' + extraClass).trim();
        button.addEventListener('click', onClick);
        return button;
    },

    createSelectionSection() {
        const section = this.createSection('Selection and Highlights');
        const actions = document.createElement('div');
        actions.className = 'network-editor-actions';

        actions.appendChild(this.createButton('Remove Selected',    () => this.removeSelected()));
        actions.appendChild(this.createButton('Remove Highlighted', () => this.removeHighlighted()));
        actions.appendChild(this.createButton('Restore All Edits',  () => this.restoreAll()));

        section.appendChild(actions);
        return section;
    },

    // ─── Species section ──────────────────────────────────────────────────────
    // Creates a container div that is initially empty; the correct widget
    // (tree or flat list) is mounted into it once _initSpeciesData() resolves.

    createSpeciesSection() {
        const section = this.createSection('Species');

        // Widget container — filled by _mountTreeWidget() or _mountFlatWidget()
        this._speciesContainer = document.createElement('div');
        this._speciesContainer.style.marginBottom = '6px';
        section.appendChild(this._speciesContainer);

        const actions = document.createElement('div');
        actions.className = 'network-editor-actions';
        actions.appendChild(this.createButton('Remove Species', () => this.applySpeciesEdit('hide')));
        actions.appendChild(this.createButton('Restore Species', () => this.applySpeciesEdit('show')));
        section.appendChild(actions);

        return section;
    },

    // ── Called once data is ready ─────────────────────────────────────────────

    async _initSpeciesData() {
        try {
            const tree = this.context.getSpeciesTree
                ? await this.context.getSpeciesTree()
                : null;

            if (tree) {
                await this._mountTreeWidget(tree);
            } else {
                await this._loadFlatSpecies();
            }
        } catch (err) {
            console.warn('NetworkEditor: species tree unavailable (' + err.message +
                         ') — falling back to flat species list.');
            await this._loadFlatSpecies();
        }
    },

    /** Fetch tree JSON and mount a compact SpeciesTreeView. */
    async _mountTreeWidget(treeData) {
        let tree = treeData;
        if (!tree) {
            const res = await fetch('/api/species-tree');
            if (res.status === 404) {
                await this._loadFlatSpecies();
                return;
            }
            if (!res.ok) throw new Error('GET /api/species-tree failed: ' + res.status);
            ({ tree } = await res.json());
        }

        this.useTree  = true;
        this.treeView = new SpeciesTreeView(this._speciesContainer, {
            maxHeight:   '190px',
            showSearch:  true,
            showToolbar: false,   // editor is already compact
            showBadges:  true,
            onChange: (selected) => {
                this.selectedSpecies.clear();
                selected.forEach(s => this.selectedSpecies.add(s.taxid));
            },
        });
        this.treeView.load(tree);
    },

    /** Original flat-list path: filter input + <select multiple>. */
    async _loadFlatSpecies() {
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
            this._mountFlatWidget();
        } catch (err) {
            this.setStatus('Species list unavailable: ' + err.message, true);
        }
    },

    _mountFlatWidget() {
        // Filter input
        this.speciesFilter = document.createElement('input');
        this.speciesFilter.type        = 'text';
        this.speciesFilter.className   = 'control-input network-editor-input';
        this.speciesFilter.placeholder = 'Filter species…';
        this.speciesFilter.addEventListener('input', () => this.debounceSpeciesOptions());
        this._speciesContainer.appendChild(this.speciesFilter);

        // <select multiple>
        this.speciesSelect = document.createElement('select');
        this.speciesSelect.className = 'network-editor-species';
        this.speciesSelect.multiple  = true;
        this.speciesSelect.size      = 8;
        this.speciesSelect.addEventListener('change', () => {
            Array.from(this.speciesSelect.options).forEach(opt => {
                if (opt.selected) this.selectedSpecies.add(opt.value);
                else              this.selectedSpecies.delete(opt.value);
            });
        });
        this._speciesContainer.appendChild(this.speciesSelect);

        this.populateSpeciesOptions();
    },

    // ─── Flat-list helpers (kept for fallback path) ───────────────────────────

    debounceSpeciesOptions() {
        if (this.speciesFilterTimer) clearTimeout(this.speciesFilterTimer);
        this.speciesFilterTimer = setTimeout(() => this.populateSpeciesOptions(), 100);
    },

    populateSpeciesOptions() {
        if (!this.speciesSelect) return;

        const filter = this.speciesFilter.value.trim().toLowerCase();
        const sorted = Array.from(this.speciesMap.entries())
            .filter(([txid, name]) => {
                if (!filter) return true;
                return txid.toLowerCase().includes(filter) ||
                       String(name).toLowerCase().includes(filter);
            })
            .sort((a, b) => a[1].localeCompare(b[1]));

        this.speciesSelect.innerHTML = '';
        sorted.forEach(([txid, name]) => {
            const opt      = document.createElement('option');
            opt.value      = txid;
            opt.textContent = name + ' (' + txid + ')';
            opt.selected   = this.selectedSpecies.has(txid);
            this.speciesSelect.appendChild(opt);
        });
    },

    // ─── Remaining sections (unchanged) ───────────────────────────────────────

    createIdSection() {
        const section = this.createSection('Protein IDs');

        this.idInput = document.createElement('textarea');
        this.idInput.className   = 'network-editor-ids';
        this.idInput.placeholder = 'P12345, Q67890';
        this.idInput.rows        = 3;
        section.appendChild(this.idInput);

        const actions = document.createElement('div');
        actions.className = 'network-editor-actions';
        actions.appendChild(this.createButton('Remove IDs',  () => this.applyIdEdit('hide')));
        actions.appendChild(this.createButton('Restore IDs', () => this.applyIdEdit('show')));
        section.appendChild(actions);

        return section;
    },

    createEdgeSection() {
        const section = this.createSection('Edges by SJI Weight');

        const help = document.createElement('div');
        help.className   = 'network-editor-help';
        help.textContent = 'Remove or restore original edges whose raw SJI weight is below the threshold; threshold must be > 0 and < 1, and aggregated cluster weights are not used.';
        section.appendChild(help);

        this.edgeThresholdInput = document.createElement('input');
        this.edgeThresholdInput.type        = 'number';
        this.edgeThresholdInput.className   = 'control-input network-editor-input';
        this.edgeThresholdInput.placeholder = '0.5';
        this.edgeThresholdInput.min         = '0';
        this.edgeThresholdInput.max         = '1';
        this.edgeThresholdInput.step        = '0.01';
        section.appendChild(this.edgeThresholdInput);

        const actions = document.createElement('div');
        actions.className = 'network-editor-actions';
        actions.appendChild(this.createButton('Remove Edges Below',  () => this.applyEdgeThreshold('hide')));
        actions.appendChild(this.createButton('Restore Edges Below', () => this.applyEdgeThreshold('show')));
        actions.appendChild(this.createButton('Restore All Edges',   () => this.restoreAllEdges()));
        section.appendChild(actions);

        return section;
    },

    createSaveSection() {
        const section = this.createSection('Save Edited Network');

        this.saveNameInput = document.createElement('input');
        this.saveNameInput.type        = 'text';
        this.saveNameInput.className   = 'control-input network-editor-input';
        this.saveNameInput.placeholder = 'edited_network';
        this.syncDefaultSaveName({ force: true });
        this.saveNameInput.addEventListener('input', () => { this.saveNameTouched = true; });
        section.appendChild(this.saveNameInput);

        const actions = document.createElement('div');
        actions.className = 'network-editor-actions';
        actions.appendChild(this.createButton('Save', () => this.saveEditedNetwork(), 'network-editor-save'));
        section.appendChild(actions);

        return section;
    },

    // ─── Popup visibility ─────────────────────────────────────────────────────

    togglePopup() {
        if (this.popup.style.display === 'none') this.openDefaultPosition();
        else this.closePopup();
    },

    openDefaultPosition() {
        this.popup.style.display   = 'block';
        this.popup.style.transform = 'none';

        const margin = this.popupMargin;
        const rect = this.popup.getBoundingClientRect();
        const left = Math.max(margin, window.innerWidth - rect.width - margin);
        const top = Math.max(margin, window.innerHeight - rect.height - margin);

        this.popup.style.left      = left + 'px';
        this.popup.style.top       = top + 'px';
        this.syncDefaultSaveName();
        this.updateStats(this.context.getEditStats());
    },

    openAt(x, y) {
        this.popup.style.display   = 'block';
        this.popup.style.transform = 'none';
        this.popup.style.left      = x + 'px';
        this.popup.style.top       = y + 'px';
        this.syncDefaultSaveName();
        this.updateStats(this.context.getEditStats());
    },

    closePopup() {
        this.popup.style.display = 'none';
    },

    // ─── Stats / status ───────────────────────────────────────────────────────

    updateStats(stats = {}) {
        if (!this.statsEl) return;
        const sel = this.context.getSelectedNodeCount ? this.context.getSelectedNodeCount() : 0;
        this.statsEl.textContent =
            'Visible proteins: '  + (stats.visibleProteinCount || 0) + ' / ' + (stats.totalProteinCount || 0) + '. ' +
            'Hidden proteins: '   + (stats.hiddenProteinCount  || 0) + '. ' +
            'Hidden edges: '      + (stats.hiddenEdgeCount     || 0) + '. ' +
            'Drawn nodes: '       + (stats.viewNodeCount       || 0) + '. ' +
            'Drawn edges: '       + (stats.viewEdgeCount       || 0) + '. ' +
            'Selected: '          + sel + '.';
    },

    setStatus(message, isError = false) {
        if (!this.statusEl) return;
        this.statusEl.textContent = message || '';
        this.statusEl.classList.toggle('error', Boolean(isError));
    },

    // ─── Edit actions (unchanged) ─────────────────────────────────────────────

    parseIds(text) {
        return text.split(/[\s,;]+/).map(t => t.trim()).filter(Boolean);
    },

    removeSelected() {
        const ids = this.context.getSelectedNodeIds ? this.context.getSelectedNodeIds() : [];
        if (ids.length === 0) { this.setStatus('No selected nodes to remove.', true); return; }
        this.context.hideNodes(ids);
        this.setStatus('Removed ' + ids.length + ' selected visible node(s).');
    },

    removeHighlighted() {
        const ids = this.context.getHighlightedProteinIds();
        if (ids.length === 0) { this.setStatus('No highlighted visible proteins to remove.', true); return; }
        this.context.hideNodes(ids);
        this.setStatus('Removed ' + ids.length + ' highlighted protein(s).');
    },

    restoreAll() {
        let changed;
        if (this.context.showAllEdits) {
            changed = this.context.showAllEdits();
        } else {
            const n = this.context.showAllNodes();
            const e = this.context.showAllEdges ? this.context.showAllEdges() : false;
            changed = Boolean(n || e);
        }
        this.setStatus(
            changed ? 'Restored all hidden proteins and edges.' : 'No hidden edits to restore.',
            !changed
        );
    },

    restoreAllEdges() {
        const changed = this.context.showAllEdges ? this.context.showAllEdges() : false;
        this.setStatus(changed ? 'Restored all hidden edges.' : 'No hidden edges to restore.', !changed);
    },

    applyIdEdit(action) {
        const ids = this.parseIds(this.idInput.value);
        if (ids.length === 0) { this.setStatus('Enter at least one protein ID.', true); return; }

        const changed = action === 'hide'
            ? this.context.hideNodes(ids)
            : this.context.showNodes(ids);
        this.setStatus(
            changed
                ? (action === 'hide' ? 'Removed' : 'Restored') + ' matching protein IDs.'
                : 'No matching protein IDs changed.',
            !changed
        );
    },

    getSelectedSpeciesIds() {
        return Array.from(this.selectedSpecies);
    },

    async fetchNodesForSpecies(speciesIds) {
        const networkName = this.context.getCurrentNetwork();
        if (!networkName) throw new Error('No network selected');

        const key = networkName + ':' + speciesIds.slice().sort().join(',');
        if (this.speciesMatchesCache.has(key)) return this.speciesMatchesCache.get(key);

        const res = await fetch('/api/networks/search-species', {
            method:  'POST',
            headers: { 'Content-Type': 'application/json' },
            body:    JSON.stringify({ network: networkName, speciesIds }),
        });
        if (!res.ok) throw new Error('Failed to fetch nodes for species');

        const result = await res.json();
        this.speciesMatchesCache.set(key, result);
        return result;
    },

    async applySpeciesEdit(action) {
        const speciesIds = this.getSelectedSpeciesIds();
        if (speciesIds.length === 0) { this.setStatus('Select at least one species.', true); return; }

        try {
            const result = await this.fetchNodesForSpecies(speciesIds);
            const ids    = (result.matches || []).map(m => m.id);
            if (ids.length === 0) { this.setStatus('No proteins found for the selected species.', true); return; }

            const changed = action === 'hide'
                ? this.context.hideNodes(ids)
                : this.context.showNodes(ids);
            this.setStatus(
                changed
                    ? (action === 'hide' ? 'Removed' : 'Restored') + ' ' + ids.length + ' protein(s) by species.'
                    : 'Species selection did not change the edited network.',
                !changed
            );
        } catch (err) {
            this.setStatus(err.message, true);
        }
    },

    parseEdgeThreshold() {
        const value = Number(this.edgeThresholdInput.value);
        if (!Number.isFinite(value) || value <= 0 || value >= 1)
            throw new Error('SJI threshold must be greater than 0 and less than 1.');
        return value;
    },

    applyEdgeThreshold(action) {
        let threshold;
        try { threshold = this.parseEdgeThreshold(); }
        catch (err) { this.setStatus(err.message, true); return; }

        try {
            const result = action === 'hide'
                ? this.context.hideEdgesByWeightBelow(threshold)
                : this.context.showEdgesByWeightBelow(threshold);
            const verb   = action === 'hide' ? 'Removed' : 'Restored';
            const suffix = 'edge(s) with SJI < ' + threshold + '.';
            this.setStatus(
                result.changedCount > 0
                    ? verb + ' ' + result.changedCount + ' ' + suffix
                    : 'No edges changed (' + result.matchedCount + ' matched the threshold).',
                result.changedCount === 0
            );
        } catch (err) {
            this.setStatus(err.message, true);
        }
    },

    // ─── Save section helpers ─────────────────────────────────────────────────

    getDefaultSaveName() {
        const current = this.context.getCurrentNetwork();
        if (!current) return '';
        return current.replace(/\.csv$/i, '') + '_edited';
    },

    syncDefaultSaveName({ force = false } = {}) {
        if (!this.saveNameInput) return;

        const currentNetwork = this.context.getCurrentNetwork();
        const nextDefault    = this.getDefaultSaveName();
        const networkChanged = currentNetwork !== this.lastNetworkName;

        if (networkChanged) {
            this.lastNetworkName  = currentNetwork;
            this.saveNameTouched  = false;
            force = true;
        }

        const shouldUpdate = force
            || !this.saveNameTouched
            || this.saveNameInput.value === this.lastDefaultSaveName;

        this.lastDefaultSaveName = nextDefault;

        if (shouldUpdate) {
            this.saveNameInput.value = nextDefault;
            this.saveNameTouched     = false;
        }
    },

    async saveEditedNetwork() {
        const source = this.context.getCurrentNetwork();
        const name   = this.saveNameInput.value.trim();

        if (!source) { this.setStatus('No network selected.', true); return; }
        if (!name)   { this.setStatus('Enter a name for the edited network.', true); return; }

        this.setStatus('Saving edited network…');

        try {
            const edgeEditPayload = this.context.getHiddenEdgeEditPayload
                ? this.context.getHiddenEdgeEditPayload()
                : {
                    hiddenEdgeIds:          this.context.getHiddenEdgeIds ? this.context.getHiddenEdgeIds() : [],
                    hiddenEdgeWeightRanges: [],
                };

            const response = await fetch('/api/networks/edited', {
                method:  'POST',
                headers: { 'Content-Type': 'application/json' },
                body:    JSON.stringify({
                    source,
                    name,
                    hiddenNodeIds: this.context.getHiddenProteinIds(),
                    ...edgeEditPayload,
                }),
            });
            const result = await response.json();

            if (!response.ok) throw new Error(result.error || 'Failed to save edited network');

            this.statusEl.innerHTML = '';
            const text = document.createElement('span');
            const hiddenEdgeText = Number.isFinite(result.hiddenInSourceEdgeCount)
                ? ' Hidden edges: ' + result.hiddenInSourceEdgeCount + '.' : '';
            text.textContent = result.network + ' saved (' + result.nodeCount +
                               ' nodes, ' + result.edgeCount + ' edges).' + hiddenEdgeText + ' ';
            const link       = document.createElement('a');
            link.href        = result.viewerUrl;
            link.textContent = 'Open saved network';
            this.statusEl.appendChild(text);
            this.statusEl.appendChild(link);
            this.statusEl.classList.remove('error');
        } catch (err) {
            this.setStatus(err.message, true);
        }
    },

    // ─── Draggable panel ──────────────────────────────────────────────────────

    makeDraggable(el) {
        const header = el.querySelector('.panel-header');
        let dragging = false, sx = 0, sy = 0, il = 0, it = 0;

        header.addEventListener('mousedown', e => {
            dragging = true;
            sx = e.clientX; sy = e.clientY;
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

    // ── Keep old fetchSpeciesData alias for external callers / tests ──────────
    fetchSpeciesData() { return this._initSpeciesData(); },
};

if (typeof module !== 'undefined' && module.exports) {
    module.exports = NetworkEditorModule;
} else {
    window.NetworkEditorModule = NetworkEditorModule;
}
