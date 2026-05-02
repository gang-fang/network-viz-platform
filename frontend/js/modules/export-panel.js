/**
 * Export Panel Module
 * Handles the "Export Mode" logic and rendering of exported data to the right panel.
 */
const ExportPanelModule = {
    id: "export-panel",
    maxGroupNameLength: 16,
    availabilityCache: new Map(),
    availabilityBatchSize: 100,
    uniprotSectionSuffix: '',
    uniprotSectionContainer: null,
    uniprotSectionButtons: null,
    batchAnalysisPopup: null,
    batchAnalysisRows: null,
    batchAnalysisStatus: null,
    batchAnalysisSaveButton: null,
    exportInFlight: false,
    protDcCache: null,
    uniprotSectionOptions: [
        { label: 'Function', suffix: '/entry#function' },
        { label: 'Names & Taxonomy', suffix: '/entry#names_and_taxonomy' },
        { label: 'Expression', suffix: '/entry#expression' },
        { label: 'Subcellular Location', suffix: '/entry#subcellular_location' },
        { label: 'Phenotypes & Variants', suffix: '/entry#phenotypes_variants' },
        { label: 'PTM/Processing', suffix: '/entry#ptm_processing' },
        { label: 'Interaction', suffix: '/entry#interaction' },
        { label: 'Structure', suffix: '/entry#structure' },
        { label: 'Family & Domains', suffix: '/entry#family_and_domains' },
        { label: 'Sequence', suffix: '/entry#sequences' },
        { label: 'Variant viewer', suffix: '/variant-viewer' },
        { label: 'Feature viewer', suffix: '/feature-viewer' },
        { label: 'Genomic coordinates', suffix: '/genomic-coordinates' },
        { label: 'Publications', suffix: '/publications' },
        { label: 'External links', suffix: '/external-links' },
    ],

    escapeHtml(value) {
        return String(value)
            .replaceAll('&', '&amp;')
            .replaceAll('<', '&lt;')
            .replaceAll('>', '&gt;')
            .replaceAll('"', '&quot;')
            .replaceAll("'", '&#39;');
    },

    init(context) {
        this.context = context;

        this.selectModeBtn = document.getElementById('select-mode-btn');
        this.exportBtn = document.getElementById('export-btn');
        this.rightPanelContent = document.getElementById('export-content');

        // Clear placeholder if it exists
        if (this.rightPanelContent.querySelector('.placeholder-text')) {
            this.rightPanelContent.innerHTML = '';
        }

        this.createBatchAnalysisPopup();
        this.createUniprotSectionButtons();
        this.setupListeners();
    },

    createBatchAnalysisPopup() {
        if (document.getElementById('batch-analysis-panel')) return;

        const popup = document.createElement('div');
        popup.id = 'batch-analysis-panel';
        popup.className = 'floating-panel batch-analysis-panel';
        popup.style.display = 'none';
        popup.style.left = '40%';
        popup.style.top = '47%';

        const header = document.createElement('div');
        header.className = 'panel-header';

        const title = document.createElement('h3');
        title.textContent = 'Batch & Comparative Analysis';

        const closeBtn = document.createElement('button');
        closeBtn.className = 'close-button';
        closeBtn.textContent = 'x';
        closeBtn.addEventListener('click', () => this.closeBatchAnalysisPopup());

        header.appendChild(title);
        header.appendChild(closeBtn);

        const content = document.createElement('div');
        content.className = 'panel-content batch-analysis-content';

        const intro = document.createElement('p');
        intro.className = 'batch-analysis-intro';
        intro.textContent = 'Group proteins for comparison.';
        content.appendChild(intro);

        const rows = document.createElement('div');
        rows.className = 'batch-analysis-rows';
        content.appendChild(rows);
        this.batchAnalysisRows = rows;

        const helpGrid = document.createElement('div');
        helpGrid.className = 'batch-analysis-help-grid';

        const nameHelp = document.createElement('div');
        nameHelp.className = 'batch-analysis-help';
        nameHelp.textContent = 'Letters, numbers, _, -, and . are allowed. Maximum 16 characters; spaces are automatically removed.';
        helpGrid.appendChild(nameHelp);

        const accessionsHelp = document.createElement('div');
        accessionsHelp.className = 'batch-analysis-help';
        accessionsHelp.textContent = 'Copy UniProt ACs from Export & Analysis. ProtDC values will be removed automatically. If copying from another source, separate ACs with new lines, spaces, commas, or semicolons, or enter your own ACs.';
        helpGrid.appendChild(accessionsHelp);

        content.appendChild(helpGrid);

        const actions = document.createElement('div');
        actions.className = 'batch-analysis-actions';

        const status = document.createElement('div');
        status.className = 'batch-analysis-status';
        actions.appendChild(status);
        this.batchAnalysisStatus = status;

        const saveButton = document.createElement('button');
        saveButton.type = 'button';
        saveButton.className = 'viewer-primary-btn batch-analysis-save-btn';
        saveButton.textContent = 'Save';
        saveButton.addEventListener('click', () => {
            this.saveBatchAnalysisGroups().catch(err => {
                console.error('Batch analysis save failed:', err);
                this.setBatchAnalysisStatus(err.message, true);
            });
        });
        actions.appendChild(saveButton);
        this.batchAnalysisSaveButton = saveButton;

        content.appendChild(actions);

        const examplesSection = document.createElement('div');
        examplesSection.className = 'batch-analysis-examples';

        const examplesTitle = document.createElement('h4');
        examplesTitle.className = 'batch-analysis-examples-title';
        examplesTitle.textContent = 'Example workflow';
        examplesSection.appendChild(examplesTitle);

        const examplesList = document.createElement('div');
        examplesList.className = 'batch-analysis-examples-body';
        examplesList.innerHTML = [
            '<p><a href="https://colab.research.google.com/drive/1tISHcE9Hj-AF9j5oIZrK9HIg5AM1yGr6" target="_blank" rel="noopener noreferrer">https://colab.research.google.com/drive/1tISHcE9Hj-AF9j5oIZrK9HIg5AM1yGr6</a></p>',
            `<p>${this.escapeHtml('Protein structures were retrieved from the AlphaFold Protein Structure Database using UniProt accession numbers. Pairwise structural alignments were then performed with USalign for three comparison sets: within Group A, within Group B, and between Groups A and B. For each alignment, the TM-score, RMSD, aligned length, and sequence identity were extracted. The mean of the two length-normalized TM-scores was used as the primary measure of global structural similarity, and the resulting distributions were visualized with boxplots.')}</p>`,
            '<p><a href="https://colab.research.google.com/drive/1tKFCOIPJClFbR08L9qIomxVQKvWKDIu-" target="_blank" rel="noopener noreferrer">https://colab.research.google.com/drive/1tKFCOIPJClFbR08L9qIomxVQKvWKDIu-</a></p>',
            `<p>${this.escapeHtml('Protein sequences from the generated groups were aligned in a single multiple sequence alignment. Group-specific phylogenetic trees were then generated for DIVERGE v4 analysis of Type-I shifts in evolutionary constraints and Type-II physicochemical amino-acid divergence.')}</p>`,
        ].join('');
        examplesSection.appendChild(examplesList);

        content.appendChild(examplesSection);

        popup.appendChild(header);
        popup.appendChild(content);
        document.body.appendChild(popup);

        this.batchAnalysisPopup = popup;
        this.makeDraggable(popup);
        this.addBatchAnalysisRow();
    },

    createUniprotSectionButtons() {
        const rightPanel = document.getElementById('right-panel');
        if (!rightPanel || document.getElementById('export-link-sections')) return;

        const container = document.createElement('div');
        container.id = 'export-link-sections';
        container.className = 'export-link-sections hidden';

        const instruction = document.createElement('p');
        instruction.className = 'export-link-sections-help';
        instruction.textContent = 'Select a section below to update the links above to the corresponding UniProt section. Select "Function" to restore the main entry link. Note that some UniProt entries may not contain annotations for the selected section.';
        container.appendChild(instruction);

        this.uniprotSectionOptions.forEach(option => {
            const button = document.createElement('button');
            button.type = 'button';
            button.className = 'export-link-section-btn';
            button.textContent = option.label;
            button.dataset.sectionSuffix = option.suffix;
            button.addEventListener('click', () => {
                this.setUniprotSection(option.suffix);
            });
            container.appendChild(button);
        });

        const spacer = document.createElement('div');
        spacer.className = 'export-link-sections-spacer';
        container.appendChild(spacer);

        const batchButton = document.createElement('button');
        batchButton.type = 'button';
        batchButton.className = 'export-link-section-btn export-link-section-btn-large';
        batchButton.textContent = 'Batch & Comparative Analysis';
        batchButton.addEventListener('click', () => this.openBatchAnalysisPopup());
        container.appendChild(batchButton);

        rightPanel.appendChild(container);
        this.uniprotSectionContainer = container;
        this.uniprotSectionButtons = Array.from(container.querySelectorAll('.export-link-section-btn'));
        this.updateUniprotSectionButtonState();
        this.updateUniprotSectionControlsVisibility();
    },

    setupListeners() {
        // Button Clicks
        this.selectModeBtn.addEventListener('click', () => {
            const newMode = !this.context.isSelectionModeEnabled();
            this.context.setSelectionMode(newMode);
        });

        this.exportBtn.addEventListener('click', () => {
            this.exportData().catch(err => {
                console.error('Export rendering failed:', err);
            });
        });

        // State Changes
        this.context.on('selectionModeChanged', (enabled) => {
            this.updateButtonStates(enabled);
        });

        this.context.on('selectionUpdated', (selectedIds) => {
            this.updateExportButtonState(selectedIds);
        });
    },

    updateButtonStates(enabled) {
        if (enabled) {
            this.selectModeBtn.classList.add('active');
            this.updateExportButtonState(this.context.getSelectedNodeIds());
        } else {
            this.selectModeBtn.classList.remove('active');
            this.exportBtn.disabled = true;
        }
    },

    updateExportButtonState(selectedIds) {
        if (this.context.isSelectionModeEnabled() && selectedIds && selectedIds.length > 0 && !this.exportInFlight) {
            this.exportBtn.disabled = false;
        } else {
            this.exportBtn.disabled = true;
        }
    },

    async exportData() {
        if (this.exportInFlight) return;

        const selectedIds = this.context.getSelectedNodeIds();
        if (selectedIds.length === 0) return;

        this.exportInFlight = true;
        this.exportBtn.disabled = true;

        try {
            // Use viewGraph because it contains the currently visible nodes (including Clusters)
            const graph = this.context.getViewGraph();
            const nhNodes = [];
            const proteinNodes = [];

            selectedIds.forEach(id => {
                const node = graph.nodes.get(id);
                if (node) {
                    if (node._isCluster || node.kind === 'cluster' || node.type === 'cluster') {
                        nhNodes.push(node);
                    } else {
                        proteinNodes.push(node);
                    }
                }
            });

            await this.processExport(nhNodes, proteinNodes);
        } finally {
            this.exportInFlight = false;
            this.updateExportButtonState(this.context.getSelectedNodeIds());
        }
    },

    async processExport(nhNodes, proteinNodes) {
        const allAccessions = new Set();
        nhNodes.forEach(nh => {
            const members = this.getClusterMembersForExport(nh.id);
            members.forEach(ac => allAccessions.add(ac));
        });
        proteinNodes.forEach(node => allAccessions.add(node.id));
        const availabilityMap = await this.getAvailabilityMap(Array.from(allAccessions));
        const protDcMap = this.calculateProtDCMap();

        // 1. NH Nodes
        if (nhNodes.length > 0) {
            const sectionId = 'nh-section';
            const existingIds = this.getExistingIds(sectionId, 'NH');
            const rows = [];

            nhNodes.forEach(nh => {
                if (!existingIds.has(nh.id)) {
                    const members = this.getClusterMembersForExport(nh.id);
                    rows.push({
                        id: String(nh.id),
                        members: members.map(accession => ({
                            accession: String(accession),
                            linked: Boolean(availabilityMap.get(accession)),
                            protDc: this.getProtDCValue(accession, protDcMap),
                            showProtDc: false,
                        })),
                    });
                    existingIds.add(nh.id);
                }
            });

            if (rows.length > 0) {
                this.appendNeighborhoodSection(sectionId, rows);
            }
        }

        // 2. Protein Nodes
        if (proteinNodes.length > 0) {
            const sectionId = 'protein-section';
            const existingIds = this.getExistingIds(sectionId, 'Protein');
            const items = [];

            proteinNodes.forEach(n => {
                if (!existingIds.has(n.id)) {
                    items.push({
                        accession: String(n.id),
                        linked: Boolean(availabilityMap.get(n.id)),
                        protDc: this.getProtDCValue(n.id, protDcMap),
                        showProtDc: false,
                    });
                    existingIds.add(n.id);
                }
            });

            if (items.length > 0) {
                this.appendProteinSection(sectionId, items);
            }
        }

        this.updateUniprotSectionControlsVisibility();
    },

    getClusterMembersForExport(clusterId) {
        return this.context.getVisibleClusterMembers(clusterId);
    },

    calculateProtDCMap() {
        const graphRevision = this.context.getGraphRevision();

        if (
            this.protDcCache &&
            this.protDcCache.graphRevision === graphRevision
        ) {
            return this.protDcCache.map;
        }

        const graph = this.context.getGraph();
        const hiddenNodeIds = new Set(this.context.getHiddenProteinIds().map(String));
        const hiddenEdgeIds = new Set(this.context.getHiddenEdgeIds().map(String));
        const sums = new Map();
        const result = new Map();

        graph.nodes.forEach(node => {
            const nodeId = String(node.id);
            if (hiddenNodeIds.has(nodeId) || !node.NH_ID) return;
            sums.set(nodeId, 0);
        });

        graph.edges.forEach(edge => {
            const edgeId = String(edge.id);
            if (hiddenEdgeIds.has(edgeId)) return;

            const sourceId = String(edge.source);
            const targetId = String(edge.target);
            if (hiddenNodeIds.has(sourceId) || hiddenNodeIds.has(targetId)) return;

            const source = graph.nodes.get(sourceId);
            const target = graph.nodes.get(targetId);
            if (!source || !target || !source.NH_ID || !target.NH_ID) return;
            if (String(source.NH_ID) !== String(target.NH_ID)) return;

            const weight = Number(edge.weight);
            if (!Number.isFinite(weight)) return;

            sums.set(sourceId, (sums.get(sourceId) || 0) + weight);
            sums.set(targetId, (sums.get(targetId) || 0) + weight);
        });

        graph.nodes.forEach(node => {
            const nodeId = String(node.id);
            const nhId = node.NH_ID ? String(node.NH_ID) : '';
            const nhSize = Number(node.NH_Size);

            if (
                hiddenNodeIds.has(nodeId) ||
                !nhId ||
                !Number.isFinite(nhSize) ||
                nhSize <= 1
            ) {
                result.set(nodeId, 0);
                return;
            }

            result.set(nodeId, (sums.get(nodeId) || 0) / nhSize);
        });

        this.protDcCache = {
            graphRevision,
            map: result,
        };
        return result;
    },

    getProtDCValue(accession, protDcMap = null) {
        const value = protDcMap instanceof Map ? protDcMap.get(String(accession)) : undefined;
        return Number.isFinite(value) ? value : 0;
    },

    formatProtDC(value) {
        const numericValue = Number(value);
        return Number.isFinite(numericValue) ? numericValue.toFixed(2) : '0.00';
    },

    renderUniProtAccession(accession, availabilityMap, protDcMap = null, forceLinked = null, showProtDc = false) {
        const safeAc = this.escapeHtml(accession);
        const protDc = this.getProtDCValue(accession, protDcMap);
        const protDcText = this.formatProtDC(protDc);
        const displayText = showProtDc ? `${safeAc}(${protDcText})` : safeAc;
        const attrs = `data-accession="${safeAc}" data-protdc="${protDcText}" title="ProtDC: ${protDcText}"`;
        const isLinked = forceLinked === null ? availabilityMap.get(accession) : forceLinked;

        if (!isLinked) {
            return `<span class="export-protein-token" ${attrs}>${displayText}</span>`;
        }

        return `<a class="export-uniprot-link export-protein-token" ${attrs} href="${this.buildUniProtHref(accession)}" target="_blank" rel="noopener noreferrer">${displayText}</a>`;
    },

    buildUniProtHref(accession) {
        return `https://www.uniprot.org/uniprotkb/${encodeURIComponent(accession)}${this.uniprotSectionSuffix}`;
    },

    setUniprotSection(sectionSuffix) {
        this.uniprotSectionSuffix = sectionSuffix;
        this.updateExportedUniProtLinks();
        this.updateUniprotSectionButtonState();
    },

    updateExportedUniProtLinks() {
        const links = this.rightPanelContent.querySelectorAll('.export-uniprot-link');
        links.forEach(link => {
            const accession = (link.dataset.accession || link.textContent.trim()).trim();
            if (!accession) return;
            link.href = this.buildUniProtHref(accession);
        });
    },

    updateUniprotSectionButtonState() {
        if (!this.uniprotSectionButtons) return;

        this.uniprotSectionButtons.forEach(button => {
            if (!button.dataset.sectionSuffix) return;
            button.classList.toggle(
                'active',
                button.dataset.sectionSuffix === this.uniprotSectionSuffix
            );
        });
    },

    async getAvailabilityMap(accessions) {
        const availabilityMap = new Map();
        const uncached = [];

        accessions.forEach(accession => {
            if (this.availabilityCache.has(accession)) {
                availabilityMap.set(accession, this.availabilityCache.get(accession));
            } else {
                uncached.push(accession);
            }
        });

        if (uncached.length === 0) {
            return availabilityMap;
        }

        for (let i = 0; i < uncached.length; i += this.availabilityBatchSize) {
            const batch = uncached.slice(i, i + this.availabilityBatchSize);

            try {
                const response = await fetch('/api/uniprot/availability', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({ accessions: batch }),
                });

                if (!response.ok) {
                    throw new Error(`Availability check failed with status ${response.status}`);
                }

                const payload = await response.json();
                const results = Array.isArray(payload.results) ? payload.results : [];

                results.forEach(result => {
                    const isAvailable = Boolean(result && result.accession && result.available);
                    if (result && result.accession) {
                        this.availabilityCache.set(result.accession, isAvailable);
                        availabilityMap.set(result.accession, isAvailable);
                    }
                });
            } catch (err) {
                console.warn('UniProt availability check failed for one export batch; rendering those accessions as plain text.', err);
                batch.forEach(accession => {
                    this.availabilityCache.set(accession, false);
                    availabilityMap.set(accession, false);
                });
            }
        }

        uncached.forEach(accession => {
            if (!availabilityMap.has(accession)) {
                this.availabilityCache.set(accession, false);
                availabilityMap.set(accession, false);
            }
        });

        return availabilityMap;
    },

    getOrCreateSection(sectionId, titleBase) {
        let section = document.getElementById(sectionId);

        if (!section) {
            section = document.createElement('div');
            section.id = sectionId;
            section.className = 'export-section';
            section.dataset.count = 0;

            const header = document.createElement('div');
            header.className = 'section-header';

            const title = document.createElement('h4');
            title.className = 'section-title';

            const actions = document.createElement('div');
            actions.className = 'section-actions';

            // Eraser Icon
            const eraser = document.createElement('span');
            eraser.className = 'action-icon clear';
            eraser.innerHTML = '🗑️'; // Eraser/Trash
            eraser.title = 'Clear Section';
            eraser.onclick = () => this.clearSection(sectionId);

            // Download Icon
            const download = document.createElement('span');
            download.className = 'action-icon download';
            download.innerHTML = '⬇️'; // Download
            download.title = 'Download CSV';
            download.onclick = () => this.downloadSection(sectionId, titleBase);

            const sortProtDc = document.createElement('span');
            sortProtDc.className = 'action-icon sort-protdc';
            sortProtDc.innerHTML = '⬇️';
            sortProtDc.title = 'Sort proteins by ProtDC.';
            sortProtDc.onclick = () => this.sortSectionByProtDC(sectionId);

            actions.appendChild(sortProtDc);
            actions.appendChild(download);
            actions.appendChild(eraser);

            header.appendChild(title);
            header.appendChild(actions);

            const codeBlock = document.createElement('div');
            codeBlock.className = 'export-block';

            section.appendChild(header);
            section.appendChild(codeBlock);
            this.rightPanelContent.appendChild(section);
        }

        return section;
    },

    getExistingIds(sectionId, type) {
        const section = document.getElementById(sectionId);
        const data = section?._exportData;
        if (!data) return new Set();

        if (type === 'NH') {
            return new Set(data.rows.map(row => row.id));
        }

        return new Set(data.items.map(item => item.accession));
    },

    renderSectionFromData(sectionId) {
        const section = document.getElementById(sectionId);
        if (!section || !section._exportData) return;

        const codeBlock = section.querySelector('.export-block');
        const title = section.querySelector('.section-title');
        const data = section._exportData;

        if (data.type === 'NH') {
            const lines = ['Neighborhood_ID,Neighbors'];
            data.rows.forEach(row => {
                const renderedMembers = row.members.map(member => (
                    this.renderUniProtAccession(
                        member.accession,
                        new Map(),
                        new Map([[member.accession, member.protDc]]),
                        member.linked,
                        Boolean(member.showProtDc)
                    )
                ));
                lines.push(`${this.escapeHtml(row.id)},"${renderedMembers.join(', ')}"`);
            });
            codeBlock.innerHTML = `${lines.join('\n')}\n`;
            section.dataset.count = String(data.rows.length);
            title.textContent = `Neighborhoods (${data.rows.length})`;
            return;
        }

        if (data.type === 'Protein') {
            const lines = [];
            for (let i = 0; i < data.items.length; i += 5) {
                const renderedChunk = data.items.slice(i, i + 5).map(item => (
                    this.renderUniProtAccession(
                        item.accession,
                        new Map(),
                        new Map([[item.accession, item.protDc]]),
                        item.linked,
                        Boolean(item.showProtDc)
                    )
                ));
                lines.push(renderedChunk.join(', '));
            }
            codeBlock.innerHTML = lines.join('\n');
            section.dataset.count = String(data.items.length);
            title.textContent = `Proteins (${data.items.length})`;
        }
    },

    appendNeighborhoodSection(sectionId, rows) {
        const section = this.getOrCreateSection(sectionId, 'Neighborhoods');
        if (!section._exportData) {
            section._exportData = { type: 'NH', rows: [] };
        }
        section._exportData.rows.push(...rows);
        this.renderSectionFromData(sectionId);
    },

    appendProteinSection(sectionId, items) {
        const section = this.getOrCreateSection(sectionId, 'Proteins');
        if (!section._exportData) {
            section._exportData = { type: 'Protein', items: [] };
        }
        section._exportData.items.push(...items);
        this.renderSectionFromData(sectionId);
    },

    sortSectionByProtDC(sectionId) {
        if (sectionId === 'nh-section') {
            this.sortNeighborhoodSectionByProtDC(sectionId);
        } else if (sectionId === 'protein-section') {
            this.sortProteinSectionByProtDC(sectionId);
        }
    },

    sortNeighborhoodSectionByProtDC(sectionId) {
        const section = document.getElementById(sectionId);
        if (!section || !section._exportData || section._exportData.type !== 'NH') return;

        const protDcMap = this.calculateProtDCMap();
        section._exportData.rows.forEach(row => {
            row.members.forEach(member => {
                member.protDc = this.getProtDCValue(member.accession, protDcMap);
                member.showProtDc = true;
            });
            row.members.sort((a, b) => b.protDc - a.protDc || a.accession.localeCompare(b.accession));
        });
        this.renderSectionFromData(sectionId);
    },

    sortProteinSectionByProtDC(sectionId) {
        const section = document.getElementById(sectionId);
        if (!section || !section._exportData || section._exportData.type !== 'Protein') return;

        const protDcMap = this.calculateProtDCMap();
        section._exportData.items.forEach(item => {
            item.protDc = this.getProtDCValue(item.accession, protDcMap);
            item.showProtDc = true;
        });
        section._exportData.items.sort((a, b) => b.protDc - a.protDc || a.accession.localeCompare(b.accession));
        this.renderSectionFromData(sectionId);
    },

    clearSection(sectionId) {
        const section = document.getElementById(sectionId);
        if (section) {
            delete section._exportData;
            section.remove();
        }
        this.updateUniprotSectionControlsVisibility();
    },

    updateUniprotSectionControlsVisibility() {
        if (!this.uniprotSectionContainer) return;
        const hasExportedSections = Boolean(this.rightPanelContent.querySelector('.export-section'));
        this.uniprotSectionContainer.classList.toggle('hidden', !hasExportedSections);
    },

    openBatchAnalysisPopup() {
        if (!this.batchAnalysisPopup) return;
        this.batchAnalysisPopup.style.display = 'flex';
    },

    closeBatchAnalysisPopup() {
        if (!this.batchAnalysisPopup) return;
        this.batchAnalysisPopup.style.display = 'none';
    },

    addBatchAnalysisRow(group = { name: '', accessions: '' }) {
        if (!this.batchAnalysisRows) return;

        const row = document.createElement('div');
        row.className = 'batch-analysis-row';

        const nameInput = document.createElement('input');
        nameInput.type = 'text';
        nameInput.className = 'control-input batch-analysis-name-input';
        nameInput.placeholder = 'Group name';
        nameInput.maxLength = this.maxGroupNameLength;
        nameInput.value = group.name || '';
        nameInput.addEventListener('input', () => {
            this.applyNormalizedInputValue(nameInput, value => value.replace(/\s+/g, ''));
            this.clearBatchAnalysisRowError(nameInput);
        });
        row.appendChild(nameInput);

        const accessionInput = document.createElement('textarea');
        accessionInput.className = 'control-input batch-analysis-accessions-input';
        accessionInput.placeholder = 'UniProt ACs';
        accessionInput.rows = 2;
        accessionInput.value = group.accessions || '';
        accessionInput.addEventListener('input', () => {
            this.applyNormalizedInputValue(accessionInput, value => this.normalizeBatchAnalysisAccessionText(value));
            this.clearBatchAnalysisRowError(accessionInput);
        });
        row.appendChild(accessionInput);

        const addButton = document.createElement('button');
        addButton.type = 'button';
        addButton.className = 'batch-analysis-add-btn';
        addButton.textContent = '+';
        addButton.title = 'Add a new group.';
        addButton.addEventListener('click', () => this.addBatchAnalysisRow());
        row.appendChild(addButton);

        this.batchAnalysisRows.appendChild(row);
        return row;
    },

    applyNormalizedInputValue(input, normalize) {
        const originalValue = input.value;
        const normalizedValue = normalize(originalValue);
        if (originalValue === normalizedValue) return;

        const selectionStart = input.selectionStart ?? originalValue.length;
        const selectionEnd = input.selectionEnd ?? selectionStart;
        const normalizedStart = normalize(originalValue.slice(0, selectionStart)).length;
        const normalizedEnd = normalize(originalValue.slice(0, selectionEnd)).length;

        input.value = normalizedValue;
        input.setSelectionRange(normalizedStart, normalizedEnd);
    },

    clearBatchAnalysisRowError(input) {
        input.classList.remove('input-invalid');
        if (input.dataset.customError) {
            delete input.dataset.customError;
        }
        if (this.batchAnalysisStatus && this.batchAnalysisStatus.classList.contains('error')) {
            this.setBatchAnalysisStatus('');
        }
    },

    parseBatchAnalysisAccessions(value) {
        return this.normalizeBatchAnalysisAccessionText(value)
            .split(/[\s,;]+/)
            .map(token => token.replace(/[^A-Za-z0-9]/g, '').trim())
            .filter(Boolean);
    },

    normalizeBatchAnalysisAccessionText(value) {
        return String(value || '')
            .replace(/\(\d+(?:\.\d+)?\)/g, ' ')
            .replace(/[^A-Za-z0-9\s]/g, ' ');
    },

    collectBatchAnalysisGroups() {
        if (!this.batchAnalysisRows) return [];
        return Array.from(this.batchAnalysisRows.querySelectorAll('.batch-analysis-row')).map(row => ({
            row,
            nameInput: row.querySelector('.batch-analysis-name-input'),
            accessionInput: row.querySelector('.batch-analysis-accessions-input'),
        }));
    },

    markBatchAnalysisInputError(input, message) {
        input.classList.add('input-invalid');
        input.dataset.customError = message;
    },

    validateBatchAnalysisGroups() {
        const rows = this.collectBatchAnalysisGroups();
        const groups = [];
        const seenNames = new Set();

        rows.forEach(({ nameInput, accessionInput }) => {
            nameInput.classList.remove('input-invalid');
            accessionInput.classList.remove('input-invalid');
            delete nameInput.dataset.customError;
            delete accessionInput.dataset.customError;
        });

        for (let index = 0; index < rows.length; index += 1) {
            const { nameInput, accessionInput } = rows[index];
            const name = nameInput.value.trim();
            const accessions = this.parseBatchAnalysisAccessions(accessionInput.value);

            if (!name && accessions.length === 0) {
                continue;
            }

            if (!name) {
                this.markBatchAnalysisInputError(nameInput, `Group ${index + 1} name is required.`);
                throw new Error(`Group ${index + 1} name is required.`);
            }

            if (name.length > this.maxGroupNameLength) {
                this.markBatchAnalysisInputError(nameInput, `Group "${name}" exceeds ${this.maxGroupNameLength} characters.`);
                throw new Error(`Group "${name}" exceeds ${this.maxGroupNameLength} characters.`);
            }

            if (!/^[A-Za-z0-9_.-]+$/.test(name)) {
                this.markBatchAnalysisInputError(nameInput, `Group "${name}" may contain only letters, numbers, "_", "-", and ".".`);
                throw new Error(`Group "${name}" may contain only letters, numbers, "_", "-", and ".".`);
            }

            const canonicalName = name.toLowerCase();
            if (seenNames.has(canonicalName)) {
                this.markBatchAnalysisInputError(nameInput, `Group name "${name}" must be unique.`);
                throw new Error(`Group name "${name}" must be unique.`);
            }
            seenNames.add(canonicalName);

            if (accessions.length === 0) {
                this.markBatchAnalysisInputError(accessionInput, `Group "${name}" must contain at least one UniProt accession.`);
                throw new Error(`Group "${name}" must contain at least one UniProt accession.`);
            }

            groups.push({
                name,
                accessions: accessionInput.value,
            });
        }

        if (groups.length === 0) {
            throw new Error('Add at least one group before saving.');
        }

        return groups;
    },

    setBatchAnalysisStatus(message = '', isError = false) {
        if (!this.batchAnalysisStatus) return;
        this.batchAnalysisStatus.textContent = message;
        this.batchAnalysisStatus.classList.toggle('error', isError);
    },

    async saveBatchAnalysisGroups() {
        const groups = this.validateBatchAnalysisGroups();
        if (this.batchAnalysisSaveButton) {
            this.batchAnalysisSaveButton.disabled = true;
        }
        this.setBatchAnalysisStatus('Saving grouped exports…');

        try {
            const response = await fetch('/api/networks/group-exports', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ groups }),
            });

            const responseText = await response.text();
            let result = null;
            if (responseText) {
                try {
                    result = JSON.parse(responseText);
                } catch (err) {
                    const looksLikeHtml = /^\s*</.test(responseText);
                    if (looksLikeHtml) {
                        throw new Error('Grouped export save endpoint returned HTML instead of JSON. Restart the server and try again.');
                    }
                    throw new Error('Grouped export save returned an invalid response.');
                }
            }

            if (!response.ok) {
                throw new Error(
                    (result && (result.error || result.message)) ||
                    `Failed to save grouped exports (${response.status})`
                );
            }

            this.setBatchAnalysisStatus(
                `Saved ${result.savedFiles.length} group file${result.savedFiles.length === 1 ? '' : 's'} to ${result.exportDir}.`
            );
        } finally {
            if (this.batchAnalysisSaveButton) {
                this.batchAnalysisSaveButton.disabled = false;
            }
        }
    },

    makeDraggable(el) {
        const header = el.querySelector('.panel-header');
        let dragging = false;
        let startX = 0;
        let startY = 0;
        let initialLeft = 0;
        let initialTop = 0;

        header.addEventListener('mousedown', event => {
            dragging = true;
            startX = event.clientX;
            startY = event.clientY;
            const rect = el.getBoundingClientRect();
            initialLeft = rect.left;
            initialTop = rect.top;
            el.style.transform = 'none';
            el.style.left = `${initialLeft}px`;
            el.style.top = `${initialTop}px`;
            el.classList.add('dragging');
        });

        document.addEventListener('mousemove', event => {
            if (!dragging) return;
            el.style.left = `${initialLeft + event.clientX - startX}px`;
            el.style.top = `${initialTop + event.clientY - startY}px`;
        });

        document.addEventListener('mouseup', () => {
            if (!dragging) return;
            dragging = false;
            el.classList.remove('dragging');
        });
    },

    async downloadSection(sectionId, titleBase) {
        const section = document.getElementById(sectionId);
        if (!section) return;

        const codeBlock = section.querySelector('.export-block');
        const content = codeBlock.textContent;
        const defaultName = `${titleBase.toLowerCase()}_export.csv`;

        try {
            if (window.showSaveFilePicker) {
                const handle = await window.showSaveFilePicker({
                    suggestedName: defaultName,
                    types: [{
                        description: 'CSV File',
                        accept: { 'text/csv': ['.csv'] },
                    }],
                });
                const writable = await handle.createWritable();
                await writable.write(content);
                await writable.close();
            } else {
                // Fallback
                const filename = prompt("Enter filename:", defaultName);
                if (filename) {
                    const blob = new Blob([content], { type: 'text/csv;charset=utf-8;' });
                    const url = URL.createObjectURL(blob);

                    const link = document.createElement("a");
                    link.setAttribute("href", url);
                    link.setAttribute("download", filename);
                    link.style.visibility = 'hidden';
                    document.body.appendChild(link);
                    link.click();
                    document.body.removeChild(link);
                }
            }
        } catch (err) {
            console.error("Download failed or cancelled:", err);
        }
    }
};

// Export
if (typeof module !== 'undefined' && module.exports) {
    module.exports = ExportPanelModule;
} else {
    window.ExportPanelModule = ExportPanelModule;
}
