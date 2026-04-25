/**
 * Export Panel Module
 * Handles the "Export Mode" logic and rendering of exported data to the right panel.
 */
const ExportPanelModule = {
    id: "export-panel",

    init(context) {
        this.context = context;

        this.selectModeBtn = document.getElementById('select-mode-btn');
        this.exportBtn = document.getElementById('export-btn');
        this.rightPanelContent = document.getElementById('export-content');

        // Clear placeholder if it exists
        if (this.rightPanelContent.querySelector('.placeholder-text')) {
            this.rightPanelContent.innerHTML = '';
        }

        this.setupListeners();
    },

    setupListeners() {
        // Button Clicks
        this.selectModeBtn.addEventListener('click', () => {
            const newMode = !this.context.isSelectionModeEnabled();
            this.context.setSelectionMode(newMode);
        });

        this.exportBtn.addEventListener('click', () => {
            this.exportData();
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
        if (this.context.isSelectionModeEnabled() && selectedIds && selectedIds.length > 0) {
            this.exportBtn.disabled = false;
        } else {
            this.exportBtn.disabled = true;
        }
    },

    exportData() {
        const selectedIds = this.context.getSelectedNodeIds();
        if (selectedIds.length === 0) return;

        // Use viewGraph because it contains the currently visible nodes (including Clusters)
        const graph = this.context.getViewGraph();
        const nhNodes = [];
        const proteinNodes = [];

        selectedIds.forEach(id => {
            const node = graph.nodes.get(id);
            if (node) {
                // Check for cluster property
                if (node._isCluster || node.kind === 'cluster' || node.type === 'cluster') {
                    nhNodes.push(node);
                } else {
                    proteinNodes.push(node);
                }
            }
        });

        this.processExport(nhNodes, proteinNodes);
    },

    processExport(nhNodes, proteinNodes) {
        // 1. NH Nodes
        if (nhNodes.length > 0) {
            const sectionId = 'nh-section';
            const existingIds = this.getExistingIds(sectionId, 'NH');
            let content = '';
            let addedCount = 0;

            nhNodes.forEach(nh => {
                if (!existingIds.has(nh.id)) {
                    const members = this.context.getVisibleClusterMembers
                        ? this.context.getVisibleClusterMembers(nh.id)
                        : this.context.getGraph().getClusterMembers(nh.id);
                    // CSV Format: ID,"ACs"
                    content += `${nh.id},"${members.join(', ')}"\n`;
                    addedCount++;
                    existingIds.add(nh.id); // Add to set to prevent duplicates within this batch too
                }
            });

            if (addedCount > 0) {
                this.updateSection(sectionId, 'Neighborhoods', content, 'Neighborhood_ID,Neighbors\n', addedCount);
            }
        }

        // 2. Protein Nodes
        if (proteinNodes.length > 0) {
            const sectionId = 'protein-section';
            const existingIds = this.getExistingIds(sectionId, 'Protein');
            let newAcs = [];

            proteinNodes.forEach(n => {
                if (!existingIds.has(n.id)) {
                    newAcs.push(n.id);
                    existingIds.add(n.id);
                }
            });

            if (newAcs.length > 0) {
                let content = '';
                // Comma separated, 5 per line
                for (let i = 0; i < newAcs.length; i += 5) {
                    const chunk = newAcs.slice(i, i + 5);
                    content += chunk.join(', ') + (i + 5 < newAcs.length ? ',\n' : '');
                }
                // Ensure trailing newline if not present
                if (content && !content.endsWith('\n')) content += '\n';

                // If appending to existing content that doesn't end with newline/comma properly?
                // Our previous logic ensures blocks end with newline.
                // But wait, if previous block ended with "AC1, AC2\n", and we append "AC3, AC4\n",
                // it becomes "AC1, AC2\nAC3, AC4\n". This is fine for readability.
                // But if we want a continuous list? The requirement says "about 5 ACs per line".
                // New batch starting on new line is acceptable and safer.

                this.updateSection(sectionId, 'Proteins', content, '', newAcs.length);
            }
        }
    },

    getExistingIds(sectionId, type) {
        const ids = new Set();
        const section = document.getElementById(sectionId);
        if (!section) return ids;

        const codeBlock = section.querySelector('.export-block');
        const text = codeBlock.textContent.trim();
        if (!text) return ids;

        if (type === 'NH') {
            // Parse lines: ID,"ACs"
            const lines = text.split('\n');
            lines.forEach(line => {
                const parts = line.split(',');
                if (parts.length > 0) {
                    const id = parts[0].trim();
                    if (id && id !== 'Neighborhood_ID') {
                        ids.add(id);
                    }
                }
            });
        } else {
            // Parse comma separated list
            // Remove newlines
            const cleanText = text.replace(/\n/g, ',');
            const parts = cleanText.split(',');
            parts.forEach(p => {
                const id = p.trim();
                if (id) ids.add(id);
            });
        }
        return ids;
    },

    updateSection(sectionId, titleBase, newContent, headerRow, addedCount) {
        let section = document.getElementById(sectionId);

        if (!section) {
            // Create new section
            section = document.createElement('div');
            section.id = sectionId;
            section.className = 'export-section';
            section.dataset.count = 0; // Track count

            // Header
            const header = document.createElement('div');
            header.className = 'section-header';

            const title = document.createElement('h4');
            title.className = 'section-title';
            // Initial title will be updated below

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

            actions.appendChild(eraser);
            actions.appendChild(download);

            header.appendChild(title);
            header.appendChild(actions);

            // Code Block
            const codeBlock = document.createElement('div');
            codeBlock.className = 'export-block';

            // Add header row if provided
            if (headerRow) {
                codeBlock.textContent = headerRow;
            }

            section.appendChild(header);
            section.appendChild(codeBlock);
            this.rightPanelContent.appendChild(section);
        }

        // Append Content
        const codeBlock = section.querySelector('.export-block');
        codeBlock.textContent += newContent;

        // Update Count and Title
        const currentCount = parseInt(section.dataset.count || 0);
        const newCount = currentCount + addedCount;
        section.dataset.count = newCount;

        const title = section.querySelector('.section-title');
        title.textContent = `${titleBase} (${newCount})`;
    },

    clearSection(sectionId) {
        const section = document.getElementById(sectionId);
        if (section) {
            section.remove();
        }
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
