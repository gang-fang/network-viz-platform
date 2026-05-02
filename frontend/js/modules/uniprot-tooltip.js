/**
 * UniProt Tooltip Module
 * Displays UniProt annotation when hovering over protein and cluster nodes.
 */
class UniProtNotFound extends Error {
    constructor(acc) {
        super(`Accession not found in UniProtKB: ${acc}`);
        this.name = 'UniProtNotFound';
    }
}

const UniprotTooltipModule = {
    id: "uniprot-tooltip",
    cache: new Map(), // Cache for UniProt data
    tooltipElement: null,
    activeHoverKey: null,
    hoverRequestToken: 0,
    inflightSummaryRequests: new Map(),

    init(context) {
        this.context = context;

        // Create tooltip element
        this.createTooltipElement();

        // Listen for node hover events
        context.on('nodeHover', (event) => this.handleNodeHover(event));
    },

    createTooltipElement() {
        if (document.getElementById('uniprot-tooltip')) return;

        const tooltip = document.createElement('div');
        tooltip.id = 'uniprot-tooltip';
        tooltip.className = 'uniprot-tooltip hidden';
        document.body.appendChild(tooltip);
        this.tooltipElement = tooltip;
    },

    handleNodeHover(event) {
        const { nodeId, data, x, y, type } = event;
        const hoverKey = `${data._isCluster ? 'cluster' : 'protein'}:${nodeId}`;

        if (type === 'mouseout') {
            this.activeHoverKey = null;
            this.hoverRequestToken += 1;
            this.hideTooltip();
            return;
        }

        this.showTooltip(x, y);

        if (type === 'mousemove' && this.activeHoverKey === hoverKey) {
            return;
        }

        this.activeHoverKey = hoverKey;
        const requestToken = ++this.hoverRequestToken;

        if (data._isCluster) {
            this.renderLoading("Cluster Info");
            this.processClusterHover(nodeId, hoverKey, requestToken);
            return;
        }

        // Check cache
        if (this.cache.has(nodeId)) {
            this.renderContent(this.cache.get(nodeId));
        } else {
            this.renderLoading(nodeId);
            this.fetchUniProtSummary(nodeId)
                .then(info => {
                    this.cache.set(nodeId, info);
                    if (this.shouldRenderHoverResult(hoverKey, requestToken)) {
                        this.renderContent(info);
                    }
                })
                .catch(err => {
                    const isNetworkError = !(err instanceof UniProtNotFound);
                    const errorInfo = {
                        accession: nodeId,
                        error: isNetworkError ? 'UniProtKB Request Failed' : null,
                        isNetworkError: isNetworkError
                    };
                    this.cache.set(nodeId, errorInfo);
                    if (this.shouldRenderHoverResult(hoverKey, requestToken)) {
                        this.renderContent(errorInfo);
                    }
                });
        }
    },

    shouldRenderHoverResult(hoverKey, requestToken) {
        return (
            this.activeHoverKey === hoverKey &&
            this.hoverRequestToken === requestToken &&
            this.tooltipElement &&
            !this.tooltipElement.classList.contains('hidden')
        );
    },

    showTooltip(x, y) {
        if (!this.tooltipElement) return;
        this.tooltipElement.classList.remove('hidden');
        this.tooltipElement.style.left = `${x + 15}px`;
        this.tooltipElement.style.top = `${y + 15}px`;
    },

    hideTooltip() {
        if (this.tooltipElement) {
            this.tooltipElement.classList.add('hidden');
        }
    },

    renderLoading(acc) {
        this.tooltipElement.innerHTML = `
            <div class="tooltip-loading">Loading ${acc}...</div>
        `;
    },

    renderContent(info) {
        let row1 = '';
        let row2 = '';

        if (info.error) {
            // Network Error
            row1 = `<div class="tooltip-row tooltip-acc">${info.accession}</div>`;
            row2 = `<div class="tooltip-row tooltip-error">${info.error}</div>`;
        } else if (!info.protein_name && !info.organism_name && !info.isNetworkError) {
            // Obsolete / No Data (No Network Error)
            // Note: If fetchUniProtSummary returns success but nulls, it falls here too.
            // But we also handle the caught UniProtNotFound which sets error=null.
            row1 = `<div class="tooltip-row tooltip-acc">${info.accession}</div>`;
            row2 = ''; // Blank
        } else {
            // Normal Case
            const namePart = info.protein_name ? `: <span class="tooltip-name-text">${info.protein_name}</span>` : '';
            row1 = `<div class="tooltip-row tooltip-name"><span class="tooltip-acc">${info.accession}</span>${namePart}</div>`;
            row2 = info.organism_name ? `<div class="tooltip-row tooltip-organism">${info.organism_name}</div>` : '';
        }

        this.tooltipElement.innerHTML = row1 + row2;
    },

    async fetchUniProtSummary(acc) {
        if (this.inflightSummaryRequests.has(acc)) {
            return this.inflightSummaryRequests.get(acc);
        }

        const request = this._fetchUniProt(acc, 'protein_name,organism_name')
            .then(entry => {
                // protein_name: recommended full name if present
                const proteinDescription = entry.proteinDescription || {};
                const recommendedName = proteinDescription.recommendedName || {};
                const fullName = recommendedName.fullName || {};
                const proteinName = fullName.value || null;

                // organism_name: scientific name
                const organism = entry.organism || {};
                const organismName = organism.scientificName || null;

                return {
                    accession: acc,
                    protein_name: proteinName,
                    organism_name: organismName,
                };
            })
            .finally(() => {
                this.inflightSummaryRequests.delete(acc);
            });

        this.inflightSummaryRequests.set(acc, request);
        return request;
    },

    async fetchUniProtClusterAnnotation(acc) {
        const entry = await this._fetchUniProt(acc, 'protein_name,organism_name,cc_function');
        // protein_name: recommended full name if present
        const proteinDescription = entry.proteinDescription || {};
        const recommendedName = proteinDescription.recommendedName || {};
        const fullName = recommendedName.fullName || {};
        const proteinName = fullName.value || null;

        // organism_name: scientific name
        const organism = entry.organism || {};
        const organismName = organism.scientificName || null;

        let ccFunction = null;
        if (Array.isArray(entry.comments)) {
            const functionTexts = entry.comments
                .filter(comment => comment.commentType === 'FUNCTION')
                .flatMap(comment => comment.texts || [])
                .map(text => (text.value || '').trim())
                .filter(Boolean);

            if (functionTexts.length > 0) {
                ccFunction = functionTexts.join(' ');
            }
        }

        return {
            accession: acc,
            protein_name: proteinName,
            organism_name: organismName,
            cc_function: ccFunction,
        };
    },

    async processClusterHover(clusterId, hoverKey, requestToken) {
        const members = this.context.getVisibleClusterMembers(clusterId);
        if (members.length === 0) {
            if (this.shouldRenderHoverResult(hoverKey, requestToken)) {
                this.renderClusterContent({ error: "Empty Cluster" });
            }
            return;
        }

        // Fisher-Yates Shuffle for better randomness
        const shuffled = [...members];
        for (let i = shuffled.length - 1; i > 0; i--) {
            const j = Math.floor(Math.random() * (i + 1));
            [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
        }

        const maxAttempts = Math.min(5, shuffled.length);

        let networkErrors = 0;
        for (let i = 0; i < maxAttempts; i++) {
            const acc = shuffled[i];
            try {
                const info = await this.fetchUniProtClusterAnnotation(acc);

                if (info.cc_function || info.protein_name || info.organism_name) {
                    if (this.shouldRenderHoverResult(hoverKey, requestToken)) {
                        this.renderClusterContent(info);
                    }
                    return;
                }
            } catch (err) {
                console.warn(`[UniprotTooltip] Error fetching ${acc}:`, err);
                if (!(err instanceof UniProtNotFound)) {
                    networkErrors++;
                }
            }
        }

        // If we reached here, all attempts failed
        let finalError = "Poorly Annotated";
        if (networkErrors > 0) {
            finalError = "UniProtKB Request Failed";
        }

        const errorResult = { error: finalError };
        if (this.shouldRenderHoverResult(hoverKey, requestToken)) {
            this.renderClusterContent(errorResult);
        }
    },

    renderClusterContent(info) {
        if (info.error) {
            this.tooltipElement.innerHTML = `
                <div class="tooltip-row tooltip-error">${info.error}</div>
            `;
        } else {
            const rows = [];
            const namePart = info.protein_name ? `: <span class="tooltip-name-text">${info.protein_name}</span>` : '';
            rows.push(`<div class="tooltip-row tooltip-name"><span class="tooltip-acc">${info.accession}</span>${namePart}</div>`);

            if (info.organism_name) {
                rows.push(`<div class="tooltip-row tooltip-organism">${info.organism_name}</div>`);
            }

            if (info.cc_function) {
                rows.push(`<div class="tooltip-row tooltip-function">${info.cc_function}</div>`);
            }

            this.tooltipElement.innerHTML = rows.join('');
        }
    },

    async _fetchUniProt(acc, fields) {
        const url = 'https://rest.uniprot.org/uniprotkb/search';
        const params = new URLSearchParams({
            query: `accession:${acc}`,
            fields: fields,
            format: 'json',
            size: '1',
        });

        const res = await fetch(`${url}?${params.toString()}`);
        if (!res.ok) {
            throw new Error(`UniProt request failed with status ${res.status}`);
        }

        const data = await res.json();
        const results = data.results || [];

        if (results.length === 0) {
            throw new UniProtNotFound(acc);
        }

        return results[0];
    }
};

// Export
if (typeof module !== 'undefined' && module.exports) {
    module.exports = UniprotTooltipModule;
} else {
    window.UniprotTooltipModule = UniprotTooltipModule;
}
