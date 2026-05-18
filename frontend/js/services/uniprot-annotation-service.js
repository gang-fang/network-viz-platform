class UniProtNotFound extends Error {
    constructor(acc) {
        super(`Accession not found in UniProtKB: ${acc}`);
        this.name = 'UniProtNotFound';
    }
}

const UniProtAnnotationService = {
    summaryCache: new Map(),
    annotationCache: new Map(),
    inflightSummaryRequests: new Map(),
    inflightAnnotationRequests: new Map(),

    async fetchSummary(acc) {
        const accession = String(acc);
        if (this.summaryCache.has(accession)) return this.summaryCache.get(accession);
        if (this.inflightSummaryRequests.has(accession)) {
            return this.inflightSummaryRequests.get(accession);
        }

        const request = this._fetchUniProt(accession, 'protein_name,organism_name')
            .then(entry => {
                const info = {
                    accession,
                    ...this._parseBaseAnnotation(entry),
                };
                this.summaryCache.set(accession, info);
                return info;
            })
            .finally(() => {
                this.inflightSummaryRequests.delete(accession);
            });

        this.inflightSummaryRequests.set(accession, request);
        return request;
    },

    async fetchAnnotation(acc) {
        const accession = String(acc);
        if (this.annotationCache.has(accession)) return this.annotationCache.get(accession);
        if (this.inflightAnnotationRequests.has(accession)) {
            return this.inflightAnnotationRequests.get(accession);
        }

        const request = this._fetchUniProt(accession, 'protein_name,organism_name,cc_function')
            .then(entry => {
                const info = {
                    accession,
                    ...this._parseBaseAnnotation(entry),
                    cc_function: this._parseFunctionComment(entry),
                };
                this.annotationCache.set(accession, info);
                return info;
            })
            .finally(() => {
                this.inflightAnnotationRequests.delete(accession);
            });

        this.inflightAnnotationRequests.set(accession, request);
        return request;
    },

    _parseBaseAnnotation(entry) {
        const proteinDescription = entry.proteinDescription || {};
        const recommendedName = proteinDescription.recommendedName || {};
        const fullName = recommendedName.fullName || {};
        const organism = entry.organism || {};

        return {
            protein_name: fullName.value || null,
            organism_name: organism.scientificName || null,
        };
    },

    _parseFunctionComment(entry) {
        if (!Array.isArray(entry.comments)) return null;

        const functionTexts = entry.comments
            .filter(comment => comment.commentType === 'FUNCTION')
            .flatMap(comment => comment.texts || [])
            .map(text => (text.value || '').trim())
            .filter(Boolean);

        return functionTexts.length > 0 ? functionTexts.join(' ') : null;
    },

    async _fetchUniProt(acc, fields) {
        const url = 'https://rest.uniprot.org/uniprotkb/search';
        const params = new URLSearchParams({
            query: `accession:${acc}`,
            fields,
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
    },
};

UniProtAnnotationService.UniProtNotFound = UniProtNotFound;

if (typeof module !== 'undefined' && module.exports) {
    module.exports = UniProtAnnotationService;
} else {
    window.UniProtAnnotationService = UniProtAnnotationService;
}
