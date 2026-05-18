describe('UniProtAnnotationService', () => {
  let service;

  beforeEach(() => {
    jest.resetModules();
    service = require('../../../frontend/js/services/uniprot-annotation-service');
    service.summaryCache.clear();
    service.annotationCache.clear();
    service.inflightSummaryRequests.clear();
    service.inflightAnnotationRequests.clear();
    global.fetch = jest.fn();
  });

  afterEach(() => {
    delete global.fetch;
  });

  test('fetchAnnotation parses name, organism, and function comment', async () => {
    global.fetch.mockResolvedValue({
      ok: true,
      json: async () => ({
        results: [{
          proteinDescription: {
            recommendedName: {
              fullName: { value: 'DNA repair protein RecA' },
            },
          },
          organism: {
            scientificName: 'Escherichia coli',
          },
          comments: [{
            commentType: 'FUNCTION',
            texts: [{ value: 'Catalyzes DNA strand exchange.' }],
          }],
        }],
      }),
    });

    await expect(service.fetchAnnotation('P0A7G6')).resolves.toEqual({
      accession: 'P0A7G6',
      protein_name: 'DNA repair protein RecA',
      organism_name: 'Escherichia coli',
      cc_function: 'Catalyzes DNA strand exchange.',
    });
  });

  test('fetchSummary reuses cached accession lookups', async () => {
    global.fetch.mockResolvedValue({
      ok: true,
      json: async () => ({
        results: [{
          proteinDescription: {
            recommendedName: {
              fullName: { value: 'Protein A' },
            },
          },
          organism: {
            scientificName: 'Bacillus subtilis',
          },
        }],
      }),
    });

    await service.fetchSummary('P12345');
    await service.fetchSummary('P12345');

    expect(global.fetch).toHaveBeenCalledTimes(1);
  });
});
