describe('uniprotController', () => {
  let axios;

  beforeEach(() => {
    jest.resetModules();
    jest.doMock('../../config/config', () => ({
      uniprotApi: {
        baseUrl: 'https://example.test/uniprot/',
        timeout: 1000,
        cacheExpiry: 60000,
        batchLimit: 3,
        concurrencyLimit: 2,
        cacheMaxEntries: 100,
      },
    }));
    jest.doMock('../../utils/logger', () => ({
      info: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
      debug: jest.fn(),
    }));

    axios = { get: jest.fn() };
    jest.doMock('axios', () => axios);
  });

  test('rejects batch requests over the configured hard cap', async () => {
    const { getBatchProteinData } = require('../../controllers/uniprotController');

    await expect(getBatchProteinData(['P1', 'P2', 'P3', 'P4']))
      .rejects.toThrow('accessions is limited to 3 items');
    expect(axios.get).not.toHaveBeenCalled();
  });

  test('limits outbound UniProt request concurrency', async () => {
    const { getBatchProteinData } = require('../../controllers/uniprotController');
    let active = 0;
    let maxActive = 0;

    axios.get.mockImplementation(async (url) => {
      active++;
      maxActive = Math.max(maxActive, active);
      await new Promise(resolve => setTimeout(resolve, 5));
      active--;
      return { data: { url } };
    });

    const result = await getBatchProteinData(['P1', 'P2', 'P3']);

    expect(result).toHaveLength(3);
    expect(maxActive).toBeLessThanOrEqual(2);
    expect(axios.get).toHaveBeenCalledTimes(3);
  });

  test('caches individual protein responses within the configured expiry window', async () => {
    const { getProteinData } = require('../../controllers/uniprotController');
    axios.get.mockResolvedValue({ data: { accession: 'P1' } });

    const first = await getProteinData('P1');
    const second = await getProteinData('P1');

    expect(first).toEqual({ accession: 'P1' });
    expect(second).toEqual({ accession: 'P1' });
    expect(axios.get).toHaveBeenCalledTimes(1);
  });

  test('marks UniProt availability true only for HTTP 200 responses', async () => {
    const { getBatchProteinAvailability } = require('../../controllers/uniprotController');
    axios.get
      .mockResolvedValueOnce({ status: 200 })
      .mockResolvedValueOnce({ status: 404 })
      .mockResolvedValueOnce({ status: 400 });

    const result = await getBatchProteinAvailability(['P1', 'P2', 'P3']);

    expect(result).toEqual([
      { accession: 'P1', available: true },
      { accession: 'P2', available: false },
      { accession: 'P3', available: false },
    ]);
  });

  test('caches UniProt availability checks within the configured expiry window', async () => {
    const { getProteinAvailability } = require('../../controllers/uniprotController');
    axios.get.mockResolvedValue({ status: 200 });

    const first = await getProteinAvailability('P1');
    const second = await getProteinAvailability('P1');

    expect(first).toEqual({ accession: 'P1', available: true });
    expect(second).toEqual({ accession: 'P1', available: true });
    expect(axios.get).toHaveBeenCalledTimes(1);
  });

  test('does not cache transient UniProt availability failures', async () => {
    const { getProteinAvailability } = require('../../controllers/uniprotController');
    axios.get.mockResolvedValue({ status: 503 });

    const first = await getProteinAvailability('P1');
    const second = await getProteinAvailability('P1');

    expect(first).toEqual({ accession: 'P1', available: false });
    expect(second).toEqual({ accession: 'P1', available: false });
    expect(axios.get).toHaveBeenCalledTimes(2);
  });
});
