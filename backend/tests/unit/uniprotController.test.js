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

  test('limits outbound UniProt request concurrency', async () => {
    const { getBatchProteinAvailability } = require('../../controllers/uniprotController');
    let active = 0;
    let maxActive = 0;

    axios.get.mockImplementation(async () => {
      active++;
      maxActive = Math.max(maxActive, active);
      await new Promise(resolve => setTimeout(resolve, 5));
      active--;
      return { status: 200 };
    });

    const result = await getBatchProteinAvailability(['P1', 'P2', 'P3']);

    expect(result).toHaveLength(3);
    expect(maxActive).toBeLessThanOrEqual(2);
    expect(axios.get).toHaveBeenCalledTimes(3);
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
