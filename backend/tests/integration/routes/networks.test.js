// Route handler tests for backend/routes/networks.js

const mockNetworkController = {
  listNetworks: jest.fn(),
  getNetworkData: jest.fn(),
  searchProteins: jest.fn(),
  searchBySpecies: jest.fn(),
  createEditedNetwork: jest.fn(),
};

jest.mock('../../../controllers/networkController', () => mockNetworkController);

jest.mock('../../../utils/logger', () => ({
  info: jest.fn(),
  warn: jest.fn(),
  error: jest.fn(),
  debug: jest.fn()
}));

const router = require('../../../routes/networks');

function getRouteHandler(path, method) {
  const layer = router.stack.find(
    entry => entry.route && entry.route.path === path && entry.route.methods[method]
  );

  if (!layer) {
    throw new Error(`Route not found: ${method.toUpperCase()} ${path}`);
  }

  return layer.route.stack[0].handle;
}

function createRes() {
  return {
    statusCode: 200,
    body: undefined,
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(payload) {
      this.body = payload;
      return this;
    },
  };
}

describe('Network route handlers', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('GET /', () => {
    test('returns a list of network files', async () => {
      const handler = getRouteHandler('/', 'get');
      const req = {};
      const res = createRes();
      const next = jest.fn();

      mockNetworkController.listNetworks.mockResolvedValue(['network1.csv', 'network2.json']);

      await handler(req, res, next);

      expect(res.statusCode).toBe(200);
      expect(res.body).toEqual(['network1.csv', 'network2.json']);
      expect(next).not.toHaveBeenCalled();
    });

    test('passes unexpected list errors to next', async () => {
      const handler = getRouteHandler('/', 'get');
      const req = {};
      const res = createRes();
      const next = jest.fn();
      const error = new Error('Failed to list network files');

      mockNetworkController.listNetworks.mockRejectedValue(error);

      await handler(req, res, next);

      expect(next).toHaveBeenCalledWith(error);
    });
  });

  describe('GET /:filename', () => {
    test('returns network data for a valid network source', async () => {
      const handler = getRouteHandler('/:filename', 'get');
      const req = { params: { filename: 'network.csv' } };
      const res = createRes();
      const next = jest.fn();

      mockNetworkController.getNetworkData.mockResolvedValue({
        elements: {
          nodes: [{ data: { id: 'node1' } }],
          edges: [{ data: { id: 'edge1' } }],
        }
      });

      await handler(req, res, next);

      expect(res.statusCode).toBe(200);
      expect(res.body.elements.nodes).toHaveLength(1);
      expect(next).not.toHaveBeenCalled();
    });

    test('maps not-found errors to 404', async () => {
      const handler = getRouteHandler('/:filename', 'get');
      const req = { params: { filename: 'missing.csv' } };
      const res = createRes();
      const next = jest.fn();

      mockNetworkController.getNetworkData.mockRejectedValue(new Error('not found'));

      await handler(req, res, next);

      expect(res.statusCode).toBe(404);
      expect(res.body.error).toBe('Network not found');
      expect(next).not.toHaveBeenCalled();
    });
  });

  describe('POST /search', () => {
    test('rejects malformed accession searches before calling the controller', async () => {
      const handler = getRouteHandler('/search', 'post');
      const req = { body: { network: 'network.csv', accessions: 'P001' } };
      const res = createRes();

      await handler(req, res);

      expect(res.statusCode).toBe(400);
      expect(res.body.error).toBe('accessions must be an array');
      expect(mockNetworkController.searchProteins).not.toHaveBeenCalled();
    });

    test('passes valid accession searches to the controller', async () => {
      const handler = getRouteHandler('/search', 'post');
      const req = { body: { network: 'network.csv', accessions: ['P001'] } };
      const res = createRes();

      mockNetworkController.searchProteins.mockResolvedValue({ matches: [{ id: 'P001' }] });

      await handler(req, res);

      expect(res.statusCode).toBe(200);
      expect(res.body.matches).toEqual([{ id: 'P001' }]);
      expect(mockNetworkController.searchProteins).toHaveBeenCalledWith('network.csv', ['P001']);
    });
  });

  describe('POST /search-species', () => {
    test('rejects malformed species searches before calling the controller', async () => {
      const handler = getRouteHandler('/search-species', 'post');
      const req = { body: { network: 'network.csv', speciesIds: [9606] } };
      const res = createRes();

      await handler(req, res);

      expect(res.statusCode).toBe(400);
      expect(res.body.error).toBe('speciesIds must contain only non-empty strings');
      expect(mockNetworkController.searchBySpecies).not.toHaveBeenCalled();
    });

    test('passes valid species searches to the controller', async () => {
      const handler = getRouteHandler('/search-species', 'post');
      const req = { body: { network: 'network.csv', speciesIds: ['9606'] } };
      const res = createRes();

      mockNetworkController.searchBySpecies.mockResolvedValue({
        matches: [{ id: 'P001', nh_id: 'NH001' }]
      });

      await handler(req, res);

      expect(res.statusCode).toBe(200);
      expect(res.body.matches).toEqual([{ id: 'P001', nh_id: 'NH001' }]);
      expect(mockNetworkController.searchBySpecies).toHaveBeenCalledWith('network.csv', ['9606']);
    });
  });

  describe('POST /edited', () => {
    test('rejects malformed edited-network save requests before calling the controller', async () => {
      const handler = getRouteHandler('/edited', 'post');
      const req = { body: { source: 'network.csv', name: 'edited', hiddenNodeIds: [123] } };
      const res = createRes();
      const next = jest.fn();

      await handler(req, res, next);

      expect(res.statusCode).toBe(400);
      expect(res.body.error).toBe('hiddenNodeIds must contain only non-empty strings');
      expect(mockNetworkController.createEditedNetwork).not.toHaveBeenCalled();
      expect(next).not.toHaveBeenCalled();
    });

    test('rejects malformed edited-network hidden edge IDs before calling the controller', async () => {
      const handler = getRouteHandler('/edited', 'post');
      const req = { body: { source: 'network.csv', name: 'edited', hiddenNodeIds: [], hiddenEdgeIds: [123] } };
      const res = createRes();
      const next = jest.fn();

      await handler(req, res, next);

      expect(res.statusCode).toBe(400);
      expect(res.body.error).toBe('hiddenEdgeIds must contain only non-empty strings');
      expect(mockNetworkController.createEditedNetwork).not.toHaveBeenCalled();
      expect(next).not.toHaveBeenCalled();
    });

    test('saves valid edited-network requests through the controller', async () => {
      const handler = getRouteHandler('/edited', 'post');
      const req = {
        body: {
          source: 'network.csv',
          name: 'edited',
          hiddenNodeIds: ['P002'],
          hiddenEdgeIds: ['P001|P003'],
          hiddenEdgeWeightRanges: [{ min: 0, max: 0.25 }]
        }
      };
      const res = createRes();
      const next = jest.fn();

      mockNetworkController.createEditedNetwork.mockResolvedValue({
        network: 'edited.csv',
        viewerUrl: '/viewer.html?network=edited.csv',
        nodeCount: 2,
        edgeCount: 1,
      });

      await handler(req, res, next);

      expect(res.statusCode).toBe(201);
      expect(res.body.network).toBe('edited.csv');
      expect(mockNetworkController.createEditedNetwork).toHaveBeenCalledWith({
        source: 'network.csv',
        name: 'edited',
        hiddenNodeIds: ['P002'],
        hiddenEdgeIds: ['P001|P003'],
        hiddenEdgeWeightRanges: [{ min: 0, max: 0.25 }],
      });
      expect(next).not.toHaveBeenCalled();
    });
  });
});
