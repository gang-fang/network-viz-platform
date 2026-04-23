const request = require('supertest');

// Mock the networkController first, before any imports
const mockNetworkController = {
  listNetworks: jest.fn(),
  getNetworkData: jest.fn(),
  searchProteins: jest.fn(),
  searchBySpecies: jest.fn(),
};

// Now mock the module
jest.mock('../../../controllers/networkController', () => mockNetworkController);

// Mock the config module
jest.mock('../../../config/config', () => ({
  port: 3003,
  dataPath: '/mock/data/networks',
  nodeAttributesPath: '/mock/data',
  logging: {
    level: 'error',
    file: 'logs/app.log'
  }
}));

// Mock the logger
jest.mock('../../../utils/logger', () => ({
  info: jest.fn(),
  warn: jest.fn(),
  error: jest.fn(),
  debug: jest.fn()
}));

let app;

describe('Network Routes Integration Tests', () => {
  beforeAll(() => {
    // Setup test environment
    process.env.NODE_ENV = 'test';
    
    // Clear module cache to get a fresh instance
    jest.resetModules();
    
    // Import the Express app
    app = require('../../../server');
  });
  
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('GET /api/networks', () => {
    test('should return a list of network files', async () => {
      // Setup the mock to return test data
      mockNetworkController.listNetworks.mockResolvedValue(['network1.csv', 'network2.json']);
      
      const response = await request(app).get('/api/networks');
      
      // Verify the response
      expect(response.status).toBe(200);
      expect(response.body).toEqual(['network1.csv', 'network2.json']);
    });
    
    test('should handle errors when listing networks', async () => {
      // Setup the mock to throw an error
      mockNetworkController.listNetworks.mockRejectedValue(new Error('Failed to list network files'));
      
      const response = await request(app).get('/api/networks');
      
      // Verify the error response
      expect(response.status).toBe(500);
      expect(response.body.message).toBeDefined();
    });
  });
  
  describe('GET /api/networks/:filename', () => {
    test('should return network data for a valid network source', async () => {
      const mockData = {
        elements: {
          nodes: [
            { data: { id: 'node1', NCBI_txID: '9606', NH_ID: 'NH001' } },
            { data: { id: 'node2', NCBI_txID: '10090', NH_ID: 'NH002' } }
          ],
          edges: [
            { data: { id: 'edge1', source: 'node1', target: 'node2', weight: 0.5 } }
          ]
        }
      };

      mockNetworkController.getNetworkData.mockResolvedValue(mockData);

      const response = await request(app).get('/api/networks/network.csv');

      expect(response.status).toBe(200);
      expect(response.body.elements).toBeDefined();
      expect(response.body.elements.nodes).toHaveLength(2);
      expect(response.body.elements.edges).toHaveLength(1);
    });
    
    test('should handle file not found errors', async () => {
      // Create error with special property that router checks for
      const notFoundError = new Error('Failed to read network file: nonexistent.csv');
      notFoundError.message = 'not found'; // This triggers the 404 in the route handler
      
      // Setup the mock to throw the error
      mockNetworkController.getNetworkData.mockRejectedValue(notFoundError);
      
      const response = await request(app).get('/api/networks/nonexistent.csv');
      
      // Verify the error response
      expect(response.status).toBe(404);
      expect(response.body.error).toBeDefined();
    });
    
  });

  describe('POST /api/networks/search', () => {
    test('should reject malformed accession search bodies before calling the controller', async () => {
      const response = await request(app)
        .post('/api/networks/search')
        .send({ network: 'network.csv', accessions: 'P001' });

      expect(response.status).toBe(400);
      expect(response.body.error).toBe('accessions must be an array');
      expect(mockNetworkController.searchProteins).not.toHaveBeenCalled();
    });

    test('should pass valid accession searches to the controller', async () => {
      mockNetworkController.searchProteins.mockResolvedValue({ matches: [{ id: 'P001' }] });

      const response = await request(app)
        .post('/api/networks/search')
        .send({ network: 'network.csv', accessions: ['P001'] });

      expect(response.status).toBe(200);
      expect(response.body.matches).toEqual([{ id: 'P001' }]);
      expect(mockNetworkController.searchProteins).toHaveBeenCalledWith('network.csv', ['P001']);
    });
  });

  describe('POST /api/networks/search-species', () => {
    test('should reject malformed species search bodies before calling the controller', async () => {
      const response = await request(app)
        .post('/api/networks/search-species')
        .send({ network: 'network.csv', speciesIds: [9606] });

      expect(response.status).toBe(400);
      expect(response.body.error).toBe('speciesIds must contain only non-empty strings');
      expect(mockNetworkController.searchBySpecies).not.toHaveBeenCalled();
    });

    test('should pass valid species searches to the controller', async () => {
      mockNetworkController.searchBySpecies.mockResolvedValue({ matches: [{ id: 'P001', nh_id: 'NH001' }] });

      const response = await request(app)
        .post('/api/networks/search-species')
        .send({ network: 'network.csv', speciesIds: ['9606'] });

      expect(response.status).toBe(200);
      expect(response.body.matches).toEqual([{ id: 'P001', nh_id: 'NH001' }]);
      expect(mockNetworkController.searchBySpecies).toHaveBeenCalledWith('network.csv', ['9606']);
    });
  });
  
}); 
