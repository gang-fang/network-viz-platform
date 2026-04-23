// Unit tests for the server

const request = require('supertest');
let server;
let app;

// Mock the config module before requiring the server
jest.mock('../../config/config', () => ({
  port: 0, // Use port 0 to let the OS assign a random available port
  dataPath: '/mock/data/path',
  nodeAttributesPath: '/mock/node/attributes',
  logging: {
    level: 'error',
    file: 'logs/app.log'
  }
}));

// Also mock the logger to avoid file system operations
jest.mock('../../utils/logger', () => ({
  info: jest.fn(),
  warn: jest.fn(),
  error: jest.fn(),
  debug: jest.fn()
}));

// Mock the uniprotController to avoid axios dependency
jest.mock('../../controllers/uniprotController', () => ({
  getProteinData: jest.fn().mockImplementation((id) => {
    return Promise.resolve({
      accession: id,
      name: 'Test Protein',
      organism: 'Homo sapiens',
      function: 'Test protein function'
    });
  })
}));

// Mock the network controller to return expected data
jest.mock('../../controllers/networkController', () => ({
  listNetworks: jest.fn().mockResolvedValue(['network1.csv', 'network2.json']),
  getNetworkData: jest.fn().mockImplementation((filename) => {
    return Promise.resolve({
      elements: {
        nodes: [
          { data: { id: 'node1', name: 'Node 1' } },
          { data: { id: 'node2', name: 'Node 2' } }
        ],
        edges: [
          { data: { id: 'edge1', source: 'node1', target: 'node2', weight: 0.5 } }
        ]
      }
    });
  })
}));

describe('Server Tests', () => {
  let serverPort;

  beforeAll(async () => {
    // Set environment for testing
    process.env.NODE_ENV = 'test';
    
    // Clear any existing servers/modules
    jest.resetModules();
    
    try {
      // Import the Express app without starting the server
      app = require('../../server');
      
      // Create a new server instance with OS-assigned port
      server = app.listen(0); // Port 0 means let the OS assign a free port
      
      // Wait for server to start and get the port
      await new Promise((resolve) => {
        server.once('listening', () => {
          serverPort = server.address().port;
          console.log(`Test server started on port ${serverPort}`);
          resolve();
        });
      });
    } catch (error) {
      console.error('Error starting server:', error);
      throw error;
    }
  }, 30000); // Reduce timeout to catch issues faster

  afterAll(async () => {
    // Close server and cleanup after all tests
    if (server) {
      await new Promise((resolve) => {
        server.close(() => {
          console.log('Test server closed');
          resolve();
        });
      });
    }
    
    // Reset modules to clean state
    jest.resetModules();
  }, 10000);

  describe('Basic Server Functionality', () => {
    test('Server should be running', () => {
      expect(server).toBeDefined();
      expect(server.listening).toBe(true);
    });

    test('GET / should return 200 OK', async () => {
      const response = await request(server).get('/');
      expect(response.status).toBe(200);
      expect(response.text).toBe('Network Visualization Platform API');
    });

    test('Non-existent route should return 404', async () => {
      const response = await request(server).get('/non-existent-route');
      expect(response.status).toBe(404);
    });
  });

  describe('Network Routes', () => {
    test('GET /api/networks should return 200 OK', async () => {
      const response = await request(server).get('/api/networks');
      expect(response.status).toBe(200);
      expect(Array.isArray(response.body)).toBe(true);
    });

  });
});