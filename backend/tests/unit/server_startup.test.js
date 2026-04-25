// Test script to verify server startup functionality

const path = require('path');
const fs = require('fs');

// Mock dependencies to avoid real network/server activity
jest.mock('../../config/config', () => ({
  port: 3002,
  dataPath: '/mock/data/path',
  nodeAttributesPath: '/mock/node/attributes',
  logging: {
    level: 'error',
    file: 'logs/app.log'
  }
}));

// Mock the logger
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

jest.mock('../../controllers/subnetworkController', () => ({
  getSubnetworkLimits: jest.fn(),
  createSubnetwork: jest.fn()
}));

describe('Server Startup Tests', () => {
  let app;
  let server;
  let originalConsoleLog;
  let originalConsoleError;
  
  beforeAll(() => {
    // Suppress console output during tests
    originalConsoleLog = console.log;
    originalConsoleError = console.error;
    console.log = jest.fn();
    console.error = jest.fn();
    
    // Set environment for testing
    process.env.NODE_ENV = 'test';
  });
  
  afterAll(() => {
    // Restore console functions
    console.log = originalConsoleLog;
    console.error = originalConsoleError;
    
    // Close server if it's running
    if (server && server.listening) {
      server.close();
    }
  });
  
  test('Server file should exist', () => {
    const serverPath = path.resolve(__dirname, '../../server.js');
    expect(fs.existsSync(serverPath)).toBe(true);
  });
  
  test('Server should start without errors', () => {
    // Import the Express app without starting the server
    app = require('../../server');
    expect(app).toBeDefined();
    
    // Check that app has expected Express properties
    expect(app.listen).toBeDefined();
    expect(typeof app.listen).toBe('function');
  });
  
  test('Server should load configuration correctly', () => {
    // Verify config is loaded
    const config = require('../../config/config');
    expect(config).toBeDefined();
    expect(config.port).toBeDefined();
    expect(typeof config.port).toBe('number');
  });
  
  test('Server should have required routes configured', () => {
    // Verify routes are configured
    const routes = app._router.stack
      .filter(layer => layer.route)
      .map(layer => layer.route.path);
    
    // Check that we have some routes
    expect(routes.length).toBeGreaterThan(0);
    
    // Check API routes by examining middleware
    const apiRoutes = app._router.stack
      .filter(layer => layer.name === 'router')
      .map(layer => String(layer.regexp));

    expect(apiRoutes.some(prefix => prefix.includes('\\/api\\/networks'))).toBe(true);
    expect(apiRoutes.some(prefix => prefix.includes('\\/api\\/subnetworks'))).toBe(true);

    const hasStaticMiddleware = app._router.stack.some(
      layer => layer.name === 'serveStatic'
    );
    expect(hasStaticMiddleware).toBe(true);
  });
});
