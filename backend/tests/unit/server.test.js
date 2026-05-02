const fs = require('fs');
const path = require('path');

// Mock the config module before requiring the server
jest.mock('../../config/config', () => ({
  port: 0,
  dataPath: '/mock/data/path',
  indexesPath: '/mock/data/indexes',
  nodeAttributesPath: '/mock/node/attributes',
  exportsPath: '/mock/data/exports',
  subnetwork: {
    pythonCommand: 'python3',
    scriptPath: '/mock/tools/extract_subnetwork.py',
    jobTempPath: '/mock/data/tmp/subnetwork-jobs',
    maxSeedCount: 10,
    maxInputLength: 2000,
    maxNameLength: 80,
    defaultMaxNodes: 500,
    maxNodesLimit: 2500,
    timeoutMs: 60000,
    maxConcurrentJobs: 2,
  },
  logging: {
    level: 'error',
    file: 'logs/app.log'
  }
}));

jest.mock('../../utils/logger', () => ({
  info: jest.fn(),
  warn: jest.fn(),
  error: jest.fn(),
  debug: jest.fn()
}));

jest.mock('../../controllers/uniprotController', () => ({
  getProteinData: jest.fn(),
  getBatchProteinData: jest.fn(),
  getProteinFields: jest.fn(),
  getBatchProteinAvailability: jest.fn(),
  getProteinsBySpecies: jest.fn(),
}));

jest.mock('../../controllers/subnetworkController', () => ({
  getSubnetworkLimits: jest.fn(),
  createSubnetwork: jest.fn()
}));

jest.mock('../../controllers/networkController', () => ({
  listNetworks: jest.fn().mockResolvedValue(['network1.csv', 'network2.json']),
  getNetworkData: jest.fn(),
  searchProteins: jest.fn(),
  searchBySpecies: jest.fn(),
  saveGroupExports: jest.fn(),
}));

describe('Server Tests', () => {
  let app;

  beforeAll(() => {
    process.env.NODE_ENV = 'test';
    jest.resetModules();
    app = require('../../server');
  });

  afterAll(() => {
    jest.resetModules();
  });

  test('Server exports an Express app', () => {
    expect(app).toBeDefined();
    expect(typeof app.use).toBe('function');
    expect(typeof app.listen).toBe('function');
  });

  test('Frontend landing page exists for static serving', () => {
    const landingPage = path.resolve(__dirname, '../../../frontend/index.html');
    const viewerPage = path.resolve(__dirname, '../../../frontend/viewer.html');

    expect(fs.existsSync(landingPage)).toBe(true);
    expect(fs.existsSync(viewerPage)).toBe(true);
  });

  test('App configures static middleware', () => {
    const hasStaticMiddleware = app._router.stack.some(
      layer => layer.name === 'serveStatic'
    );

    expect(hasStaticMiddleware).toBe(true);
  });

  test('App mounts API routers', () => {
    const routerLayers = app._router.stack.filter(layer => layer.name === 'router');
    expect(routerLayers.length).toBeGreaterThanOrEqual(4);

    const mountedPrefixes = routerLayers.map(layer => String(layer.regexp));
    expect(mountedPrefixes.some(prefix => prefix.includes('\\/api\\/networks'))).toBe(true);
    expect(mountedPrefixes.some(prefix => prefix.includes('\\/api\\/subnetworks'))).toBe(true);
  });
});
