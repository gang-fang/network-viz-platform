const fs = require('fs');
const os = require('os');
const path = require('path');

const mockTestRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'network-edit-service-'));

const mockDb = {
  all: jest.fn(),
  run: jest.fn(),
  prepare: jest.fn(),
};

jest.mock('../../config/config', () => ({
  dataPath: require('path').join(mockTestRoot, 'data/networks'),
  tempDataPath: require('path').join(mockTestRoot, 'data/tmp'),
  networkEdit: {
    maxNameLength: 80,
    maxSuffixAttempts: 5,
    watcherSuppressMs: 30000,
  },
}));

jest.mock('../../config/database', () => mockDb);

jest.mock('../../scripts/ingestData', () => ({
  ingestNetworks: jest.fn().mockResolvedValue(undefined),
}));

jest.mock('../../utils/logger', () => ({
  info: jest.fn(),
  warn: jest.fn(),
  error: jest.fn(),
  debug: jest.fn(),
}));

jest.mock('../../services/fileWatcher', () => ({
  suppressNetworkIngest: jest.fn(),
}));

const { ingestNetworks } = require('../../scripts/ingestData');
const fileWatcher = require('../../services/fileWatcher');
const config = require('../../config/config');
const service = require('../../services/networkEditService');

function callbackOk(args) {
  const cb = args[args.length - 1];
  if (typeof cb === 'function') cb(null);
}

describe('networkEditService', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    fs.rmSync(mockTestRoot, { recursive: true, force: true });
    fs.mkdirSync(config.dataPath, { recursive: true });
    fs.mkdirSync(config.tempDataPath, { recursive: true });

    mockDb.run.mockImplementation((...args) => callbackOk(args));
    mockDb.prepare.mockReturnValue({
      run: jest.fn((...args) => callbackOk(args)),
      finalize: jest.fn(),
    });
  });

  afterAll(() => {
    fs.rmSync(mockTestRoot, { recursive: true, force: true });
  });

  test('normalizeHiddenNodeIds rejects non-string IDs', () => {
    expect(() => service.normalizeHiddenNodeIds(['P1', 123])).toThrow('hiddenNodeIds must contain only non-empty strings');
  });

  test('normalizeHiddenEdgeIds rejects non-string IDs', () => {
    expect(() => service.normalizeHiddenEdgeIds(['P1|P2', 123])).toThrow('hiddenEdgeIds must contain only non-empty strings');
  });

  test('normalizeHiddenEdgeIds canonicalizes undirected edge IDs', () => {
    expect(service.normalizeHiddenEdgeIds(['P2|P1'])).toEqual(['P1|P2']);
  });

  test('normalizeHiddenEdgeIds rejects malformed edge IDs', () => {
    expect(() => service.normalizeHiddenEdgeIds(['P1'])).toThrow('node1|node2');
    expect(() => service.normalizeHiddenEdgeIds(['P1|'])).toThrow('node1|node2');
    expect(() => service.normalizeHiddenEdgeIds(['P1|P2|P3'])).toThrow('node1|node2');
  });

  test('normalizeHiddenEdgeWeightRanges validates compact threshold ranges', () => {
    expect(service.normalizeHiddenEdgeWeightRanges([{ min: 0, max: 0.5 }])).toEqual([{ min: 0, max: 0.5 }]);
    expect(() => service.normalizeHiddenEdgeWeightRanges('bad')).toThrow('hiddenEdgeWeightRanges must be an array');
    expect(() => service.normalizeHiddenEdgeWeightRanges([null])).toThrow('range objects');
    expect(() => service.normalizeHiddenEdgeWeightRanges([{ min: -0.1, max: 0.5 }])).toThrow('0 <= min < max < 1');
    expect(() => service.normalizeHiddenEdgeWeightRanges([{ min: 0, max: 1 }])).toThrow('0 <= min < max < 1');
    expect(() => service.normalizeHiddenEdgeWeightRanges([{ min: 0.4, max: 0.4 }])).toThrow('0 <= min < max < 1');
    expect(() => service.normalizeHiddenEdgeWeightRanges(Array.from({ length: 1001 }, () => ({ min: 0, max: 0.1 })))).toThrow('limited to 1000');
  });

  test('normalizeOutputName rejects path inputs and overlong names', () => {
    expect(() => service.normalizeOutputName('../bad')).toThrow('name may contain only');
    expect(() => service.normalizeOutputName('x'.repeat(81))).toThrow('name is limited to 80 characters');
  });

  test('createEditedNetwork writes only edges whose endpoints remain visible and preserves visible node membership', async () => {
    mockDb.all.mockImplementation((sql, params, cb) => {
      if (sql.includes('SELECT node_id AS id')) {
        return cb(null, [{ id: 'P1' }, { id: 'P2' }, { id: 'P3' }]);
      }
      if (sql.includes('SELECT id, node1, node2, weight')) {
        return cb(null, [
          { node1: 'P1', node2: 'P2', weight: 0.5 },
          { node1: 'P1', node2: 'P3', weight: 0.9 },
        ]);
      }
      return cb(null, []);
    });

    const result = await service.createEditedNetwork({
      source: 'source.csv',
      name: 'edited',
      hiddenNodeIds: ['P2', 'NOT_IN_SOURCE'],
    });

    expect(result).toMatchObject({
      network: 'edited.csv',
      nodeCount: 2,
      edgeCount: 1,
      hiddenInSourceNodeCount: 1,
      submittedHiddenNodeCount: 2,
    });
    expect(fs.readFileSync(path.join(config.dataPath, 'edited.csv'), 'utf8')).toBe('P1,P3,0.9\n');
    expect(fileWatcher.suppressNetworkIngest).toHaveBeenCalledWith('edited.csv');
    expect(ingestNetworks).toHaveBeenCalledWith('edited.csv');
    expect(mockDb.run).toHaveBeenCalledWith(
      expect.stringContaining('INSERT OR IGNORE INTO network_nodes'),
      ['edited.csv', 'P1', 'edited.csv', 'P3'],
      expect.any(Function)
    );
  });

  test('createEditedNetwork excludes explicitly hidden edges while preserving visible endpoints', async () => {
    mockDb.all.mockImplementation((sql, params, cb) => {
      if (sql.includes('SELECT node_id AS id')) {
        return cb(null, [{ id: 'P1' }, { id: 'P2' }, { id: 'P3' }]);
      }
      if (sql.includes('SELECT id, node1, node2, weight')) {
        return cb(null, [
          { id: 'P1|P2', node1: 'P1', node2: 'P2', weight: 0.2 },
          { id: 'P2|P3', node1: 'P2', node2: 'P3', weight: 0.7 },
          { id: 'P1|P3', node1: 'P1', node2: 'P3', weight: 0.9 },
        ]);
      }
      return cb(null, []);
    });

    const result = await service.createEditedNetwork({
      source: 'source.csv',
      name: 'edited',
      hiddenNodeIds: [],
      hiddenEdgeIds: ['P2|P1', 'P3|P4'],
    });

    expect(result).toMatchObject({
      network: 'edited.csv',
      nodeCount: 3,
      edgeCount: 2,
      hiddenInSourceEdgeCount: 1,
      submittedHiddenEdgeCount: 2,
    });
    expect(fs.readFileSync(path.join(config.dataPath, 'edited.csv'), 'utf8')).toBe('P2,P3,0.7\nP1,P3,0.9\n');
    expect(mockDb.run).toHaveBeenCalledWith(
      expect.stringContaining('INSERT OR IGNORE INTO network_nodes'),
      ['edited.csv', 'P1', 'edited.csv', 'P2', 'edited.csv', 'P3'],
      expect.any(Function)
    );
  });

  test('createEditedNetwork expands hidden edge weight ranges on the server', async () => {
    mockDb.all.mockImplementation((sql, params, cb) => {
      if (sql.includes('SELECT node_id AS id')) {
        return cb(null, [{ id: 'P1' }, { id: 'P2' }, { id: 'P3' }, { id: 'P4' }]);
      }
      if (sql.includes('SELECT id, node1, node2, weight')) {
        return cb(null, [
          { id: 'P1|P2', node1: 'P1', node2: 'P2', weight: 0.2 },
          { id: 'P2|P3', node1: 'P2', node2: 'P3', weight: 0.4 },
          { id: 'P3|P4', node1: 'P3', node2: 'P4', weight: 0.8 },
        ]);
      }
      return cb(null, []);
    });

    const result = await service.createEditedNetwork({
      source: 'source.csv',
      name: 'edited',
      hiddenNodeIds: [],
      hiddenEdgeIds: [],
      hiddenEdgeWeightRanges: [{ min: 0, max: 0.5 }],
    });

    expect(result).toMatchObject({
      network: 'edited.csv',
      nodeCount: 4,
      edgeCount: 1,
      hiddenInSourceEdgeCount: 2,
      submittedHiddenEdgeCount: 0,
      submittedHiddenEdgeRangeCount: 1,
    });
    expect(fs.readFileSync(path.join(config.dataPath, 'edited.csv'), 'utf8')).toBe('P3,P4,0.8\n');
  });

  test('createEditedNetwork appends suffixes for existing names', async () => {
    fs.writeFileSync(path.join(config.dataPath, 'edited.csv'), 'old\n');
    fs.writeFileSync(path.join(config.dataPath, 'edited_1.csv'), 'old\n');

    mockDb.all.mockImplementation((sql, params, cb) => {
      if (sql.includes('SELECT node_id AS id')) {
        return cb(null, [{ id: 'P1' }, { id: 'P2' }]);
      }
      if (sql.includes('SELECT id, node1, node2, weight')) {
        return cb(null, [{ node1: 'P1', node2: 'P2', weight: 1 }]);
      }
      return cb(null, []);
    });

    const result = await service.createEditedNetwork({
      source: 'source.csv',
      name: 'edited',
      hiddenNodeIds: [],
    });

    expect(result.network).toBe('edited_2.csv');
    expect(fs.existsSync(path.join(config.dataPath, 'edited_2.csv'))).toBe(true);
  });

  test('createEditedNetwork rejects edits with no visible source nodes', async () => {
    mockDb.all.mockImplementation((sql, params, cb) => {
      if (sql.includes('SELECT node_id AS id')) {
        return cb(null, [{ id: 'P1' }, { id: 'P2' }]);
      }
      if (sql.includes('SELECT id, node1, node2, weight')) {
        return cb(null, []);
      }
      return cb(null, []);
    });

    await expect(service.createEditedNetwork({
      source: 'source.csv',
      name: 'empty',
      hiddenNodeIds: ['P1', 'P2'],
    })).rejects.toMatchObject({ status: 422 });
  });

  test('createEditedNetwork fails before rename when watcher suppression is unavailable', async () => {
    fileWatcher.suppressNetworkIngest.mockImplementationOnce(() => {
      throw new Error('watcher unavailable');
    });

    mockDb.all.mockImplementation((sql, params, cb) => {
      if (sql.includes('SELECT node_id AS id')) {
        return cb(null, [{ id: 'P1' }, { id: 'P2' }]);
      }
      if (sql.includes('SELECT id, node1, node2, weight')) {
        return cb(null, [{ node1: 'P1', node2: 'P2', weight: 1 }]);
      }
      return cb(null, []);
    });

    await expect(service.createEditedNetwork({
      source: 'source.csv',
      name: 'edited',
      hiddenNodeIds: [],
    })).rejects.toMatchObject({
      status: 500,
      message: expect.stringContaining('Could not suppress watcher ingest'),
    });
    expect(fs.existsSync(path.join(config.dataPath, 'edited.csv'))).toBe(false);
    expect(ingestNetworks).not.toHaveBeenCalled();
  });
});
