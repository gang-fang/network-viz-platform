const fs = require('fs');
const os = require('os');
const path = require('path');
const EventEmitter = require('events');

jest.mock('child_process', () => ({
  spawn: jest.fn(),
}));

jest.mock('../../scripts/ingestData', () => ({
  ingestNetworks: jest.fn().mockResolvedValue(undefined),
}));

jest.mock('../../utils/logger', () => ({
  info: jest.fn(),
  warn: jest.fn(),
  error: jest.fn(),
  debug: jest.fn(),
}));

const mockTestRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'subnetwork-service-'));

jest.mock('../../config/config', () => ({
  dataPath: require('path').join(mockTestRoot, 'data/networks'),
  indexesPath: require('path').join(mockTestRoot, 'data/indexes'),
  subnetwork: {
    pythonCommand: 'python3',
    scriptPath: require('path').join(mockTestRoot, 'tools/extract_subnetwork.py'),
    jobTempPath: require('path').join(mockTestRoot, 'data/tmp/subnetwork-jobs'),
    maxSeedCount: 10,
    maxInputLength: 2000,
    maxNameLength: 80,
    defaultMaxNodes: 500,
    maxNodesLimit: 2000,
    timeoutMs: 1000,
    maxConcurrentJobs: 2,
  },
}));

const { spawn } = require('child_process');
const { ingestNetworks } = require('../../scripts/ingestData');
const service = require('../../services/subnetworkService');
const config = require('../../config/config');

function createMockChildProcess({ stdout = '', stderr = '', exitCode = 0, onSpawn } = {}) {
  const child = new EventEmitter();
  child.stdout = new EventEmitter();
  child.stderr = new EventEmitter();
  child.kill = jest.fn();

  process.nextTick(async () => {
    if (onSpawn) {
      await onSpawn();
    }
    if (stdout) child.stdout.emit('data', Buffer.from(stdout));
    if (stderr) child.stderr.emit('data', Buffer.from(stderr));
    child.emit('close', exitCode, null);
  });

  return child;
}

describe('subnetworkService', () => {
  let originalTimeoutMs;
  let originalMaxConcurrentJobs;

  beforeEach(() => {
    jest.clearAllMocks();
    spawn.mockReset();
    ingestNetworks.mockReset();
    ingestNetworks.mockResolvedValue(undefined);
    originalTimeoutMs = config.subnetwork.timeoutMs;
    originalMaxConcurrentJobs = config.subnetwork.maxConcurrentJobs;
    fs.rmSync(mockTestRoot, { recursive: true, force: true });
    fs.mkdirSync(config.dataPath, { recursive: true });
    fs.mkdirSync(config.indexesPath, { recursive: true });
    fs.mkdirSync(config.subnetwork.jobTempPath, { recursive: true });
    fs.mkdirSync(path.dirname(config.subnetwork.scriptPath), { recursive: true });
    fs.writeFileSync(config.subnetwork.scriptPath, '#!/usr/bin/env python3\n');
    fs.writeFileSync(path.join(config.indexesPath, 'ba.adj.bin'), 'bin');
    fs.writeFileSync(path.join(config.indexesPath, 'ba.adj.index.bin'), 'index');
    fs.writeFileSync(path.join(config.indexesPath, 'ba.node_ids.tsv'), '0\tP2\n');
    fs.writeFileSync(path.join(config.indexesPath, 'eu.adj.bin'), 'bin');
    fs.writeFileSync(path.join(config.indexesPath, 'eu.adj.index.bin'), 'index');
    fs.writeFileSync(path.join(config.indexesPath, 'eu.node_ids.tsv'), '0\tP1\n');
  });

  afterAll(() => {
    fs.rmSync(mockTestRoot, { recursive: true, force: true });
  });

  afterEach(() => {
    config.subnetwork.timeoutMs = originalTimeoutMs;
    config.subnetwork.maxConcurrentJobs = originalMaxConcurrentJobs;
  });

  test('normalizeOutputName appends .csv', () => {
    expect(service.normalizeOutputName('example')).toBe('example.csv');
  });

  test('normalizeOutputName rejects invalid names', () => {
    expect(() => service.normalizeOutputName('../bad')).toThrow();
  });

  test('createSubnetwork writes the generated CSV into data/networks and ingests it', async () => {
    spawn.mockImplementation((command, args) => {
      const outputPath = args[args.indexOf('-o') + 1];
      return createMockChildProcess({
        stdout: '__SUBNET_JSON__ {"ok":true,"missingSeeds":["MISSING1"],"edgeCount":1,"resolvedSeedCount":1,"inputSeedCount":2,"selectedNodeCount":2,"emptySubnetwork":false,"elapsedMs":12}\n',
        stderr: 'WARNING: 1 input seed(s) were not found in the network: MISSING1\n',
        onSpawn: async () => {
          await fs.promises.writeFile(outputPath, 'P1,P2,0.75\n');
        },
      });
    });

    const result = await service.createSubnetwork({
      seeds: ['P1', 'MISSING1'],
      name: 'generated',
      index: 'eu',
      maxNodes: 500,
    });

    expect(result.network).toBe('generated.csv');
    expect(result.viewerUrl).toBe('/viewer.html?network=generated.csv');
    expect(result.edgeCount).toBe(1);
    expect(result.missingSeeds).toEqual(['MISSING1']);
    expect(result.resolvedSeedCount).toBe(1);
    expect(result.inputSeedCount).toBe(2);
    expect(result.elapsedMs).toBe(12);
    expect(fs.existsSync(path.join(config.dataPath, 'generated.csv'))).toBe(true);
    expect(ingestNetworks).toHaveBeenCalledWith('generated.csv');
    expect(spawn).toHaveBeenCalledWith(
      'python3',
      expect.arrayContaining(['--json', '--']),
      expect.any(Object)
    );
  });

  test('createSubnetwork keeps existing networks and appends a suffix for duplicate names', async () => {
    fs.writeFileSync(path.join(config.dataPath, 'existing.csv'), 'P1,P2,0.5\n');

    spawn.mockImplementation((command, args) => {
      const outputPath = args[args.indexOf('-o') + 1];
      return createMockChildProcess({
        stdout: '__SUBNET_JSON__ {"ok":true,"missingSeeds":[],"edgeCount":1,"resolvedSeedCount":1,"inputSeedCount":1,"selectedNodeCount":2,"emptySubnetwork":false,"elapsedMs":9}\n',
        onSpawn: async () => {
          await fs.promises.writeFile(outputPath, 'P1,P2,0.5\n');
        },
      });
    });

    const result = await service.createSubnetwork({
      seeds: ['P1'],
      name: 'existing',
    });

    expect(result.network).toBe('existing_1.csv');
    expect(result.viewerUrl).toBe('/viewer.html?network=existing_1.csv');
    expect(fs.existsSync(path.join(config.dataPath, 'existing.csv'))).toBe(true);
    expect(fs.existsSync(path.join(config.dataPath, 'existing_1.csv'))).toBe(true);
    expect(ingestNetworks).toHaveBeenCalledWith('existing_1.csv');
  });

  test('createSubnetwork increments the suffix until it finds a free network name', async () => {
    fs.writeFileSync(path.join(config.dataPath, 'existing.csv'), 'P1,P2,0.5\n');
    fs.writeFileSync(path.join(config.dataPath, 'existing_1.csv'), 'P1,P2,0.6\n');

    spawn.mockImplementation((command, args) => {
      const outputPath = args[args.indexOf('-o') + 1];
      return createMockChildProcess({
        stdout: '__SUBNET_JSON__ {"ok":true,"missingSeeds":[],"edgeCount":1,"resolvedSeedCount":1,"inputSeedCount":1,"selectedNodeCount":2,"emptySubnetwork":false,"elapsedMs":9}\n',
        onSpawn: async () => {
          await fs.promises.writeFile(outputPath, 'P1,P2,0.7\n');
        },
      });
    });

    const result = await service.createSubnetwork({
      seeds: ['P1'],
      name: 'existing',
    });

    expect(result.network).toBe('existing_2.csv');
    expect(fs.existsSync(path.join(config.dataPath, 'existing.csv'))).toBe(true);
    expect(fs.existsSync(path.join(config.dataPath, 'existing_1.csv'))).toBe(true);
    expect(fs.existsSync(path.join(config.dataPath, 'existing_2.csv'))).toBe(true);
  });

  test('createSubnetwork rejects empty generated subnetworks', async () => {
    spawn.mockImplementation((command, args) => {
      const outputPath = args[args.indexOf('-o') + 1];
      return createMockChildProcess({
        stdout: '__SUBNET_JSON__ {"ok":true,"missingSeeds":[],"edgeCount":0,"resolvedSeedCount":1,"inputSeedCount":1,"selectedNodeCount":1,"emptySubnetwork":true,"elapsedMs":8}\n',
        stderr: 'WARNING: Generated subnetwork is empty.\n',
        onSpawn: async () => {
          await fs.promises.writeFile(outputPath, '');
        },
      });
    });

    await expect(service.createSubnetwork({
      seeds: ['P1'],
      name: 'empty-network',
    })).rejects.toMatchObject({
      status: 422,
      details: expect.objectContaining({
        emptySubnetwork: true,
        edgeCount: 0,
      }),
    });
  });

  test('normalizeOutputName preserves an existing uppercase .CSV extension', () => {
    expect(service.normalizeOutputName('Example.CSV')).toBe('Example.csv');
  });

  test('getAllowedIndexPrefixes discovers complete index triplets from disk', () => {
    expect(service.getAllowedIndexPrefixes()).toEqual(['ba', 'eu']);
  });

  test('createSubnetwork rejects invalid indexes', async () => {
    await expect(service.createSubnetwork({
      seeds: ['P1'],
      name: 'generated',
      index: 'bad',
    })).rejects.toMatchObject({ status: 400 });
  });

  test('createSubnetwork rejects oversized seed lists', async () => {
    await expect(service.createSubnetwork({
      seeds: new Array(11).fill('P1'),
      name: 'generated',
    })).rejects.toMatchObject({ status: 400 });
  });

  test('createSubnetwork rejects oversized seed input length', async () => {
    await expect(service.createSubnetwork({
      seeds: ['P'.repeat(2001)],
      name: 'generated',
    })).rejects.toMatchObject({ status: 400 });
  });

  test('createSubnetwork rejects seed identifiers that look like flags', async () => {
    await expect(service.createSubnetwork({
      seeds: ['-o'],
      name: 'generated',
    })).rejects.toMatchObject({ status: 400 });
  });

  test('createSubnetwork returns 429 when the in-process concurrency cap is reached', async () => {
    config.subnetwork.maxConcurrentJobs = 1;

    let releaseFirstJob;
    let markSpawnStarted;
    const spawnStarted = new Promise(resolve => {
      markSpawnStarted = resolve;
    });
    spawn.mockImplementation(() => {
      const child = new EventEmitter();
      child.stdout = new EventEmitter();
      child.stderr = new EventEmitter();
      child.kill = jest.fn();

      const blocker = new Promise(resolve => {
        releaseFirstJob = resolve;
      });

      process.nextTick(async () => {
        await blocker;
        child.emit('close', 0, null);
      });

      markSpawnStarted();
      return child;
    });

    const firstJob = service.createSubnetwork({
      seeds: ['P1'],
      name: 'generated-1',
    });

    await spawnStarted;

    await expect(service.createSubnetwork({
      seeds: ['P1'],
      name: 'generated-2',
    })).rejects.toMatchObject({ status: 429 });

    releaseFirstJob();
    await expect(firstJob).rejects.toMatchObject({ status: 500 });
  });

  test('createSubnetwork returns 504 on extraction timeout', async () => {
    config.subnetwork.timeoutMs = 1;

    spawn.mockImplementation(() => {
      const child = new EventEmitter();
      child.stdout = new EventEmitter();
      child.stderr = new EventEmitter();
      child.kill = jest.fn(() => {
        process.nextTick(() => child.emit('close', null, 'SIGTERM'));
      });
      return child;
    });

    await expect(service.createSubnetwork({
      seeds: ['P1'],
      name: 'timeout-network',
    })).rejects.toMatchObject({ status: 504 });
  });
});
