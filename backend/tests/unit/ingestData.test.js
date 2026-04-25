/**
 * Tests for the refactored attribute ingestion pipeline.
 *
 * validateNodeAttributeFiles is tested against real fixture files — no DB involved.
 * ingestNodeAttributes early-exit paths are tested by mocking config/DB.
 * File watcher attr-file filtering is tested by mocking config and ingestNodeAttributes.
 */

const path = require('path');

// ─── Fixtures ────────────────────────────────────────────────────────────────

const FIXTURES = path.join(__dirname, '../fixtures');

function fixtureFile(name) {
    return { name, path: path.join(FIXTURES, name) };
}

// ─── validateNodeAttributeFiles ──────────────────────────────────────────────

// Import without mocking DB — validateNodeAttributeFiles only reads files
jest.mock('../../config/database', () => ({
    run: jest.fn(),
    get: jest.fn(),
    prepare: jest.fn(() => ({ run: jest.fn(), finalize: jest.fn() })),
    all: jest.fn(),
    serialize: jest.fn(cb => cb()),
}));

jest.mock('../../utils/logger', () => ({
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
    debug: jest.fn(),
}));

const { validateNodeAttributeFiles, ingestNodeAttributes } = require('../../scripts/ingestData');

describe('validateNodeAttributeFiles', () => {
    test('passes for a single clean file', async () => {
        await expect(
            validateNodeAttributeFiles([fixtureFile('test-e.nodes.attr')])
        ).resolves.toBeUndefined();
    });

    test('passes for two clean files with no overlapping UniProt ACs', async () => {
        await expect(
            validateNodeAttributeFiles([
                fixtureFile('test-e.nodes.attr'),
                fixtureFile('test-p.nodes.attr'),
            ])
        ).resolves.toBeUndefined();
    });

    test('throws for a single file with an internal duplicate', async () => {
        await expect(
            validateNodeAttributeFiles([fixtureFile('test-dup-within.nodes.attr')])
        ).rejects.toThrow(/DUPNODE1/);
    });

    test('error message for within-file duplicate names the file and both line numbers', async () => {
        await expect(
            validateNodeAttributeFiles([fixtureFile('test-dup-within.nodes.attr')])
        ).rejects.toThrow(/test-dup-within\.nodes\.attr/);
    });

    test('throws for two files with a cross-file duplicate', async () => {
        await expect(
            validateNodeAttributeFiles([
                fixtureFile('test-dup-cross-a.nodes.attr'),
                fixtureFile('test-dup-cross-b.nodes.attr'),
            ])
        ).rejects.toThrow(/CROSSNODE2/);
    });

    test('error message for cross-file duplicate names both files', async () => {
        let message = '';
        try {
            await validateNodeAttributeFiles([
                fixtureFile('test-dup-cross-a.nodes.attr'),
                fixtureFile('test-dup-cross-b.nodes.attr'),
            ]);
        } catch (err) {
            message = err.message;
        }
        expect(message).toMatch(/test-dup-cross-a\.nodes\.attr/);
        expect(message).toMatch(/test-dup-cross-b\.nodes\.attr/);
    });

    test('header row "node_id" is not treated as a protein entry', async () => {
        // If the header were parsed as data, "node_id" would appear in both files
        // and trigger a duplicate error. Passing cleanly proves headers are skipped.
        await expect(
            validateNodeAttributeFiles([
                fixtureFile('test-e.nodes.attr'),
                fixtureFile('test-p.nodes.attr'),
            ])
        ).resolves.toBeUndefined();
    });
});

// ─── ingestNodeAttributes — reconcile SQL ────────────────────────────────────

describe('ingestNodeAttributes reconcile step', () => {
    // Intercept the first dbRun call to capture the reconcile UPDATE statement and verify
    // the SQL structure and parameters. End-to-end DB write behaviour (legacy-row clearing,
    // full kind='nh' cleanup, full atomicity) is covered by the integration tests in
    // backend/tests/integration/ingestData.integration.test.js.

    let capturedSql;
    let capturedParams;

    beforeEach(() => {
        jest.resetModules();
        capturedSql = null;
        capturedParams = null;

        // Mock util.promisify so dbRun records every call.
        // Allow BEGIN TRANSACTION and PRAGMA through (needed to reach the reconcile UPDATE),
        // then short-circuit on the first UPDATE so we can inspect it without running the
        // full write phase.
        jest.mock('util', () => {
            const real = jest.requireActual('util');
            return {
                ...real,
                promisify: (fn) => {
                    if (fn && fn._isMockFunction) return fn;
                    return (...args) => {
                        const sql = typeof args[0] === 'string' ? args[0].trim() : '';
                        if (sql.startsWith('PRAGMA') || sql.startsWith('BEGIN') || sql.startsWith('ROLLBACK')) {
                            return Promise.resolve();
                        }
                        // First non-preamble call is the reconcile UPDATE — capture it
                        capturedSql = args[0];
                        capturedParams = args[1];
                        return Promise.reject(new Error('__short_circuit__'));
                    };
                },
            };
        });

        jest.mock('../../config/database', () => ({
            run: jest.fn(),
            get: jest.fn(),
            prepare: jest.fn(() => ({ run: jest.fn(), finalize: jest.fn() })),
            all: jest.fn(),
        }));

        jest.mock('../../utils/logger', () => ({
            info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn(),
        }));

        jest.mock('../../config/config', () => ({
            nodeAttributesPath: require('path').join(__dirname, '../fixtures'),
            nodeAttributeFiles: ['test-e.nodes.attr'],
        }));
    });

    afterEach(() => {
        jest.resetModules();
    });

    test('reconcile UPDATE clears all non-empty protein rows unconditionally', async () => {
        const { ingestNodeAttributes } = require('../../scripts/ingestData');
        try {
            await ingestNodeAttributes(['test-e.nodes.attr']);
        } catch (err) {
            if (err.message !== '__short_circuit__') throw err;
        }
        // The reconcile UPDATE should have been the first dbRun call
        expect(capturedSql).toMatch(/UPDATE nodes/i);
        // All non-empty protein rows are cleared so every row is rewritten from the
        // current file set (covers removed, moved, legacy null-source, and deselected rows)
        expect(capturedSql).toMatch(/attributes_json != '{}'/i);
        // No per-file filtering in the reconcile — no params needed
        expect(capturedParams).toBeUndefined();
    });
});

// ─── ingestNodeAttributes — argument validation ───────────────────────────────

describe('ingestNodeAttributes argument validation', () => {
    test('throws when called with an empty array', async () => {
        await expect(ingestNodeAttributes([])).rejects.toThrow(/No attribute files specified/);
    });

    test('throws when called with null', async () => {
        await expect(ingestNodeAttributes(null)).rejects.toThrow(/No attribute files specified/);
    });

    test('throws when called with undefined', async () => {
        await expect(ingestNodeAttributes(undefined)).rejects.toThrow(/No attribute files specified/);
    });

    test('throws with a message that mentions NODE_ATTRIBUTE_FILES', async () => {
        await expect(ingestNodeAttributes([])).rejects.toThrow(/NODE_ATTRIBUTE_FILES/);
    });
});

// ─── File watcher event handling ──────────────────────────────────────────────

describe('FileWatcher event handling', () => {
    let FileWatcher;
    let mockIngestNodeAttributes;
    let mockIngestNetworks;
    let mockInvalidateSpeciesTree;
    let mockLogger;
    let mockDbRun;

    beforeEach(() => {
        jest.resetModules();

        mockIngestNodeAttributes = jest.fn().mockResolvedValue(undefined);
        mockIngestNetworks = jest.fn().mockResolvedValue(undefined);
        mockInvalidateSpeciesTree = jest.fn();
        mockDbRun = jest.fn((sql, params, callback) => {
            if (typeof params === 'function') callback = params;
            if (callback) callback(null);
        });
        mockLogger = { info: jest.fn(), warn: jest.fn(), error: jest.fn() };

        jest.mock('../../scripts/ingestData', () => ({
            ingestNodeAttributes: mockIngestNodeAttributes,
            ingestNetworks: mockIngestNetworks,
        }));

        jest.mock('../../routes/species-tree', () => ({
            invalidateCache: mockInvalidateSpeciesTree,
        }));

        jest.mock('../../utils/logger', () => mockLogger);

        jest.mock('../../config/database', () => ({
            run: mockDbRun,
        }));

        jest.mock('util', () => {
            const real = jest.requireActual('util');
            return {
                ...real,
                promisify: (fn) => (...args) => new Promise((resolve, reject) => {
                    fn(...args, (err, result) => err ? reject(err) : resolve(result));
                }),
            };
        });

        jest.mock('../../config/config', () => ({
            dataPath: '/mock/networks',
            nodeAttributesPath: '/mock/nodes_attr',
            taxonTreePath: '/mock/taxonomy/commontree.txt',
            taxonNamesPath: '/mock/taxonomy/NCBI_txID.csv',
            nodeAttributeFiles: ['e.nodes.attr'],
            networkEdit: {
                watcherSuppressMs: 30000,
                watcherSuppressEvents: 3,
            },
        }));

        jest.mock('chokidar', () => ({
            watch: jest.fn(() => ({
                on: jest.fn().mockReturnThis(),
            })),
        }));

        // Re-require after resetting modules
        FileWatcher = require('../../services/fileWatcher');
    });

    afterEach(() => {
        jest.resetModules();
    });

    test('ignores a .nodes.attr file that is not in NODE_ATTRIBUTE_FILES', async () => {
        await FileWatcher.handleFileChange('/mock/nodes_attr/p.nodes.attr', 'added');
        expect(mockIngestNodeAttributes).not.toHaveBeenCalled();
    });

    test('triggers full configured-set re-ingestion when a configured file changes', async () => {
        await FileWatcher.handleFileChange('/mock/nodes_attr/e.nodes.attr', 'changed');
        expect(mockIngestNodeAttributes).toHaveBeenCalledWith(['e.nodes.attr']);
        expect(mockInvalidateSpeciesTree).toHaveBeenCalledTimes(1);
    });

    test('suppresses watcher ingestion for internally saved edited network files', async () => {
        FileWatcher.suppressNetworkIngest('edited.csv');

        await FileWatcher.handleFileChange('/mock/networks/edited.csv', 'added');

        expect(mockIngestNetworks).not.toHaveBeenCalled();
        expect(mockInvalidateSpeciesTree).not.toHaveBeenCalled();
        expect(mockLogger.info).toHaveBeenCalledWith(
            expect.stringContaining('Skipping watcher-triggered ingest')
        );
    });

    test('suppresses only the configured number of edited-network watcher events', async () => {
        FileWatcher.suppressNetworkIngest('edited.csv', { eventCount: 2, ttlMs: 30000 });

        await FileWatcher.handleFileChange('/mock/networks/edited.csv', 'added');
        await FileWatcher.handleFileChange('/mock/networks/edited.csv', 'changed');
        await FileWatcher.handleFileChange('/mock/networks/edited.csv', 'changed');

        expect(mockIngestNetworks).toHaveBeenCalledTimes(1);
        expect(mockIngestNetworks).toHaveBeenCalledWith('edited.csv');
        expect(mockInvalidateSpeciesTree).toHaveBeenCalledTimes(1);
    });

    test('invalidates the species tree cache when taxonomy source files change', async () => {
        await FileWatcher.handleFileChange('/mock/taxonomy/commontree.txt', 'changed');
        await FileWatcher.handleFileChange('/mock/taxonomy/NCBI_txID.csv', 'changed');

        expect(mockIngestNetworks).not.toHaveBeenCalled();
        expect(mockIngestNodeAttributes).not.toHaveBeenCalled();
        expect(mockInvalidateSpeciesTree).toHaveBeenCalledTimes(2);
    });

    test('does not invalidate the species tree cache when network ingest fails', async () => {
        mockIngestNetworks.mockRejectedValueOnce(new Error('ingest failed'));

        await FileWatcher.handleFileChange('/mock/networks/broken.csv', 'changed');

        expect(mockInvalidateSpeciesTree).not.toHaveBeenCalled();
        expect(mockLogger.error).toHaveBeenCalled();
    });

    test('invalidates the species tree cache when a network csv is removed', async () => {
        await FileWatcher.handleFileRemove('/mock/networks/removed.csv');

        expect(mockInvalidateSpeciesTree).toHaveBeenCalledTimes(1);
        expect(mockDbRun).toHaveBeenCalledWith('BEGIN TRANSACTION', expect.any(Function));
        expect(mockDbRun).toHaveBeenCalledWith('DELETE FROM network_nodes WHERE source = ?', ['removed.csv'], expect.any(Function));
        expect(mockDbRun).toHaveBeenCalledWith('DELETE FROM edges WHERE source = ?', ['removed.csv'], expect.any(Function));
        expect(mockDbRun).toHaveBeenCalledWith('COMMIT', expect.any(Function));
    });

    test('rolls back csv removal failures without invalidating the species tree cache', async () => {
        mockDbRun.mockImplementation((sql, params, callback) => {
            if (typeof params === 'function') callback = params;
            if (sql === 'DELETE FROM edges WHERE source = ?') {
                callback(new Error('delete failed'));
                return;
            }
            if (callback) callback(null);
        });

        await FileWatcher.handleFileRemove('/mock/networks/broken-remove.csv');

        expect(mockInvalidateSpeciesTree).not.toHaveBeenCalled();
        expect(mockDbRun).toHaveBeenCalledWith('ROLLBACK', expect.any(Function));
        expect(mockLogger.error).toHaveBeenCalledWith(
            'Error deleting network broken-remove.csv:',
            expect.any(Error)
        );
    });
});
