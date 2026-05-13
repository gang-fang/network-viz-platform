/**
 * Integration tests for attribute ingestion using a real in-memory SQLite database.
 *
 * These tests verify DB-level behaviour that cannot be confirmed through SQL inspection alone:
 *   - Existing protein rows without source tracking are cleared by the reconcile step
 *     before the selected files are replayed.
 *   - Full-selection atomicity: a failure during the write phase (after the outer transaction
 *     has started) rolls back reconcile and all prior file writes.
 */

const sqlite3 = require('sqlite3').verbose();
const path = require('path');

const FIXTURES = path.join(__dirname, '../fixtures');

// ─── Schema helpers ───────────────────────────────────────────────────────────

function createSchema(db) {
    return new Promise((resolve, reject) => {
        db.serialize(() => {
            db.run(`CREATE TABLE IF NOT EXISTS nodes (
              id TEXT PRIMARY KEY,
              attributes_json TEXT,
              attribute_source TEXT
            )`);
            db.run(`CREATE TABLE IF NOT EXISTS edges (
              id TEXT NOT NULL,
              node1 TEXT NOT NULL,
              node2 TEXT NOT NULL,
              weight REAL,
              source TEXT NOT NULL,
              attributes_json TEXT,
              PRIMARY KEY (id, source),
              FOREIGN KEY(node1) REFERENCES nodes(id),
              FOREIGN KEY(node2) REFERENCES nodes(id)
            )`);
            db.run(`CREATE TABLE IF NOT EXISTS network_nodes (
              source TEXT NOT NULL,
              node_id TEXT NOT NULL,
              PRIMARY KEY (source, node_id),
              FOREIGN KEY(node_id) REFERENCES nodes(id)
            )`, (err) => (err ? reject(err) : resolve()));
        });
    });
}

function dbRun(db, sql, params = []) {
    return new Promise((resolve, reject) =>
        db.run(sql, params, (err) => (err ? reject(err) : resolve()))
    );
}

function dbAll(db, sql, params = []) {
    return new Promise((resolve, reject) =>
        db.all(sql, params, (err, rows) => (err ? reject(err) : resolve(rows)))
    );
}

// ─── Test suite ───────────────────────────────────────────────────────────────

describe('Attribute ingestion — integration tests (real SQLite)', () => {
    let testDb;
    let ingestNodeAttributes;

    beforeAll(async () => {
        jest.resetModules();

        testDb = new sqlite3.Database(':memory:');
        await createSchema(testDb);

        jest.doMock('../../utils/logger', () => ({
            info: jest.fn(),
            warn: jest.fn(),
            error: jest.fn(),
            debug: jest.fn(),
        }));
        jest.doMock('../../config/database', () => testDb);
        jest.doMock('../../config/config', () => ({
            nodeAttributesPath: FIXTURES,
        }));

        ({ ingestNodeAttributes } = require('../../scripts/ingestData'));
    });

    afterAll((done) => {
        testDb.close(done);
        jest.resetModules();
    });

    beforeEach(async () => {
        await dbRun(testDb, 'DELETE FROM network_nodes');
        await dbRun(testDb, 'DELETE FROM edges');
        await dbRun(testDb, 'DELETE FROM nodes');
    });

    // ── Null-source rows ──────────────────────────────────────────────────────

    test('protein rows without source tracking are cleared by reconcile', async () => {
        // Simulate a row with real attributes but no source tracking
        await dbRun(testDb,
            `INSERT INTO nodes (id, attributes_json, attribute_source)
             VALUES ('LEGACY001', '{"NH_ID":"old.1","NCBI_txID":"9606"}', NULL)`
        );

        // Ingest test-e.nodes.attr — LEGACY001 is not in that file
        await ingestNodeAttributes(['test-e.nodes.attr']);

        const rows = await dbAll(testDb, 'SELECT * FROM nodes WHERE id = ?', ['LEGACY001']);
        expect(rows).toHaveLength(1);
        expect(rows[0].attributes_json).toBe('{}');
        expect(rows[0].attribute_source).toBeNull();
    });

    test('untracked-source rows in the current selection are cleared then re-written correctly', async () => {
        // Directly insert a row for node1 without source tracking.
        await dbRun(testDb,
            `INSERT INTO nodes (id, attributes_json, attribute_source)
             VALUES ('node1', '{"NH_ID":"old.untracked","NCBI_txID":"0"}', NULL)`
        );

        // Ingest test-e.nodes.attr — node1 is in that file with NH001
        await ingestNodeAttributes(['test-e.nodes.attr']);

        const rows = await dbAll(testDb, 'SELECT * FROM nodes WHERE id = ?', ['node1']);
        expect(rows).toHaveLength(1);
        expect(rows[0].attribute_source).toBe('test-e.nodes.attr');
        const attrs = JSON.parse(rows[0].attributes_json);
        expect(attrs.NH_ID).toBe('NH001'); // correct data from file, not stale value
    });

    // ── Reconcile stale-row behaviour ─────────────────────────────────────────

    test('protein removed from a selected file falls back to an empty placeholder', async () => {
        // Seed a row that claims to be from test-e.nodes.attr but uses an ID that
        // does not appear in that file — simulating a protein removed since last ingest.
        await dbRun(testDb,
            `INSERT INTO nodes (id, attributes_json, attribute_source)
             VALUES ('REMOVED001', '{"NH_ID":"x.1","NCBI_txID":"9606"}', 'test-e.nodes.attr')`
        );

        await ingestNodeAttributes(['test-e.nodes.attr']);

        const rows = await dbAll(testDb, 'SELECT * FROM nodes WHERE id = ?', ['REMOVED001']);
        expect(rows).toHaveLength(1);
        expect(rows[0].attributes_json).toBe('{}');
        expect(rows[0].attribute_source).toBeNull();
    });

    test('protein moved between selected files is cleanly reassigned to the new source', async () => {
        // Seed node4 as if it was previously sourced from test-e.nodes.attr.
        // node4 only appears in test-p.nodes.attr — simulates it having moved between files.
        await dbRun(testDb,
            `INSERT INTO nodes (id, attributes_json, attribute_source)
             VALUES ('node4', '{"NH_ID":"old.e","NCBI_txID":"9606"}', 'test-e.nodes.attr')`
        );

        await ingestNodeAttributes(['test-e.nodes.attr', 'test-p.nodes.attr']);

        const rows = await dbAll(testDb, 'SELECT * FROM nodes WHERE id = ?', ['node4']);
        expect(rows).toHaveLength(1);
        expect(rows[0].attribute_source).toBe('test-p.nodes.attr');
        const attrs = JSON.parse(rows[0].attributes_json);
        expect(attrs.NH_ID).toBe('NH101'); // correct data from test-p, not stale test-e value
    });

    // ── Full-selection atomicity (write-phase failure) ────────────────────────

    test('a failure during the write phase rolls back reconcile and all prior file writes', async () => {
        // Establish initial state
        await ingestNodeAttributes(['test-e.nodes.attr']);

        // Add a row from a deselected source — the reconcile will clear it,
        // but the rollback should restore it if ingest fails
        await dbRun(testDb,
            `INSERT INTO nodes (id, attributes_json, attribute_source)
             VALUES ('CANARY', '{"NH_ID":"x.1"}', 'other.nodes.attr')`
        );

        // Install a TEMP TRIGGER that fires after the first row from test-p.nodes.attr is
        // inserted, forcing a write-phase failure in the middle of the second file.
        await dbRun(testDb,
            `CREATE TEMP TRIGGER fail_mid_write
             AFTER INSERT ON nodes
             WHEN (SELECT COUNT(*) FROM nodes WHERE attribute_source = 'test-p.nodes.attr') >= 1
             BEGIN
               SELECT RAISE(FAIL, 'intentional test failure');
             END`
        );

        await expect(
            ingestNodeAttributes(['test-e.nodes.attr', 'test-p.nodes.attr'])
        ).rejects.toThrow();

        await dbRun(testDb, `DROP TRIGGER IF EXISTS fail_mid_write`);

        // CANARY should be restored — the reconcile that cleared it was rolled back
        const canary = await dbAll(testDb, `SELECT * FROM nodes WHERE id = 'CANARY'`);
        expect(canary).toHaveLength(1);
        expect(canary[0].attribute_source).toBe('other.nodes.attr');

        // test-p.nodes.attr data must not have been committed
        const pRows = await dbAll(testDb, `SELECT id FROM nodes WHERE attribute_source = 'test-p.nodes.attr'`);
        expect(pRows).toHaveLength(0);

        // test-e.nodes.attr data from the initial ingest should be intact
        const eRows = await dbAll(testDb, `SELECT id FROM nodes WHERE attribute_source = 'test-e.nodes.attr'`);
        expect(eRows.length).toBeGreaterThan(0);
    });
});

// ─── ingestNetworks — integration tests (real SQLite) ────────────────────────

describe('ingestNetworks — integration tests (real SQLite)', () => {
    let testDb;
    let ingestNetworks;

    beforeAll(async () => {
        jest.resetModules();

        testDb = new sqlite3.Database(':memory:');
        await createSchema(testDb);

        jest.doMock('../../utils/logger', () => ({
            info: jest.fn(),
            warn: jest.fn(),
            error: jest.fn(),
            debug: jest.fn(),
        }));
        jest.doMock('../../config/database', () => testDb);
        jest.doMock('../../config/config', () => ({
            dataPath: FIXTURES,
            nodeAttributesPath: FIXTURES,
        }));

        ({ ingestNetworks } = require('../../scripts/ingestData'));
    });

    afterAll((done) => {
        testDb.close(done);
        jest.resetModules();
    });

    beforeEach(async () => {
        await dbRun(testDb, 'DELETE FROM network_nodes');
        await dbRun(testDb, 'DELETE FROM edges');
        await dbRun(testDb, 'DELETE FROM nodes');
    });

    test('removes stale edges when a source is re-ingested', async () => {
        // First ingest — test-network.csv has 5 data rows
        await ingestNetworks('test-network.csv');
        const firstEdges = await dbAll(testDb, `SELECT id FROM edges WHERE source = 'test-network.csv'`);
        expect(firstEdges.length).toBe(5);
        const firstMembers = await dbAll(testDb, `SELECT node_id FROM network_nodes WHERE source = 'test-network.csv'`);
        expect(firstMembers.length).toBeGreaterThan(0);

        // Manually add a phantom edge that is not in the CSV
        await dbRun(testDb,
            `INSERT OR IGNORE INTO nodes (id, attributes_json) VALUES ('PHANTOM_A', '{}'), ('PHANTOM_B', '{}')`
        );
        await dbRun(testDb,
            `INSERT INTO edges (id, node1, node2, weight, source, attributes_json)
             VALUES ('PHANTOM_A|PHANTOM_B', 'PHANTOM_A', 'PHANTOM_B', 0.1, 'test-network.csv', '{}')`
        );
        expect((await dbAll(testDb, `SELECT id FROM edges WHERE source = 'test-network.csv'`)).length).toBe(6);

        // Re-ingest — phantom must be gone
        await ingestNetworks('test-network.csv');
        const afterEdges = await dbAll(testDb, `SELECT id FROM edges WHERE source = 'test-network.csv'`);
        expect(afterEdges.length).toBe(5);
        expect(afterEdges.map(r => r.id)).not.toContain('PHANTOM_A|PHANTOM_B');
    });

    test('succeeds with FK enforcement on when node IDs are new', async () => {
        await dbRun(testDb, 'PRAGMA foreign_keys = ON');
        try {
            await expect(ingestNetworks('test-network.csv')).resolves.not.toThrow();
            const edges = await dbAll(testDb, `SELECT id FROM edges WHERE source = 'test-network.csv'`);
            expect(edges.length).toBe(5);
        } finally {
            await dbRun(testDb, 'PRAGMA foreign_keys = OFF');
        }
    });

    test('serializes concurrent ingests of the same source file', async () => {
        await expect(Promise.all([
            ingestNetworks('test-network.csv'),
            ingestNetworks('test-network.csv'),
        ])).resolves.toHaveLength(2);

        const edges = await dbAll(testDb, `SELECT id FROM edges WHERE source = 'test-network.csv'`);
        expect(edges).toHaveLength(5);
    });

    test('rolls back all edges for a source on mid-write failure', async () => {
        // Canary edge from a different source — must survive the rollback
        await dbRun(testDb,
            `INSERT OR IGNORE INTO nodes (id, attributes_json) VALUES ('C1', '{}'), ('C2', '{}')`
        );
        await dbRun(testDb,
            `INSERT INTO edges (id, node1, node2, weight, source, attributes_json)
             VALUES ('C1|C2', 'C1', 'C2', 1.0, 'other.csv', '{}')`
        );

        // Trigger that fires after the first edge insert, forcing a failure
        await dbRun(testDb,
            `CREATE TEMP TRIGGER fail_network_write
             AFTER INSERT ON edges
             WHEN NEW.source = 'test-network.csv'
             BEGIN
               SELECT RAISE(FAIL, 'intentional test failure');
             END`
        );

        await expect(ingestNetworks('test-network.csv')).rejects.toThrow();

        await dbRun(testDb, `DROP TRIGGER IF EXISTS fail_network_write`);

        // No test-network.csv edges should have been committed
        const netEdges = await dbAll(testDb, `SELECT id FROM edges WHERE source = 'test-network.csv'`);
        expect(netEdges).toHaveLength(0);

        // Canary must be intact
        const canary = await dbAll(testDb, `SELECT id FROM edges WHERE source = 'other.csv'`);
        expect(canary).toHaveLength(1);
    });

    test('malformed rows (short, blank IDs, non-finite weight) are skipped — no edges or nodes written', async () => {
        // malformed.csv contains:
        //   rows with < 3 columns (skipped by length check)
        //   3-column rows with a blank node1, blank node2, or non-finite weight (skipped by guard)
        // Nothing valid survives, so the DB must stay empty.
        await ingestNetworks('malformed.csv');

        const edges = await dbAll(testDb, `SELECT id FROM edges WHERE source = 'malformed.csv'`);
        expect(edges).toHaveLength(0);

        // No placeholder nodes should have been created from malformed data
        const nodes = await dbAll(testDb, `SELECT id FROM nodes`);
        expect(nodes).toHaveLength(0);
    });
});
