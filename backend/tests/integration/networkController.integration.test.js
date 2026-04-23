/**
 * Integration tests for the network controller using a real in-memory SQLite database.
 *
 * These tests verify controller behaviour that cannot be confirmed through mocks alone:
 *   - Unknown network source throws "Network not found" so the route can map it to 404.
 *   - Known source returns the correct nodes and edges.
 *   - searchProteins scopes results to the requested network source; accessions that
 *     exist only in a different network are not returned.
 */

const sqlite3 = require('sqlite3').verbose();

// ─── Schema helpers (shared with ingestion integration tests) ─────────────────

function createSchema(db) {
    return new Promise((resolve, reject) => {
        db.serialize(() => {
            db.run(`CREATE TABLE IF NOT EXISTS nodes (
              id TEXT PRIMARY KEY,
              kind TEXT NOT NULL,
              attributes_json TEXT,
              attribute_source TEXT
            )`);
            db.run(`CREATE TABLE IF NOT EXISTS edges (
              id TEXT NOT NULL,
              node1 TEXT,
              node2 TEXT,
              weight REAL,
              source TEXT,
              attributes_json TEXT,
              PRIMARY KEY (id, source)
            )`, (err) => (err ? reject(err) : resolve()));
        });
    });
}

function dbRun(db, sql, params = []) {
    return new Promise((resolve, reject) =>
        db.run(sql, params, (err) => (err ? reject(err) : resolve()))
    );
}

// ─── Test suites ──────────────────────────────────────────────────────────────

describe('getNetworkData — integration tests (real SQLite)', () => {
    let testDb;
    let getNetworkData;

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

        ({ getNetworkData } = require('../../controllers/networkController'));
    });

    afterAll((done) => {
        testDb.close(done);
        jest.resetModules();
    });

    beforeEach(async () => {
        await dbRun(testDb, 'DELETE FROM nodes');
        await dbRun(testDb, 'DELETE FROM edges');
    });

    test('throws "Network not found" for an unknown source so the route returns 404', async () => {
        await expect(getNetworkData('nonexistent.csv'))
            .rejects.toThrow('Network not found: nonexistent.csv');
    });

    test('returns nodes and edges for a known source', async () => {
        await dbRun(testDb,
            `INSERT INTO nodes (id, kind, attributes_json) VALUES ('A', 'protein', '{"NH_ID":"NH001"}'), ('B', 'protein', '{}')`
        );
        await dbRun(testDb,
            `INSERT INTO edges (id, node1, node2, weight, source, attributes_json) VALUES ('A|B', 'A', 'B', 0.9, 'net.csv', '{}')`
        );

        const result = await getNetworkData('net.csv');
        const nodeIds = result.elements.nodes.map(n => n.data.id).sort();
        expect(nodeIds).toEqual(['A', 'B']);
        expect(result.elements.edges).toHaveLength(1);
        expect(result.elements.edges[0].data.weight).toBe(0.9);
    });
});

describe('searchProteins — integration tests (real SQLite)', () => {
    let testDb;
    let searchProteins;

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

        ({ searchProteins } = require('../../controllers/networkController'));
    });

    afterAll((done) => {
        testDb.close(done);
        jest.resetModules();
    });

    beforeEach(async () => {
        await dbRun(testDb, 'DELETE FROM nodes');
        await dbRun(testDb, 'DELETE FROM edges');
    });

    test('returns a match only for accessions present in the requested network', async () => {
        // P001 is in net-a.csv; P002 is in net-b.csv only
        await dbRun(testDb,
            `INSERT INTO nodes (id, kind, attributes_json) VALUES
             ('P001', 'protein', '{"NH_ID":"NH001"}'),
             ('P002', 'protein', '{"NH_ID":"NH002"}')`
        );
        await dbRun(testDb,
            `INSERT INTO edges (id, node1, node2, weight, source, attributes_json) VALUES
             ('P001|X', 'P001', 'X', 1.0, 'net-a.csv', '{}'),
             ('P002|Y', 'P002', 'Y', 1.0, 'net-b.csv', '{}')`
        );
        await dbRun(testDb,
            `INSERT INTO nodes (id, kind, attributes_json) VALUES
             ('X', 'protein', '{}'), ('Y', 'protein', '{}')`
        );

        const result = await searchProteins('net-a.csv', ['P001', 'P002']);

        // P001 is in net-a.csv — must be returned
        expect(result.matches.map(m => m.id)).toContain('P001');
        // P002 is only in net-b.csv — must not be returned
        expect(result.matches.map(m => m.id)).not.toContain('P002');
    });

    test('returns nh_id from node attributes', async () => {
        await dbRun(testDb,
            `INSERT INTO nodes (id, kind, attributes_json) VALUES ('P003', 'protein', '{"NH_ID":"NH099"}')`
        );
        await dbRun(testDb,
            `INSERT INTO edges (id, node1, node2, weight, source, attributes_json) VALUES
             ('P003|Q', 'P003', 'Q', 1.0, 'net-a.csv', '{}')`
        );
        await dbRun(testDb,
            `INSERT INTO nodes (id, kind, attributes_json) VALUES ('Q', 'protein', '{}')`
        );

        const result = await searchProteins('net-a.csv', ['P003']);

        expect(result.matches).toHaveLength(1);
        expect(result.matches[0].nh_id).toBe('NH099');
    });
});

describe('searchBySpecies — integration tests (real SQLite)', () => {
    let testDb;
    let searchBySpecies;

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

        ({ searchBySpecies } = require('../../controllers/networkController'));
    });

    afterAll((done) => {
        testDb.close(done);
        jest.resetModules();
    });

    beforeEach(async () => {
        await dbRun(testDb, 'DELETE FROM nodes');
        await dbRun(testDb, 'DELETE FROM edges');
    });

    test('returns only species matches from the requested network', async () => {
        await dbRun(testDb,
            `INSERT INTO nodes (id, kind, attributes_json) VALUES
             ('P001', 'protein', '{"NCBI_txID":"9606","NH_ID":"NH001"}'),
             ('P002', 'protein', '{"NCBI_txID":"10090","NH_ID":"NH002"}'),
             ('P003', 'protein', '{"NCBI_txID":"9606","NH_ID":"NH003"}'),
             ('X', 'protein', '{}'),
             ('Y', 'protein', '{}'),
             ('Z', 'protein', '{}')`
        );
        await dbRun(testDb,
            `INSERT INTO edges (id, node1, node2, weight, source, attributes_json) VALUES
             ('P001|X', 'P001', 'X', 1.0, 'net-a.csv', '{}'),
             ('P002|Y', 'P002', 'Y', 1.0, 'net-a.csv', '{}'),
             ('P003|Z', 'P003', 'Z', 1.0, 'net-b.csv', '{}')`
        );

        const result = await searchBySpecies('net-a.csv', ['9606']);

        expect(result.matches).toEqual([{ id: 'P001', nh_id: 'NH001' }]);
    });
});
