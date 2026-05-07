const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse');
const db = require('../config/database');
const logger = require('../utils/logger');
const util = require('util');
const config = require('../config/config');

const NODES_ATTR_DIR = config.nodeAttributesPath;
const NETWORKS_DIR = config.dataPath;
const BATCH_SIZE = 10000;

const ATTR_COLUMNS = ['node_id', 'NCBI_txID', 'NH_ID', 'NH_Size', 'reserved1', 'reserved2', 'reserved3', 'reserved4', 'reserved5'];
const REQUIRED_ATTR_HEADER_PREFIX = ['node_id', 'NCBI_txID', 'NH_ID', 'NH_Size'];
let networkIngestQueue = Promise.resolve();
let networkIngestRunId = 0;

// Promisify DB run
const dbRun = util.promisify(db.run.bind(db));
const dbGet = util.promisify(db.get.bind(db));
const dbAll = util.promisify(db.all.bind(db));

async function getPragmaValue(name) {
    const row = await dbGet(`PRAGMA ${name}`);
    return row && row[name];
}

async function restoreIngestPragmas(previousPragmas) {
    if (!previousPragmas) return;

    if (previousPragmas.journal_mode) {
        await dbRun(`PRAGMA journal_mode = ${previousPragmas.journal_mode}`);
    }
    if (previousPragmas.synchronous !== undefined) {
        await dbRun(`PRAGMA synchronous = ${previousPragmas.synchronous}`);
    }
    if (previousPragmas.foreign_keys !== undefined) {
        await dbRun(`PRAGMA foreign_keys = ${previousPragmas.foreign_keys}`);
    }
}

async function ingestData() {
    let previousPragmas = null;
    let failed = false;
    try {
        logger.info('Starting full data ingestion...');

        const attrFiles = resolveSingleNodeAttributeFile();

        previousPragmas = {
            foreign_keys: await getPragmaValue('foreign_keys'),
            synchronous: await getPragmaValue('synchronous'),
            journal_mode: await getPragmaValue('journal_mode'),
        };

        // Disable durability checks for faster ingestion, then restore them below.
        await dbRun('PRAGMA foreign_keys = OFF');
        await dbRun('PRAGMA synchronous = OFF');
        await dbRun('PRAGMA journal_mode = MEMORY');

        await ingestNodeAttributes(attrFiles);
        await ingestNetworks();

        logger.info('Data ingestion completed successfully.');
    } catch (error) {
        console.error('Data ingestion failed:', error);
        logger.error('Data ingestion failed:', error);
        failed = true;
    } finally {
        try {
            await restoreIngestPragmas(previousPragmas);
        } catch (restoreError) {
            logger.error(`Failed to restore SQLite pragmas after ingest: ${restoreError.message}`);
            if (!failed) throw restoreError;
        }
    }

    if (failed) {
        process.exit(1);
    }
}

function getNodeAttributeFiles() {
    if (!fs.existsSync(NODES_ATTR_DIR)) {
        throw new Error(
            `Node attributes directory not found: ${NODES_ATTR_DIR}. ` +
            'Create it and place exactly one .nodes.attr file there.'
        );
    }

    return fs.readdirSync(NODES_ATTR_DIR)
        .filter(file => file.endsWith('.nodes.attr'))
        .sort();
}

function resolveSingleNodeAttributeFile() {
    const files = getNodeAttributeFiles();

    if (files.length !== 1) {
        const found = files.length > 0 ? files.join(', ') : 'none';
        throw new Error(
            `Expected exactly one .nodes.attr file in ${NODES_ATTR_DIR}, found ${files.length}: ${found}. ` +
            'Place exactly one properly formatted .nodes.attr file there and restart.'
        );
    }

    return files;
}

async function readFirstNonEmptyLine(filePath) {
    return new Promise((resolve, reject) => {
        const stream = fs.createReadStream(filePath, { encoding: 'utf8' });
        let buffer = '';
        let settled = false;

        function finish(value) {
            if (settled) return;
            settled = true;
            resolve(value);
        }

        stream.on('data', chunk => {
            buffer += chunk;
            const lines = buffer.split(/\r?\n/);
            const completeLines = buffer.endsWith('\n') || buffer.endsWith('\r')
                ? lines
                : lines.slice(0, -1);
            const first = completeLines.find(line => line.trim());

            if (first !== undefined) {
                stream.destroy();
                finish(first);
            }
        });
        stream.on('end', () => finish(buffer.split(/\r?\n/).find(line => line.trim()) || ''));
        stream.on('error', reject);
    });
}

async function validateNodeAttributeHeader(filename, filePath) {
    const headerLine = await readFirstNonEmptyLine(filePath);
    const columns = headerLine.split(',').map(column => column.trim());
    const hasRequiredPrefix = REQUIRED_ATTR_HEADER_PREFIX.every((column, index) => columns[index] === column);

    if (!hasRequiredPrefix) {
        throw new Error(
            `${filename} has an invalid .nodes.attr header. Expected first columns: ` +
            REQUIRED_ATTR_HEADER_PREFIX.join(', ')
        );
    }
}

/**
 * Ingest node attributes from an explicit list of .nodes.attr files.
 *
 * @param {string[]} files - Filenames (not full paths) to ingest. Required; throws if empty.
 */
async function ingestNodeAttributes(files) {
    if (!files || files.length === 0) {
        throw new Error(
            'No attribute files specified. Place exactly one .nodes.attr file in the node attributes directory ' +
            'or pass an explicit non-empty file list to ingestNodeAttributes().'
        );
    }

    if (!fs.existsSync(NODES_ATTR_DIR)) {
        throw new Error(`Node attributes directory not found: ${NODES_ATTR_DIR}`);
    }

    // Resolve and verify all files before touching the DB
    const resolvedFiles = files.map(name => ({ name, path: path.join(NODES_ATTR_DIR, name) }));

    for (const { name, path: filePath } of resolvedFiles) {
        if (!fs.existsSync(filePath)) {
            throw new Error(`Attribute file not found: ${name} (looked in ${NODES_ATTR_DIR})`);
        }
        await validateNodeAttributeHeader(name, filePath);
    }

    // Preflight: reject if any UniProt AC appears more than once across the selected files
    await validateNodeAttributeFiles(resolvedFiles);

    // Single outer transaction: reconcile + all file writes are atomic.
    // If any file fails, the entire operation rolls back — no partial state.
    await dbRun('BEGIN TRANSACTION');
    try {
        // Reconcile protein rows: clear all non-empty protein attributes so every row
        // is rewritten from the current file set. This ensures:
        //   - Proteins removed from a file fall back to edge placeholders (not stale data).
        //   - Proteins moved between files are cleanly reassigned.
        //   - Legacy null-source rows are cleared (non-empty attrs, no source tracking).
        // Rows already at '{}' (edge-created placeholders) are untouched.
        await dbRun(
            `UPDATE nodes SET attributes_json = '{}', attribute_source = NULL
             WHERE kind = 'protein' AND attributes_json != '{}'`
        );

        // Remove unreferenced kind='nh' rows. NH nodes are derived by the frontend
        // from protein attributes at render time — they are never read from the DB.
        // This DELETE is a one-time migration cleanup for rows written by older code;
        // it is a no-op on databases that have already been through this step.
        // Rows referenced by edges (via FK) are intentionally skipped to avoid a
        // SQLITE_CONSTRAINT failure when foreign_keys is ON; such rows are anomalous
        // in the current schema and can be addressed separately if they appear.
        await dbRun(`DELETE FROM nodes WHERE kind = 'nh'
             AND id NOT IN (SELECT node1 FROM edges UNION SELECT node2 FROM edges)`);

        // Write each file within the outer transaction.
        // SAVEPOINTs are used inside each file for batch memory management — they do not
        // break the outer atomicity boundary.
        for (const { name, path: filePath } of resolvedFiles) {
            await ingestSingleNodeAttributeFile(name, filePath);
        }

        await dbRun('COMMIT');
    } catch (err) {
        await dbRun('ROLLBACK').catch(rollbackErr =>
            logger.error(`Failed to rollback attribute ingestion: ${rollbackErr}`)
        );
        throw err;
    }

    logger.info(`Attribute ingestion complete: [${files.join(', ')}]`);
}

/**
 * Scan the selected attribute files and throw if any UniProt AC appears more than once.
 * Reports file name, line number, NH_ID and NCBI_txID for each conflicting entry.
 *
 * @param {{ name: string, path: string }[]} resolvedFiles
 */
async function validateNodeAttributeFiles(resolvedFiles) {
    const seen = new Map(); // nodeId → { file, line, NH_ID, NCBI_txID }
    const conflicts = [];

    for (const { name, path: filePath } of resolvedFiles) {
        const parser = fs.createReadStream(filePath).pipe(parse({
            columns: ATTR_COLUMNS,
            trim: true,
            skip_empty_lines: true,
            from_line: 2, // skip header row
            info: true,   // wrap each record as { record, info } for line numbers
        }));

        for await (const { record, info } of parser) {
            const { node_id, NCBI_txID, NH_ID } = record;
            const line = info.lines;

            if (!node_id) {
                logger.warn(`${name}:${line} — blank node_id, skipping row`);
                continue;
            }

            if (seen.has(node_id)) {
                const prev = seen.get(node_id);
                conflicts.push({ node_id, prev, curr: { file: name, line, NCBI_txID, NH_ID } });
            } else {
                seen.set(node_id, { file: name, line, NCBI_txID, NH_ID });
            }
        }
    }

    if (conflicts.length === 0) return;

    const shown = conflicts.slice(0, 20);
    const detail = shown.map(c =>
        `  ${c.node_id}\n` +
        `    first:  ${c.prev.file}:${c.prev.line}  NH_ID=${c.prev.NH_ID}  NCBI_txID=${c.prev.NCBI_txID}\n` +
        `    second: ${c.curr.file}:${c.curr.line}  NH_ID=${c.curr.NH_ID}  NCBI_txID=${c.curr.NCBI_txID}`
    ).join('\n');
    const trailer = conflicts.length > 20 ? `\n  ... and ${conflicts.length - 20} more` : '';

    throw new Error(
        `${conflicts.length} duplicate UniProt AC(s) detected in selected attribute files. ` +
        `Resolve before ingesting:\n${detail}${trailer}`
    );
}

/**
 * Write one attribute file to the DB.
 * Reconcile has already cleared all non-empty protein rows, so every existing row
 * encountered here is a '{}' placeholder (edge-created or reconcile-cleared).
 */
async function ingestSingleNodeAttributeFile(filename, filePath) {
    logger.info(`Processing attribute file: ${filename}`);

    const parser = fs.createReadStream(filePath).pipe(parse({
        columns: ATTR_COLUMNS,
        trim: true,
        skip_empty_lines: true,
        from_line: 2, // skip header row
    }));

    // Prepared upsert: one DB call per row, no read-before-write.
    // Reconcile has already cleared all non-empty protein rows to '{}', so every
    // existing row is a placeholder — we always want to overwrite it.
    // ON CONFLICT(id) DO UPDATE is a true in-place upsert (unlike OR REPLACE, which
    // deletes + re-inserts and would violate FK constraints from edge rows).
    const stmt = db.prepare(
        `INSERT INTO nodes (id, kind, attributes_json, attribute_source)
         VALUES (?, 'protein', ?, ?)
         ON CONFLICT(id) DO UPDATE SET
             kind             = 'protein',
             attributes_json  = excluded.attributes_json,
             attribute_source = excluded.attribute_source`
    );

    let count = 0;

    // Use a named SAVEPOINT for batch memory management within the outer transaction.
    // RELEASE writes the batch to the outer transaction's WAL, reducing peak memory.
    // On error, ROLLBACK TO + RELEASE undoes only this file's partial changes before
    // re-throwing, so the outer ROLLBACK returns to the pre-ingest state cleanly.
    const SP = 'attr_batch';
    await dbRun(`SAVEPOINT ${SP}`);

    try {
        for await (const record of parser) {
            const { node_id, ...attrs } = record;

            if (!node_id) continue; // preflight already warned; skip silently here

            // Drop reserved fields (always omitted from stored attributes) and empty strings
            for (const key of Object.keys(attrs)) {
                if (key.startsWith('reserved') || attrs[key] === '') {
                    delete attrs[key];
                }
            }

            const incomingJson = JSON.stringify(attrs);

            await new Promise((resolve, reject) => {
                stmt.run(node_id, incomingJson, filename, (err) => err ? reject(err) : resolve());
            });

            count++;
            if (count % BATCH_SIZE === 0) {
                await dbRun(`RELEASE SAVEPOINT ${SP}`);
                await dbRun(`SAVEPOINT ${SP}`);
            }
        }

        await dbRun(`RELEASE SAVEPOINT ${SP}`);
    } catch (err) {
        // Undo this file's partial writes so the outer ROLLBACK sees a clean boundary.
        await dbRun(`ROLLBACK TO SAVEPOINT ${SP}`).catch(() => {});
        await dbRun(`RELEASE SAVEPOINT ${SP}`).catch(() => {});
        throw err;
    } finally {
        stmt.finalize();
    }

    logger.info(`Finished processing ${filename}: ${count} records`);
}

async function ingestNetworksInternal(specificFile = null) {
    if (!fs.existsSync(NETWORKS_DIR)) {
        logger.warn(`Networks directory not found: ${NETWORKS_DIR}`);
        return;
    }

    const files = specificFile ? [specificFile] : fs.readdirSync(NETWORKS_DIR).filter(f => f.endsWith('.csv'));

    for (const file of files) {
        logger.info(`Processing network file: ${file}`);
        const filePath = path.join(NETWORKS_DIR, file);

        if (!fs.existsSync(filePath)) {
            logger.warn(`File not found: ${filePath}`);
            continue;
        }

        const parser = fs.createReadStream(filePath).pipe(parse({
            columns: false,
            trim: true,
            skip_empty_lines: true,
            relax_column_count: true,
        }));

        let count = 0;
        const runId = ++networkIngestRunId;
        const stmt = db.prepare(`INSERT INTO edges (id, node1, node2, weight, source, attributes_json) VALUES (?, ?, ?, ?, ?, ?)`);
        const nodeStmt = db.prepare(`INSERT OR IGNORE INTO nodes (id, kind, attributes_json) VALUES (?, ?, ?)`);
        const networkNodeStmt = db.prepare(`INSERT OR IGNORE INTO network_nodes (source, node_id) VALUES (?, ?)`);

        try {
            await dbRun('BEGIN TRANSACTION');

            // Clear all existing edges for this source so that rows removed from
            // the CSV are not left behind (reconcile before replay).
            await dbRun('DELETE FROM edges WHERE source = ?', [file]);
            await dbRun('DELETE FROM network_nodes WHERE source = ?', [file]);

            // Use SAVEPOINTs for batch memory management so the outer transaction
            // is never committed mid-file. A failure at any point rolls back the
            // entire source (delete + all inserts), preserving the prior DB state.
            const SP = `net_batch_${file.replace(/\W/g, '_')}_${runId}`;
            await dbRun(`SAVEPOINT "${SP}"`);

            for await (const record of parser) {
                if (record.length < 3) continue;

                const node1 = record[0];
                const node2 = record[1];
                const weight = Number(record[2]);

                if (!node1 || !node2 || !Number.isFinite(weight)) continue;

                const [u, v] = [node1, node2].sort();
                const id = `${u}|${v}`;

                await new Promise((resolve, reject) => {
                    // Insert placeholder nodes before the edge so FK constraints are
                    // satisfied even when foreign_keys = ON (e.g. during hot-reload).
                    // INSERT OR IGNORE leaves attr-ingested rows untouched.
                    nodeStmt.run(u, 'protein', '{}', (err) => {
                        if (err) return reject(err);
                        nodeStmt.run(v, 'protein', '{}', (err) => {
                            if (err) return reject(err);
                            networkNodeStmt.run(file, u, (err) => {
                                if (err) return reject(err);
                                networkNodeStmt.run(file, v, (err) => {
                                    if (err) return reject(err);
                                    stmt.run(id, u, v, weight, file, '{}', (err) => {
                                        if (err) return reject(err);
                                        resolve();
                                    });
                                });
                            });
                        });
                    });
                });

                count++;
                if (count % BATCH_SIZE === 0) {
                    await dbRun(`RELEASE "${SP}"`);
                    await dbRun(`SAVEPOINT "${SP}"`);
                }
            }

            await dbRun(`RELEASE "${SP}"`);
            await dbRun('COMMIT');
            logger.info(`Finished processing ${file}: ${count} edges`);
        } catch (err) {
            await dbRun('ROLLBACK').catch(() => {});
            logger.error(`Failed processing ${file}: ${err.message}`);
            throw err;
        } finally {
            stmt.finalize();
            nodeStmt.finalize();
            networkNodeStmt.finalize();
        }
    }
}

async function ingestNetworks(specificFile = null) {
    // Keep the queue chain alive after failures so later ingestion requests still run.
    const run = networkIngestQueue.then(() => ingestNetworksInternal(specificFile));
    networkIngestQueue = run.catch(() => {});
    return run;
}

async function cleanupOrphans() {
    logger.info('Checking for orphan networks...');
    try {
        const rows = await new Promise((resolve, reject) => {
            db.all(`
                SELECT source FROM edges
                UNION
                SELECT source FROM network_nodes
            `, (err, rows) => {
                if (err) reject(err);
                else resolve(rows);
            });
        });

        for (const row of rows) {
            const source = row.source;
            const filePath = path.join(NETWORKS_DIR, source);

            if (!fs.existsSync(filePath)) {
                logger.warn(`Orphan network detected: ${source}. Removing from database...`);
                await new Promise((resolve, reject) => {
                    db.run('DELETE FROM edges WHERE source = ?', [source], (err) => {
                        if (err) reject(err);
                        else resolve();
                    });
                });
                await new Promise((resolve, reject) => {
                    db.run('DELETE FROM network_nodes WHERE source = ?', [source], (err) => {
                        if (err) reject(err);
                        else resolve();
                    });
                });
                logger.info(`Removed orphan network: ${source}`);
            }
        }
        logger.info('Orphan cleanup completed.');
    } catch (error) {
        logger.error('Error during orphan cleanup:', error);
    }
}

async function syncMissingNetworks(ingestFn = ingestNetworks) {
    if (!fs.existsSync(NETWORKS_DIR)) {
        logger.warn(`Networks directory not found: ${NETWORKS_DIR}`);
        return [];
    }

    const diskFiles = fs.readdirSync(NETWORKS_DIR).filter(f => f.endsWith('.csv'));
    if (diskFiles.length === 0) return [];

    const rows = await dbAll(`
        SELECT source FROM edges WHERE source IS NOT NULL
        UNION
        SELECT source FROM network_nodes WHERE source IS NOT NULL
    `);
    const knownSources = new Set(rows.map(row => row.source));
    const missingFiles = diskFiles.filter(file => !knownSources.has(file));

    for (const file of missingFiles) {
        logger.info(`Startup sync: ingesting network missing from database: ${file}`);
        await ingestFn(file);
    }

    return missingFiles;
}

// Run if called directly
if (require.main === module) {
    ingestData();
}

module.exports = {
    ingestData,
    ingestNodeAttributes,
    ingestNetworks,
    cleanupOrphans,
    syncMissingNetworks,
    resolveSingleNodeAttributeFile,
    validateNodeAttributeFiles,
    restoreIngestPragmas,
};
