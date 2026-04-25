'use strict';

/**
 * species-tree.js  —  GET /api/species-tree
 *
 * Serves the annotated taxonomic tree built from:
 *   • config.taxonTreePath  (commontree.txt downloaded from NCBI Common Tree)
 *   • config.taxonNamesPath (NCBI_txID.csv — taxid ↔ species_name mapping)
 *
 * The parsed result is cached in memory for the lifetime of the server process.
 * POST /api/species-tree/invalidate clears the cache (useful after data
 * ingestion without a full restart).
 *
 * Both file paths default to data/NCBI_txID/ and can be overridden via the
 * environment variables TAXON_TREE_PATH and TAXON_NAMES_PATH, so users who
 * supply their own tree and mapping files never need to touch any code.
 */

const express            = require('express');
const router             = express.Router();
const fs                 = require('fs').promises;
const path               = require('path');
const util               = require('util');
const db                 = require('../config/database');
const config             = require('../config/config');
const { parseTaxonTree } = require('../utils/taxon-tree-parser');
const logger             = require('../utils/logger');

const dbAll = util.promisify(db.all.bind(db));

// ─── In-memory cache ──────────────────────────────────────────────────────────

let _cache = null;   // { tree, stats } once built

function invalidateCache() {
    _cache = null;
    logger.info('species-tree: cache invalidated');
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

/**
 * Check whether the required data files are readable.
 */
async function checkFiles() {
    const treePath  = config.taxonTreePath;
    const namesPath = config.taxonNamesPath;

    const [treeResult, namesResult] = await Promise.allSettled([
        fs.access(treePath,  fs.constants.R_OK),
        fs.access(namesPath, fs.constants.R_OK),
    ]);

    return {
        treeExists:  treeResult.status  === 'fulfilled',
        namesExists: namesResult.status === 'fulfilled',
        treePath,
        namesPath,
    };
}

/**
 * Query the active DB for every distinct NCBI_txID that has at least one
 * protein node.  These taxids receive isDbSpecies = true in the tree.
 *
 * The NCBI_txID is stored as a JSON field inside attributes_json:
 *   { …, "NCBI_txID": "9606", … }
 * An index on that expression already exists (idx_nodes_ncbi_txid).
 */
async function getDbTaxids() {
    const rows = await dbAll(`
        SELECT DISTINCT
            CAST(json_extract(attributes_json, '$.NCBI_txID') AS TEXT) AS taxid
        FROM nodes
        WHERE kind = 'protein'
          AND attributes_json IS NOT NULL
          AND json_extract(attributes_json, '$.NCBI_txID') IS NOT NULL
    `);
    return new Set(rows.map(r => String(r.taxid).trim()));
}

/**
 * Return the cached tree, building it first if necessary.
 * Throws with err.code = 'FILES_MISSING' when the input files are absent.
 */
async function getTree() {
    if (_cache) return _cache;

    const { treeExists, namesExists, treePath, namesPath } = await checkFiles();

    if (!treeExists || !namesExists) {
        const err = new Error(
            `Species tree input files not found — ` +
            `tree: ${treeExists}, names: ${namesExists}. ` +
            `Set TAXON_TREE_PATH and TAXON_NAMES_PATH in your .env, ` +
            `or place commontree.txt and NCBI_txID.csv in data/NCBI_txID/.`
        );
        err.code = 'FILES_MISSING';
        throw err;
    }

    logger.info('species-tree: building tree (first request or after cache invalidation)…');

    const dbTaxids       = await getDbTaxids();
    const { root, stats } = await parseTaxonTree(treePath, namesPath, dbTaxids);

    _cache = {
        tree:  root,
        stats: {
            ...stats,
            dbTaxidCount: dbTaxids.size,
            treeFile:     path.basename(treePath),
            namesFile:    path.basename(namesPath),
        },
    };

    logger.info(
        `species-tree: cache ready — ` +
        `${stats.totalNodes} nodes, ${stats.dbSpeciesNodes} DB species`
    );

    return _cache;
}

// ─── Routes ───────────────────────────────────────────────────────────────────

/**
 * GET /api/species-tree/status
 *
 * Lightweight availability check called by the frontend before it decides
 * whether to render the tree widget or fall back to the flat species list.
 * Always responds — never 4xx/5xx — so the frontend can branch safely.
 */
router.get('/status', async (req, res) => {
    try {
        const { treeExists, namesExists, treePath, namesPath } = await checkFiles();
        const available = treeExists && namesExists;

        const payload = {
            available,
            treeFile:       path.basename(treePath),
            namesFile:      path.basename(namesPath),
            treeFileFound:  treeExists,
            namesFileFound: namesExists,
        };

        // If the cache is already warm, include its stats at no extra cost
        if (available && _cache) {
            payload.stats = _cache.stats;
        }

        res.json(payload);
    } catch (err) {
        logger.error(`species-tree /status error: ${err.message}`);
        // Still respond 200 so the frontend can read available:false
        res.json({ available: false, error: err.message });
    }
});

/**
 * GET /api/species-tree
 *
 * Returns the full annotated tree as JSON.
 * Response: { tree: <root node>, stats: { … } }
 *
 * Each node in the tree has the shape:
 *   {
 *     name:        string,       // taxon name from commontree.txt
 *     taxid:       string|null,  // NCBI taxonomy ID, null if not found in CSV
 *     isDbSpecies: boolean,      // true  → has proteins in the active DB
 *     children:    Node[]        // empty array for leaf species
 *   }
 */
router.get('/', async (req, res) => {
    try {
        const { tree, stats } = await getTree();
        res.json({ tree, stats });
    } catch (err) {
        if (err.code === 'FILES_MISSING') {
            return res.status(404).json({ available: false, error: err.message });
        }
        logger.error(`species-tree / error: ${err.message}`);
        res.status(500).json({ error: err.message });
    }
});

/**
 * POST /api/species-tree/invalidate
 *
 * Clears the in-memory cache so the tree is rebuilt on the next GET request.
 * Call this after ingesting new network data without restarting the server,
 * so that newly added species immediately appear with isDbSpecies = true.
 */
router.post('/invalidate', (req, res) => {
    invalidateCache();
    res.json({ ok: true, message: 'Species tree cache cleared. Tree will be rebuilt on next request.' });
});

module.exports = router;
module.exports.invalidateCache = invalidateCache;
