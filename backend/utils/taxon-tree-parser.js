'use strict';

/**
 * taxon-tree-parser.js
 *
 * Parses an NCBI Common Tree ASCII file (commontree.txt) into a nested JSON
 * tree and annotates every node with its NCBI taxonomy ID (sourced from
 * NCBI_txID.csv) and a flag indicating whether the species has proteins in
 * the current database.
 *
 * Both input file paths are passed in from config so users can supply their
 * own files without touching any code.
 */

const fs      = require('fs').promises;
const { parse } = require('csv-parse/sync');
const logger  = require('./logger');

// ─── Line parser ─────────────────────────────────────────────────────────────

/**
 * Parse one line of NCBI Common Tree ASCII format.
 *
 * Structure: each depth level prepends exactly 2 characters to the prefix
 * before the 2-character branch indicator ([+\][+-]).
 * The root line has no branch indicator (depth 0).
 *
 * Examples
 *   "cellular organisms"          → { depth: 0, name: "cellular organisms" }
 *   "+-Eukaryota"                 → { depth: 1, name: "Eukaryota" }
 *   "| +-Metamonada"              → { depth: 2, name: "Metamonada" }
 *   "| | ++Parabasalia"           → { depth: 3, name: "Parabasalia" }
 *   "  ++Fusobacteriati"          → { depth: 2, name: "Fusobacteriati" }
 *   "  |   ++Fusobacteriaceae"    → { depth: 4, name: "Fusobacteriaceae" }
 *
 * @param  {string} line
 * @returns {{ depth: number, name: string } | null}
 */
function parseLine(line) {
    // Skip empty lines and the trailing "---" separator
    if (!line || /^\s*$/.test(line) || /^[\s\-]+$/.test(line)) return null;

    // Find the index of the first letter, digit, or '[' — that is where the
    // taxon name begins.
    const nameStart = line.search(/[A-Za-z0-9\[]/);
    if (nameStart === -1) return null;

    const name   = line.slice(nameStart).trim();
    const prefix = line.slice(0, nameStart);   // e.g. "| | +-" or "  |   ++"

    if (!name) return null;

    // Root node: no branch indicator prefix at all
    if (prefix.length === 0) return { depth: 0, name };

    // The last 2 characters of `prefix` are always the branch indicator
    // (one of: +-, ++, \-, \+).  Everything before them is the indent and
    // contributes exactly 2 characters per depth level.
    const indent = prefix.slice(0, -2);
    const depth  = Math.floor(indent.length / 2) + 1;

    return { depth, name };
}

// ─── Tree builder ─────────────────────────────────────────────────────────────

/**
 * Convert commontree.txt text into a nested object tree.
 *
 * @param  {string} text
 * @returns {{ name: string, children: Array, taxid: null, isDbSpecies: false }}
 */
function buildTree(text) {
    const lines = text.split(/\r?\n/);
    // stack[depth] always holds the most-recently-seen node at that depth,
    // which is by definition the correct parent for depth+1 nodes.
    const stack = [];
    let root = null;

    for (const line of lines) {
        const parsed = parseLine(line);
        if (!parsed) continue;

        const { depth, name } = parsed;
        const node = {
            name,
            children:    [],
            taxid:       null,
            isDbSpecies: false,
        };

        stack[depth] = node;

        if (depth === 0) {
            root = node;
        } else {
            const parent = stack[depth - 1];
            if (parent) {
                parent.children.push(node);
            } else {
                logger.warn(
                    `taxon-tree-parser: orphan node "${name}" at depth ${depth} ` +
                    `(no parent at depth ${depth - 1}) — skipped`
                );
            }
        }
    }

    return root;
}

// ─── Name → taxid map ────────────────────────────────────────────────────────

/**
 * Load NCBI_txID.csv and return two-way lookup maps.
 *
 * Expected CSV columns (header required): ncbi_txid, species_name
 * The header column names are flexible — see below.
 *
 * @param  {string} csvPath
 * @returns {{ nameToTaxid: Map<string,string> }}
 */
async function loadNameMap(csvPath) {
    const text = await fs.readFile(csvPath, 'utf8');

    const records = parse(text, {
        columns:                 true,   // first row is header
        skip_empty_lines:        true,
        trim:                    true,
        skip_records_with_error: true,
    });

    const nameToTaxid = new Map();   // normalised-name → taxid

    for (const row of records) {
        // Accept both the project's standard column names and NCBI's originals
        const taxid = String(row.ncbi_txid  || row.tax_id   || '').trim();
        const name  = String(row.species_name || row.name_txt || '').trim();
        if (!taxid || !name) continue;

        const normalizedName = name.toLowerCase();
        if (nameToTaxid.has(normalizedName) && nameToTaxid.get(normalizedName) !== taxid) {
            logger.warn(
                `taxon-tree-parser: duplicate lowercase species name "${normalizedName}" ` +
                `maps to both ${nameToTaxid.get(normalizedName)} and ${taxid}; using last value`
            );
        }

        nameToTaxid.set(normalizedName, taxid);
    }

    logger.info(
        `taxon-tree-parser: name map loaded — ${nameToTaxid.size} entries from ${csvPath}`
    );
    return { nameToTaxid };
}

// ─── Annotation ───────────────────────────────────────────────────────────────

/**
 * Recursively walk the tree and attach taxid + isDbSpecies to every node.
 *
 * @param {object}          node
 * @param {Map<string,string>} nameToTaxid  normalised name → taxid
 * @param {Set<string>}     dbTaxids        taxids present in the DB
 */
function annotateTree(node, nameToTaxid, dbTaxids) {
    const key = node.name.toLowerCase();

    if (nameToTaxid.has(key)) {
        node.taxid       = nameToTaxid.get(key);
        node.isDbSpecies = dbTaxids.has(node.taxid);
    }

    for (const child of node.children) {
        annotateTree(child, nameToTaxid, dbTaxids);
    }
}

// ─── Stats ────────────────────────────────────────────────────────────────────

function collectStats(node) {
    let total = 0, annotated = 0, dbSpecies = 0;
    function walk(n) {
        total++;
        if (n.taxid)       annotated++;
        if (n.isDbSpecies) dbSpecies++;
        for (const c of n.children) walk(c);
    }
    walk(node);
    return { totalNodes: total, annotatedNodes: annotated, dbSpeciesNodes: dbSpecies };
}

// ─── Public API ───────────────────────────────────────────────────────────────

/**
 * Parse and annotate the taxonomic tree.
 *
 * @param {string}      treePath   Path to commontree.txt
 * @param {string}      csvPath    Path to NCBI_txID.csv
 * @param {Set<string>} dbTaxids   Taxids that have proteins in the active DB
 * @returns {Promise<{ root: object, stats: object }>}
 */
async function parseTaxonTree(treePath, csvPath, dbTaxids) {
    const [treeText, { nameToTaxid }] = await Promise.all([
        fs.readFile(treePath, 'utf8'),
        loadNameMap(csvPath),
    ]);

    const root = buildTree(treeText);
    if (!root) throw new Error('taxon-tree-parser: could not find a root node in the tree file');

    annotateTree(root, nameToTaxid, dbTaxids);

    const stats = collectStats(root);
    logger.info(
        `taxon-tree-parser: done — ` +
        `${stats.totalNodes} nodes, ${stats.annotatedNodes} annotated, ` +
        `${stats.dbSpeciesNodes} DB species`
    );

    return { root, stats };
}

module.exports = { parseTaxonTree };
