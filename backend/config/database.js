const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const fs = require('fs');
const logger = require('../utils/logger');
const config = require('./config');

const dbPath = config.dbPath || path.join(__dirname, '../../data/network_viz.db');

// Ensure data directory exists
const dataDir = path.dirname(dbPath);
if (!fs.existsSync(dataDir)) {
  fs.mkdirSync(dataDir, { recursive: true });
}

const db = new sqlite3.Database(dbPath, (err) => {
  if (err) {
    logger.error('Could not connect to database', err);
  } else {
    logger.info('Connected to SQLite database');
    initializeSchema();
  }
});

function initializeSchema() {
  db.serialize(() => {
    // Nodes table
    db.run(`CREATE TABLE IF NOT EXISTS nodes (
      id TEXT PRIMARY KEY,
      kind TEXT NOT NULL, -- 'protein' or 'nh'
      attributes_json TEXT,
      attribute_source TEXT -- filename that last wrote attributes for this protein node
    )`);

    // Edges table
    // Storing canonical edges: node1 < node2
    // We include source in PK to allow same edge in multiple networks (files)
    db.run(`CREATE TABLE IF NOT EXISTS edges (
      id TEXT NOT NULL, -- canonical id: node1|node2
      node1 TEXT NOT NULL,
      node2 TEXT NOT NULL,
      weight REAL,
      source TEXT NOT NULL, -- filename or source identifier
      attributes_json TEXT,
      PRIMARY KEY (id, source),
      FOREIGN KEY(node1) REFERENCES nodes(id),
      FOREIGN KEY(node2) REFERENCES nodes(id)
    )`);

    // Explicit membership table for networks. Edge endpoints are populated here
    // during ingestion; edited networks may also include isolated visible nodes.
    db.run(`CREATE TABLE IF NOT EXISTS network_nodes (
      source TEXT NOT NULL,
      node_id TEXT NOT NULL,
      PRIMARY KEY (source, node_id),
      FOREIGN KEY(node_id) REFERENCES nodes(id)
    )`);

    // Migration: add attribute_source column to databases created before this column existed.
    // Only run ALTER TABLE when the column is actually missing; ignore nothing else.
    db.all(`PRAGMA table_info(nodes)`, (err, cols) => {
      if (!err && cols && !cols.some(c => c.name === 'attribute_source')) {
        db.run(`ALTER TABLE nodes ADD COLUMN attribute_source TEXT`);
      }
    });

    // Indexes for performance
    db.run(`CREATE INDEX IF NOT EXISTS idx_nodes_kind ON nodes(kind)`);
    db.run(`CREATE INDEX IF NOT EXISTS idx_edges_node1 ON edges(node1)`);
    db.run(`CREATE INDEX IF NOT EXISTS idx_edges_node2 ON edges(node2)`);
    db.run(`CREATE INDEX IF NOT EXISTS idx_edges_source ON edges(source)`);
    db.run(`CREATE INDEX IF NOT EXISTS idx_network_nodes_source ON network_nodes(source)`);
    db.run(`CREATE INDEX IF NOT EXISTS idx_network_nodes_node ON network_nodes(node_id)`);
    db.run(`CREATE INDEX IF NOT EXISTS idx_nodes_ncbi_txid
      ON nodes(CAST(json_extract(attributes_json, '$.NCBI_txID') AS TEXT))`);

    logger.info('Database schema initialized');
  });
}

module.exports = db;
