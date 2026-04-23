require('dotenv').config();
const path = require('path');

const config = {
  // Server configuration
  port: parseInt(process.env.PORT, 10) || 3000,
  nodeEnv: process.env.NODE_ENV || 'development',

  // Startup mode: 'serve' | 'ingest' | 'ingest-and-serve'
  startMode: process.env.START_MODE || 'serve',

  // CORS settings
  corsOrigin: process.env.CORS_ORIGIN || '*',

  // Database path (override for Docker volumes)
  dbPath: process.env.DB_PATH || path.join(__dirname, '../../data/network_viz.db'),

  // Data paths
  dataPath: process.env.DATA_PATH || path.join(__dirname, '../../data/networks'),
  nodeAttributesPath: process.env.NODE_ATTRIBUTES_PATH || path.join(__dirname, '../../data/nodes_attr'),
  speciesPath: process.env.SPECIES_PATH || path.join(__dirname, '../../data/NCBI_txID/NCBI_txID.csv'),

  // Explicit list of .nodes.attr filenames to ingest (required for attribute ingestion)
  // e.g. NODE_ATTRIBUTE_FILES=e.nodes.attr,p.nodes.attr
  nodeAttributeFiles: process.env.NODE_ATTRIBUTE_FILES
    ? process.env.NODE_ATTRIBUTE_FILES.split(',').map(f => f.trim()).filter(Boolean)
    : [],

  // File watching (set FILE_WATCH_ENABLED=false to disable)
  fileWatchEnabled: process.env.FILE_WATCH_ENABLED !== 'false',

  // UniProt API configuration
  uniprotApi: {
    baseUrl: 'https://rest.uniprot.org/uniprotkb/',
    timeout: 10000, // 10 seconds
    retryAttempts: 3,
    cacheExpiry: 86400000, // 24 hours in milliseconds
    batchLimit: 100,
    concurrencyLimit: 5,
    cacheMaxEntries: 1000,
  },

  // Logging configuration
  logging: {
    level: process.env.LOG_LEVEL || 'info',
    file: process.env.LOG_FILE || 'logs/server.log',
  },
};

module.exports = config;
