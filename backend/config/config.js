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
  dataPath:           process.env.DATA_PATH            || path.join(__dirname, '../../data/networks'),
  exportsPath:        process.env.EXPORTS_PATH         || path.join(__dirname, '../../data/exports'),
  indexesPath:        process.env.INDEXES_PATH         || path.join(__dirname, '../../data/indexes'),
  nodeAttributesPath: process.env.NODE_ATTRIBUTES_PATH || path.join(__dirname, '../../data/nodes_attr'),
  tempDataPath:       process.env.TEMP_DATA_PATH       || path.join(__dirname, '../../data/tmp'),

  // ── Species / taxonomy ────────────────────────────────────────────────────
  //
  // speciesPath    (legacy, kept for /api/species-names backward compat)
  //   The CSV that drives the flat species-name list.  After consolidation
  //   this points to the same file as taxonNamesPath.
  //
  // taxonNamesPath
  //   NCBI_txID.csv — two-column CSV with header "ncbi_txid,species_name".
  //   Contains ALL taxids found in commontree.txt (internal ranks + leaves).
  //   Users replace this file with their own NCBI download; no code changes
  //   are required — just set TAXON_NAMES_PATH in the .env file.
  //
  // taxonTreePath
  //   commontree.txt — ASCII tree downloaded from the NCBI Common Tree tool.
  //   Users replace this file with their own download; set TAXON_TREE_PATH.
  //
  // Both files default to data/NCBI_txID/ which is where NCBI downloads land
  // after the standard setup instructions.
  // ─────────────────────────────────────────────────────────────────────────
  speciesPath:    process.env.SPECIES_PATH     || path.join(__dirname, '../../data/NCBI_txID/NCBI_txID.csv'),
  taxonNamesPath: process.env.TAXON_NAMES_PATH || path.join(__dirname, '../../data/NCBI_txID/NCBI_txID.csv'),
  taxonTreePath:  process.env.TAXON_TREE_PATH  || path.join(__dirname, '../../data/NCBI_txID/commontree.txt'),

  // File watching (set FILE_WATCH_ENABLED=false to disable)
  fileWatchEnabled: process.env.FILE_WATCH_ENABLED !== 'false',

  // UniProt API configuration
  uniprotApi: {
    baseUrl:          'https://rest.uniprot.org/uniprotkb/',
    timeout:          10000,
    retryAttempts:    3,
    cacheExpiry:      86400000,
    batchLimit:       100,
    concurrencyLimit: 5,
    cacheMaxEntries:  1000,
  },

  // Subnetwork extraction
  subnetwork: {
    pythonCommand:    process.env.PYTHON_COMMAND               || 'python3',
    scriptPath:       process.env.SUBNETWORK_SCRIPT_PATH       || path.join(__dirname, '../../tools/runtime/extract_subnetwork.py'),
    jobTempPath:      process.env.SUBNETWORK_JOB_TEMP_PATH     || path.join(__dirname, '../../data/tmp/subnetwork-jobs'),
    maxSeedCount:     parseInt(process.env.SUBNETWORK_MAX_SEEDS,          10) || 10,
    maxInputLength:   parseInt(process.env.SUBNETWORK_MAX_INPUT_LENGTH,   10) || 2000,
    maxNameLength:    parseInt(process.env.SUBNETWORK_MAX_NAME_LENGTH,    10) || 80,
    defaultMaxNodes:  parseInt(process.env.SUBNETWORK_DEFAULT_MAX_NODES,  10) || 500,
    maxNodesLimit:    parseInt(process.env.SUBNETWORK_MAX_NODES_LIMIT,    10) || 2500,
    timeoutMs:        parseInt(process.env.SUBNETWORK_TIMEOUT_MS,         10) || 120000,
    maxConcurrentJobs: parseInt(process.env.SUBNETWORK_MAX_CONCURRENT_JOBS, 10) || 2,
  },

  // Edited network saves
  networkEdit: {
    maxNameLength:       parseInt(process.env.NETWORK_EDIT_MAX_NAME_LENGTH,      10) || 80,
    maxSuffixAttempts:   parseInt(process.env.NETWORK_EDIT_MAX_SUFFIX_ATTEMPTS,  10) || 1000,
    watcherSuppressMs:   parseInt(process.env.NETWORK_EDIT_WATCHER_SUPPRESS_MS,  10) || 300000,
    watcherSuppressEvents: parseInt(process.env.NETWORK_EDIT_WATCHER_SUPPRESS_EVENTS, 10) || 3,
  },

  // Logging configuration
  logging: {
    level: process.env.LOG_LEVEL || 'info',
    file:  process.env.LOG_FILE  || 'logs/server.log',
  },
};

module.exports = config;
