const chokidar = require('chokidar');
const path = require('path');
const logger = require('../utils/logger');
const config = require('../config/config');
const { ingestNodeAttributes, ingestNetworks, resolveSingleNodeAttributeFile } = require('../scripts/ingestData');
const speciesTreeRoute = require('../routes/species-tree');

const NETWORKS_DIR = config.dataPath;
const NODES_ATTR_DIR = config.nodeAttributesPath;
const TAXON_TREE_PATH = path.resolve(config.taxonTreePath);
const TAXON_NAMES_PATH = path.resolve(config.taxonNamesPath);

class FileWatcher {
    constructor() {
        this.watcher = null;
        this.suppressedNetworkIngests = new Map();
    }

    pruneExpiredSuppressions(now = Date.now()) {
        for (const [key, suppression] of this.suppressedNetworkIngests) {
            if (now > suppression.expiresAt || suppression.remainingEvents <= 0) {
                this.suppressedNetworkIngests.delete(key);
            }
        }
    }

    suppressNetworkIngest(
        filename,
        {
            ttlMs = config.networkEdit?.watcherSuppressMs || 300000,
            eventCount = config.networkEdit?.watcherSuppressEvents || 3,
        } = {}
    ) {
        this.pruneExpiredSuppressions();
        const key = path.basename(filename).toLowerCase();
        this.suppressedNetworkIngests.set(key, {
            expiresAt: Date.now() + ttlMs,
            remainingEvents: eventCount,
        });
    }

    shouldSuppressNetworkIngest(filename) {
        const now = Date.now();
        this.pruneExpiredSuppressions(now);

        const key = path.basename(filename).toLowerCase();
        const suppression = this.suppressedNetworkIngests.get(key);
        if (!suppression) return false;

        suppression.remainingEvents -= 1;
        if (suppression.remainingEvents <= 0) {
            this.suppressedNetworkIngests.delete(key);
        }

        return true;
    }

    initialize() {
        logger.info('Initializing File Watcher Service...');
        logger.info(`FileWatcher: Watching directories/files:\n - ${NETWORKS_DIR}\n - ${NODES_ATTR_DIR}\n - ${TAXON_TREE_PATH}\n - ${TAXON_NAMES_PATH}`);

        this.watcher = chokidar.watch([
            NETWORKS_DIR,
            NODES_ATTR_DIR,
            TAXON_TREE_PATH,
            TAXON_NAMES_PATH,
        ], {
            persistent: true,
            ignoreInitial: true,
            usePolling: true, // Force polling for better compatibility
            interval: 1000,
            awaitWriteFinish: {
                stabilityThreshold: 2000,
                pollInterval: 100
            }
        });

        this.watcher
            .on('add', path => this.handleFileChange(path, 'added'))
            .on('change', path => this.handleFileChange(path, 'changed'))
            .on('unlink', path => this.handleFileRemove(path))
            .on('error', error => logger.error(`FileWatcher Error: ${error}`));

        logger.info(`FileWatcher initialized.`);
    }

    async handleFileChange(filePath, eventType) {
        const filename = path.basename(filePath);
        const resolvedPath = path.resolve(filePath);
        logger.info(`File ${eventType}: ${filename}.`);

        try {
            if (resolvedPath === TAXON_TREE_PATH || resolvedPath === TAXON_NAMES_PATH) {
                speciesTreeRoute.invalidateCache();
                logger.info(`Species tree cache invalidated after taxonomy file ${eventType}: ${filename}`);
            } else if (filename.endsWith('.csv')) {
                if (this.shouldSuppressNetworkIngest(filename)) {
                    logger.info(`Skipping watcher-triggered ingest for internally saved network ${filename}`);
                    return;
                }
                logger.info(`Triggering network ingestion for ${filename}...`);
                await ingestNetworks(filename);
                speciesTreeRoute.invalidateCache();
                logger.info(`Successfully updated network database for ${filename}`);
            } else if (filename.endsWith('.nodes.attr')) {
                const files = resolveSingleNodeAttributeFile();
                logger.info(`Triggering attribute re-ingestion for ${files[0]}`);
                await ingestNodeAttributes(files);
                speciesTreeRoute.invalidateCache();
                logger.info(`Successfully updated attribute database for ${files[0]}`);
            }
        } catch (error) {
            logger.error(`Error processing ${filename}:`, error);
        }
    }

    async handleFileRemove(filePath) {
        const filename = path.basename(filePath);
        const resolvedPath = path.resolve(filePath);
        logger.info(`File removed: ${filename}.`);

        try {
            if (resolvedPath === TAXON_TREE_PATH || resolvedPath === TAXON_NAMES_PATH) {
                speciesTreeRoute.invalidateCache();
                logger.warn(`Species taxonomy file removed: ${filename}. Tree cache invalidated.`);
            } else if (filename.endsWith('.csv')) {
                const db = require('../config/database');
                const util = require('util');
                const dbRun = util.promisify(db.run.bind(db));

                try {
                    await dbRun('BEGIN TRANSACTION');
                    await dbRun('DELETE FROM network_nodes WHERE source = ?', [filename]);
                    await dbRun('DELETE FROM edges WHERE source = ?', [filename]);
                    await dbRun('COMMIT');
                    speciesTreeRoute.invalidateCache();
                    logger.info(`Successfully removed network ${filename} from database.`);
                } catch (err) {
                    await dbRun('ROLLBACK').catch(() => {});
                    logger.error(`Error deleting network ${filename}:`, err);
                }
            } else if (filename.endsWith('.nodes.attr')) {
                try {
                    const files = resolveSingleNodeAttributeFile();
                    logger.info(`Attribute file removed; re-ingesting remaining file ${files[0]}`);
                    await ingestNodeAttributes(files);
                    speciesTreeRoute.invalidateCache();
                    logger.info(`Successfully updated attribute database for ${files[0]}`);
                } catch (err) {
                    logger.error(`Node attribute folder is invalid after removing ${filename}: ${err.message}`);
                }
            }
        } catch (error) {
            logger.error(`Error handling removal of ${filename}:`, error);
        }
    }
}

module.exports = new FileWatcher();
