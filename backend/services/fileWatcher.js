const chokidar = require('chokidar');
const path = require('path');
const logger = require('../utils/logger');
const config = require('../config/config');
const { ingestNodeAttributes, ingestNetworks } = require('../scripts/ingestData');

const NETWORKS_DIR = config.dataPath;
const NODES_ATTR_DIR = config.nodeAttributesPath;

class FileWatcher {
    constructor() {
        this.watcher = null;
    }

    initialize() {
        logger.info('Initializing File Watcher Service...');
        logger.info(`FileWatcher: Watching directories:\n - ${NETWORKS_DIR}\n - ${NODES_ATTR_DIR}`);

        this.watcher = chokidar.watch([
            NETWORKS_DIR,
            NODES_ATTR_DIR
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
        logger.info(`File ${eventType}: ${filename}.`);

        try {
            if (filename.endsWith('.csv')) {
                logger.info(`Triggering network ingestion for ${filename}...`);
                await ingestNetworks(filename);
                logger.info(`Successfully updated network database for ${filename}`);
            } else if (filename.endsWith('.nodes.attr')) {
                const configured = config.nodeAttributeFiles;
                if (!configured.includes(filename)) {
                    logger.info(
                        `Ignoring attr file "${filename}" — not listed in NODE_ATTRIBUTE_FILES ` +
                        `(configured: [${configured.join(', ')}])`
                    );
                    return;
                }
                // Re-ingest the complete configured set so the preflight can validate
                // the full selection, not just the changed file in isolation.
                logger.info(`Triggering attribute re-ingestion for configured set: [${configured.join(', ')}]`);
                await ingestNodeAttributes(configured);
                logger.info(`Successfully updated attribute database for configured set`);
            }
        } catch (error) {
            logger.error(`Error processing ${filename}:`, error);
        }
    }

    async handleFileRemove(filePath) {
        const filename = path.basename(filePath);
        logger.info(`File removed: ${filename}.`);

        try {
            if (filename.endsWith('.csv')) {
                const db = require('../config/database');
                // Delete edges associated with this source
                db.run('DELETE FROM edges WHERE source = ?', [filename], (err) => {
                    if (err) logger.error(`Error deleting network ${filename}:`, err);
                    else logger.info(`Successfully removed network ${filename} from database.`);
                });
            } else if (filename.endsWith('.nodes.attr')) {
                const configured = config.nodeAttributeFiles;
                if (configured.includes(filename)) {
                    logger.warn(
                        `Configured attr file "${filename}" was removed from disk. ` +
                        `Remove it from NODE_ATTRIBUTE_FILES and restart, or restore the file.`
                    );
                } else {
                    logger.info(`Removed attr file "${filename}" was not in NODE_ATTRIBUTE_FILES — no action needed.`);
                }
            }
        } catch (error) {
            logger.error(`Error handling removal of ${filename}:`, error);
        }
    }
}

module.exports = new FileWatcher();
