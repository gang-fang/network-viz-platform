require('dotenv').config();

const config = require('./config/config');
const logger = require('./utils/logger');

// Check whether the DB has any data (first-run detection)
async function isFirstRun() {
    const db = require('./config/database');
    return new Promise((resolve) => {
        db.get('SELECT COUNT(*) AS count FROM nodes', (err, row) => {
            if (err) resolve(true); // treat schema error as first run
            else resolve(row.count === 0);
        });
    });
}

async function runIngest() {
    logger.info('Running data ingestion...');
    const { ingestData } = require('./scripts/ingestData');
    await ingestData();
}

async function runServe() {
    if (config.fileWatchEnabled) {
        const { cleanupOrphans } = require('./scripts/ingestData');
        const fileWatcher = require('./services/fileWatcher');
        try {
            await cleanupOrphans();
        } catch (err) {
            logger.warn(`Orphan cleanup failed, continuing: ${err.message}`);
        }
        fileWatcher.initialize();
    } else {
        logger.info('File watching disabled (FILE_WATCH_ENABLED=false)');
    }
    require('./server');
}

async function main() {
    const mode = config.startMode;
    logger.info(`Startup mode: ${mode}`);

    if (mode === 'ingest') {
        await runIngest();
        process.exit(0);
    }

    if (mode === 'ingest-and-serve') {
        await runIngest();
        await runServe();
        return;
    }

    // Default: serve — but auto-ingest on first run if DB is empty
    if (await isFirstRun()) {
        logger.info('Empty database detected — running first-run bootstrap ingestion...');
        await runIngest();
    }

    await runServe();
}

main().catch((err) => {
    logger.error(`Startup failed: ${err.message}`);
    process.exit(1);
});
