const fs = require('fs');
const path = require('path');

const DEFAULT_LOCK_STALE_MS = 10 * 60 * 1000;
const DEFAULT_MAX_SUFFIX_ATTEMPTS = 1000;

function splitOutputFilename(filename) {
  const ext = path.extname(filename);
  const basename = path.basename(filename, ext);
  return { basename, ext };
}

function makeErrorFactory(ErrorClass) {
  return (message, status = 400, details = {}) => new ErrorClass(message, status, details);
}

function normalizeCsvOutputName(nameInput, { maxNameLength, errorFactory }) {
  if (typeof nameInput !== 'string' || nameInput.trim() === '') {
    throw errorFactory('name must be a non-empty string');
  }

  const trimmed = nameInput.trim();
  if (trimmed.length > maxNameLength) {
    throw errorFactory(`name is limited to ${maxNameLength} characters`);
  }

  const normalized = trimmed.toLowerCase().endsWith('.csv')
    ? `${trimmed.slice(0, -4)}.csv`
    : `${trimmed}.csv`;

  if (!/^[A-Za-z0-9_-][A-Za-z0-9_.-]*\.csv$/i.test(normalized)) {
    throw errorFactory(
      'name may contain only letters, numbers, ".", "_" and "-", and must resolve to a .csv filename'
    );
  }

  if (normalized === '.' || normalized === '..' || normalized.includes('/') || normalized.includes('\\')) {
    throw errorFactory('name must be a plain filename, not a path');
  }

  return normalized;
}

async function getExistingOutputFilenames(finalDir) {
  try {
    const existingFiles = await fs.promises.readdir(finalDir);
    return new Map(
      existingFiles
        .filter(name => !name.endsWith('.lock'))
        .map(name => [name.toLowerCase(), name])
    );
  } catch (err) {
    if (err.code === 'ENOENT') return new Map();
    throw err;
  }
}

async function clearStaleReservationLock(lockPath, existingFilenames, canonicalBasename, lockStaleMs) {
  try {
    const lockStat = await fs.promises.stat(lockPath);
    const lockAgeMs = Date.now() - lockStat.mtimeMs;
    if (lockAgeMs > lockStaleMs && !existingFilenames.has(canonicalBasename)) {
      await fs.promises.rm(lockPath, { force: true });
    }
  } catch (err) {
    if (err.code !== 'ENOENT') throw err;
  }
}

async function tryReserveOutputName(finalDir, finalBasename, options) {
  const {
    existingFilenames,
    errorFactory,
    lockStaleMs = DEFAULT_LOCK_STALE_MS,
    resourceLabel = 'network name',
  } = options;
  const canonicalBasename = finalBasename.toLowerCase();
  const lockPath = path.join(finalDir, `.${canonicalBasename}.lock`);

  await clearStaleReservationLock(lockPath, existingFilenames, canonicalBasename, lockStaleMs);

  try {
    const handle = await fs.promises.open(lockPath, 'wx');
    try {
      const conflictingFile = existingFilenames.get(canonicalBasename);
      if (conflictingFile) {
        throw errorFactory(`The ${resourceLabel} "${conflictingFile}" already exists`, 409);
      }
    } catch (err) {
      await handle.close().catch(() => {});
      await fs.promises.rm(lockPath, { force: true }).catch(() => {});
      throw err;
    }

    return {
      outputFilename: finalBasename,
      finalOutputPath: path.join(finalDir, finalBasename),
      handle,
      lockPath,
    };
  } catch (err) {
    if (err.code === 'EEXIST') return null;
    if (err.status === 409) return null;
    throw err;
  }
}

async function reserveOutputName(requestedOutputName, options) {
  const {
    finalDir,
    errorFactory,
    maxAttempts = DEFAULT_MAX_SUFFIX_ATTEMPTS,
    lockStaleMs = DEFAULT_LOCK_STALE_MS,
    resourceLabel = 'network name',
  } = options;
  const { basename, ext } = splitOutputFilename(requestedOutputName);
  const existingFilenames = await getExistingOutputFilenames(finalDir);

  for (let suffix = 0; suffix < maxAttempts; suffix += 1) {
    const candidateName = suffix === 0 ? requestedOutputName : `${basename}_${suffix}${ext}`;
    const reservation = await tryReserveOutputName(finalDir, candidateName, {
      existingFilenames,
      errorFactory,
      lockStaleMs,
      resourceLabel,
    });
    if (reservation) return reservation;
  }

  throw errorFactory(
    `Could not reserve a unique ${resourceLabel} after ${maxAttempts} attempts`,
    503
  );
}

async function createUniqueFile(requestedFilename, content, options) {
  const {
    finalDir,
    errorFactory,
    maxAttempts = DEFAULT_MAX_SUFFIX_ATTEMPTS,
    encoding = 'utf8',
    resourceLabel = 'filename',
  } = options;
  const { basename, ext } = splitOutputFilename(requestedFilename);
  const existingFilenames = await getExistingOutputFilenames(finalDir);

  for (let suffix = 0; suffix < maxAttempts; suffix += 1) {
    const filename = suffix === 0 ? requestedFilename : `${basename}_${suffix}${ext}`;
    const canonicalFilename = filename.toLowerCase();

    if (existingFilenames.has(canonicalFilename)) {
      continue;
    }

    const outputPath = path.join(finalDir, filename);
    try {
      const handle = await fs.promises.open(outputPath, 'wx');
      try {
        await handle.writeFile(content, encoding);
      } finally {
        await handle.close();
      }
      return filename;
    } catch (err) {
      if (err.code === 'EEXIST') {
        existingFilenames.set(canonicalFilename, filename);
        continue;
      }
      throw err;
    }
  }

  throw errorFactory(
    `Could not reserve a unique ${resourceLabel} after ${maxAttempts} attempts`,
    503
  );
}

function suppressWatcherIngest(filename, errorFactory) {
  try {
    const fileWatcher = require('../services/fileWatcher');
    if (fileWatcher && typeof fileWatcher.suppressNetworkIngest === 'function') {
      fileWatcher.suppressNetworkIngest(filename);
      return;
    }
    throw new Error('fileWatcher.suppressNetworkIngest is unavailable');
  } catch (err) {
    throw errorFactory(
      `Could not suppress watcher ingest for ${filename}: ${err.message}`,
      500
    );
  }
}

module.exports = {
  DEFAULT_MAX_SUFFIX_ATTEMPTS,
  createUniqueFile,
  makeErrorFactory,
  normalizeCsvOutputName,
  reserveOutputName,
  suppressWatcherIngest,
};
