const fs = require('fs');
const path = require('path');
const util = require('util');
const db = require('../config/database');
const config = require('../config/config');
const logger = require('../utils/logger');
const { ingestNetworks } = require('../scripts/ingestData');

const dbAll = util.promisify(db.all.bind(db));
const dbRun = util.promisify(db.run.bind(db));

const LOCK_STALE_MS = 10 * 60 * 1000;
const MAX_HIDDEN_NODE_IDS = 200000;
const MAX_HIDDEN_EDGE_IDS = 20000;
const MAX_HIDDEN_EDGE_WEIGHT_RANGES = 1000;
const MEMBERSHIP_INSERT_CHUNK_SIZE = 400;

function getMaxNameLength() {
  return config.networkEdit?.maxNameLength || 80;
}

function getMaxSuffixAttempts() {
  return config.networkEdit?.maxSuffixAttempts || 1000;
}

class NetworkEditError extends Error {
  constructor(message, status = 400, details = {}) {
    super(message);
    this.name = 'NetworkEditError';
    this.status = status;
    this.details = details;
  }
}

function normalizeOutputName(nameInput) {
  if (typeof nameInput !== 'string' || nameInput.trim() === '') {
    throw new NetworkEditError('name must be a non-empty string');
  }

  const trimmed = nameInput.trim();
  const maxNameLength = getMaxNameLength();
  if (trimmed.length > maxNameLength) {
    throw new NetworkEditError(`name is limited to ${maxNameLength} characters`);
  }

  const normalized = trimmed.toLowerCase().endsWith('.csv')
    ? `${trimmed.slice(0, -4)}.csv`
    : `${trimmed}.csv`;

  if (!/^[A-Za-z0-9_-][A-Za-z0-9_.-]*\.csv$/i.test(normalized)) {
    throw new NetworkEditError(
      'name may contain only letters, numbers, ".", "_" and "-", and must resolve to a .csv filename'
    );
  }

  if (normalized === '.' || normalized === '..' || normalized.includes('/') || normalized.includes('\\')) {
    throw new NetworkEditError('name must be a plain filename, not a path');
  }

  return normalized;
}

function normalizeHiddenNodeIds(value) {
  if (value === undefined || value === null) return [];
  if (!Array.isArray(value)) {
    throw new NetworkEditError('hiddenNodeIds must be an array');
  }
  if (value.length > MAX_HIDDEN_NODE_IDS) {
    throw new NetworkEditError(`hiddenNodeIds is limited to ${MAX_HIDDEN_NODE_IDS} items`);
  }
  if (!value.every(item => typeof item === 'string' && item.trim() !== '')) {
    throw new NetworkEditError('hiddenNodeIds must contain only non-empty strings');
  }
  return Array.from(new Set(value.map(item => item.trim())));
}

function normalizeHiddenEdgeIds(value) {
  if (value === undefined || value === null) return [];
  if (!Array.isArray(value)) {
    throw new NetworkEditError('hiddenEdgeIds must be an array');
  }
  if (value.length > MAX_HIDDEN_EDGE_IDS) {
    throw new NetworkEditError(`hiddenEdgeIds is limited to ${MAX_HIDDEN_EDGE_IDS} items`);
  }
  if (!value.every(item => typeof item === 'string' && item.trim() !== '')) {
    throw new NetworkEditError('hiddenEdgeIds must contain only non-empty strings');
  }
  return Array.from(new Set(value.map(normalizeEdgeIdInput)));
}

function normalizeHiddenEdgeWeightRanges(value) {
  if (value === undefined || value === null) return [];
  if (!Array.isArray(value)) {
    throw new NetworkEditError('hiddenEdgeWeightRanges must be an array');
  }
  if (value.length > MAX_HIDDEN_EDGE_WEIGHT_RANGES) {
    throw new NetworkEditError(`hiddenEdgeWeightRanges is limited to ${MAX_HIDDEN_EDGE_WEIGHT_RANGES} items`);
  }

  return value.map(range => {
    if (!range || typeof range !== 'object' || Array.isArray(range)) {
      throw new NetworkEditError('hiddenEdgeWeightRanges must contain range objects');
    }

    const min = Number(range.min);
    const max = Number(range.max);
    if (!Number.isFinite(min) || !Number.isFinite(max) || min < 0 || max <= min || max >= 1) {
      throw new NetworkEditError('hiddenEdgeWeightRanges entries must satisfy 0 <= min < max < 1');
    }

    return { min, max };
  });
}

function getEdgeId(node1, node2) {
  return [String(node1), String(node2)].sort().join('|');
}

function normalizeEdgeIdInput(value) {
  const trimmed = value.trim();
  const parts = trimmed.split('|');
  if (parts.length !== 2 || !parts[0].trim() || !parts[1].trim()) {
    throw new NetworkEditError('hiddenEdgeIds must contain edge IDs in "node1|node2" format');
  }
  return getEdgeId(parts[0].trim(), parts[1].trim());
}

function isEdgeHiddenByWeightRanges(edge, ranges) {
  if (!ranges || ranges.length === 0) return false;
  const weight = Number(edge.weight);
  return Number.isFinite(weight) && ranges.some(range => weight >= range.min && weight < range.max);
}

function splitOutputFilename(filename) {
  const ext = path.extname(filename);
  const basename = path.basename(filename, ext);
  return { basename, ext };
}

async function hasConflictingNetworkFile(finalDir, canonicalBasename) {
  try {
    const existingFiles = await fs.promises.readdir(finalDir);
    return existingFiles.some(name =>
      !name.endsWith('.lock') && name.toLowerCase() === canonicalBasename
    );
  } catch (err) {
    if (err.code === 'ENOENT') return false;
    throw err;
  }
}

async function clearStaleReservationLock(lockPath, finalDir, canonicalBasename) {
  try {
    const lockStat = await fs.promises.stat(lockPath);
    const lockAgeMs = Date.now() - lockStat.mtimeMs;
    if (lockAgeMs > LOCK_STALE_MS && !(await hasConflictingNetworkFile(finalDir, canonicalBasename))) {
      await fs.promises.rm(lockPath, { force: true });
    }
  } catch (err) {
    if (err.code !== 'ENOENT') throw err;
  }
}

async function tryReserveOutputName(finalDir, finalBasename) {
  const canonicalBasename = finalBasename.toLowerCase();
  const lockPath = path.join(finalDir, `.${canonicalBasename}.lock`);

  await clearStaleReservationLock(lockPath, finalDir, canonicalBasename);

  try {
    const handle = await fs.promises.open(lockPath, 'wx');
    try {
      if (await hasConflictingNetworkFile(finalDir, canonicalBasename)) {
        throw new NetworkEditError(`A network named "${finalBasename}" already exists`, 409);
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
    if (err instanceof NetworkEditError && err.status === 409) return null;
    throw err;
  }
}

async function reserveOutputName(requestedOutputName) {
  const finalDir = config.dataPath;
  const { basename, ext } = splitOutputFilename(requestedOutputName);
  const maxAttempts = getMaxSuffixAttempts();

  for (let suffix = 0; suffix < maxAttempts; suffix += 1) {
    const candidateName = suffix === 0 ? requestedOutputName : `${basename}_${suffix}${ext}`;
    const reservation = await tryReserveOutputName(finalDir, candidateName);
    if (reservation) return reservation;
  }

  throw new NetworkEditError(
    `Could not reserve a unique network name after ${maxAttempts} attempts`,
    503
  );
}

async function getSourceNodeIds(source) {
  const rows = await dbAll(`
    SELECT node_id AS id FROM network_nodes WHERE source = ?
    UNION
    SELECT node1 AS id FROM edges WHERE source = ?
    UNION
    SELECT node2 AS id FROM edges WHERE source = ?
  `, [source, source, source]);

  return rows.map(row => String(row.id));
}

async function getSourceEdges(source) {
  return dbAll(
    'SELECT id, node1, node2, weight FROM edges WHERE source = ?',
    [source]
  );
}

function csvCell(value) {
  const text = String(value ?? '');
  if (!/[",\r\n]/.test(text)) return text;
  return `"${text.replace(/"/g, '""')}"`;
}

function edgeToCsv(edge) {
  return [edge.node1, edge.node2, edge.weight].map(csvCell).join(',');
}

async function insertNetworkMembership(source, nodeIds) {
  if (!nodeIds || nodeIds.length === 0) return;

  await dbRun('BEGIN TRANSACTION');
  try {
    for (let i = 0; i < nodeIds.length; i += MEMBERSHIP_INSERT_CHUNK_SIZE) {
      const chunk = nodeIds.slice(i, i + MEMBERSHIP_INSERT_CHUNK_SIZE);
      const placeholders = chunk.map(() => '(?, ?)').join(',');
      const params = chunk.flatMap(nodeId => [source, nodeId]);
      await dbRun(
        `INSERT OR IGNORE INTO network_nodes (source, node_id) VALUES ${placeholders}`,
        params
      );
    }
    await dbRun('COMMIT');
  } catch (err) {
    await dbRun('ROLLBACK').catch(() => {});
    throw err;
  }
}

async function removeNetworkFromDatabase(source) {
  try {
    await dbRun('BEGIN TRANSACTION');
    await dbRun('DELETE FROM network_nodes WHERE source = ?', [source]);
    await dbRun('DELETE FROM edges WHERE source = ?', [source]);
    await dbRun('COMMIT');
  } catch (err) {
    await dbRun('ROLLBACK').catch(() => {});
    logger.error(`Failed to remove partially saved edited network ${source}: ${err.message}`);
    throw err;
  }
}

function suppressWatcherIngest(filename) {
  try {
    const fileWatcher = require('./fileWatcher');
    if (fileWatcher && typeof fileWatcher.suppressNetworkIngest === 'function') {
      fileWatcher.suppressNetworkIngest(filename);
      return;
    }
    throw new Error('fileWatcher.suppressNetworkIngest is unavailable');
  } catch (err) {
    throw new NetworkEditError(
      `Could not suppress watcher ingest for ${filename}: ${err.message}`,
      500
    );
  }
}

async function createEditedNetwork({ source, name, hiddenNodeIds, hiddenEdgeIds, hiddenEdgeWeightRanges } = {}) {
  if (typeof source !== 'string' || source.trim() === '') {
    throw new NetworkEditError('source must be a non-empty string');
  }

  const normalizedSource = source.trim();
  const outputFilename = normalizeOutputName(name);
  const hiddenIds = new Set(normalizeHiddenNodeIds(hiddenNodeIds));
  const explicitHiddenEdgeIds = normalizeHiddenEdgeIds(hiddenEdgeIds);
  const hiddenEdgeRanges = normalizeHiddenEdgeWeightRanges(hiddenEdgeWeightRanges);
  const hiddenEdgeIdSet = new Set(explicitHiddenEdgeIds);

  const sourceNodeIds = await getSourceNodeIds(normalizedSource);
  if (sourceNodeIds.length === 0) {
    throw new NetworkEditError(`Network not found: ${normalizedSource}`, 404);
  }

  const sourceNodeSet = new Set(sourceNodeIds);
  const visibleNodeIds = sourceNodeIds.filter(nodeId => !hiddenIds.has(nodeId));
  if (visibleNodeIds.length === 0) {
    throw new NetworkEditError('Edited network must contain at least one visible node', 422);
  }

  const visibleSet = new Set(visibleNodeIds);
  const sourceEdges = await getSourceEdges(normalizedSource);
  const sourceEdgeIds = new Set(sourceEdges.map(edge => String(edge.id || getEdgeId(edge.node1, edge.node2))));

  sourceEdges.forEach(edge => {
    if (isEdgeHiddenByWeightRanges(edge, hiddenEdgeRanges)) {
      hiddenEdgeIdSet.add(String(edge.id || getEdgeId(edge.node1, edge.node2)));
    }
  });

  const visibleEdges = sourceEdges.filter(edge => {
    const edgeId = String(edge.id || getEdgeId(edge.node1, edge.node2));
    return !hiddenEdgeIdSet.has(edgeId)
      && visibleSet.has(String(edge.node1))
      && visibleSet.has(String(edge.node2));
  });
  const hiddenInSourceCount = Array.from(hiddenIds).filter(id => sourceNodeSet.has(id)).length;
  const hiddenEdgesInSourceCount = Array.from(hiddenEdgeIdSet).filter(id => sourceEdgeIds.has(id)).length;

  await fs.promises.mkdir(config.dataPath, { recursive: true });
  await fs.promises.mkdir(config.tempDataPath, { recursive: true });

  let reservation = null;
  let tempDir = null;

  try {
    reservation = await reserveOutputName(outputFilename);
    const { outputFilename: reservedOutputFilename, finalOutputPath } = reservation;
    tempDir = await fs.promises.mkdtemp(path.join(config.tempDataPath, 'edit-network-'));
    const tempOutputPath = path.join(tempDir, reservedOutputFilename);

    const csv = visibleEdges.map(edgeToCsv).join('\n');
    await fs.promises.writeFile(tempOutputPath, csv ? `${csv}\n` : '', 'utf8');
    suppressWatcherIngest(reservedOutputFilename);
    await fs.promises.rename(tempOutputPath, finalOutputPath);

    try {
      await ingestNetworks(reservedOutputFilename);
      await insertNetworkMembership(reservedOutputFilename, visibleNodeIds);
    } catch (err) {
      await fs.promises.rm(finalOutputPath, { force: true }).catch(() => {});
      let cleanupError = null;
      try {
        await removeNetworkFromDatabase(reservedOutputFilename);
      } catch (cleanupErr) {
        cleanupError = cleanupErr;
      }
      throw new NetworkEditError(
        `Edited network could not be ingested: ${err.message}`,
        500,
        cleanupError ? { cleanupError: cleanupError.message } : {}
      );
    }

    logger.info(
      `Saved edited network ${reservedOutputFilename} from ${normalizedSource}: ` +
      `${visibleNodeIds.length} nodes, ${visibleEdges.length} edges`
    );

    return {
      network: reservedOutputFilename,
      viewerUrl: `/viewer.html?network=${encodeURIComponent(reservedOutputFilename)}`,
      source: normalizedSource,
      nodeCount: visibleNodeIds.length,
      edgeCount: visibleEdges.length,
      hiddenInSourceNodeCount: hiddenInSourceCount,
      submittedHiddenNodeCount: hiddenIds.size,
      hiddenInSourceEdgeCount: hiddenEdgesInSourceCount,
      submittedHiddenEdgeCount: explicitHiddenEdgeIds.length,
      submittedHiddenEdgeRangeCount: hiddenEdgeRanges.length,
    };
  } finally {
    if (reservation) {
      await reservation.handle.close().catch(err => {
        logger.warn(`Failed to close edited-network reservation handle: ${err.message}`);
      });
      await fs.promises.rm(reservation.lockPath, { force: true }).catch(() => {});
    }
    if (tempDir) {
      await fs.promises.rm(tempDir, { recursive: true, force: true }).catch(() => {});
    }
  }
}

module.exports = {
  NetworkEditError,
  createEditedNetwork,
  normalizeOutputName,
  normalizeHiddenNodeIds,
  normalizeHiddenEdgeIds,
  normalizeHiddenEdgeWeightRanges,
  reserveOutputName,
};
