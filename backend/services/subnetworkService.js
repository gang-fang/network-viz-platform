const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');
const config = require('../config/config');
const logger = require('../utils/logger');
const { ingestNetworks } = require('../scripts/ingestData');

// This is an in-process concurrency cap. It does not coordinate across
// multiple Node server instances.
let activeJobCount = 0;
const LOCK_STALE_MS = 10 * 60 * 1000;
const JSON_SENTINEL = '__SUBNET_JSON__ ';

class SubnetworkError extends Error {
  constructor(message, status = 400, details = {}) {
    super(message);
    this.name = 'SubnetworkError';
    this.status = status;
    this.details = details;
  }
}

function splitSeedInput(seedInput) {
  if (Array.isArray(seedInput)) {
    if (!seedInput.every(seed => typeof seed === 'string')) {
      throw new SubnetworkError('seeds must contain only string identifiers');
    }
    return seedInput;
  }

  if (typeof seedInput === 'string') {
    return seedInput.split(/[\s,;]+/);
  }

  throw new SubnetworkError('seeds must be an array of identifiers');
}

function normalizeSeeds(seedInput) {
  const seedTokens = splitSeedInput(seedInput)
    .map(seed => String(seed).trim())
    .filter(Boolean);

  const combinedLength = seedTokens.join('\n').length;
  if (combinedLength === 0) {
    throw new SubnetworkError('At least one seed protein is required');
  }

  if (combinedLength > config.subnetwork.maxInputLength) {
    throw new SubnetworkError(
      `Seed input is limited to ${config.subnetwork.maxInputLength} characters`
    );
  }

  if (seedTokens.length > config.subnetwork.maxSeedCount) {
    throw new SubnetworkError(
      `Seed input is limited to ${config.subnetwork.maxSeedCount} identifiers`
    );
  }

  const invalidSeeds = seedTokens.filter(seed => !/^[A-Za-z0-9][A-Za-z0-9._:-]*$/.test(seed));
  if (invalidSeeds.length > 0) {
    throw new SubnetworkError(
      'Seed identifiers must start with a letter or digit and may contain only letters, digits, ".", "_", ":" and "-"'
    );
  }

  return seedTokens;
}

function normalizeOutputName(nameInput) {
  if (typeof nameInput !== 'string' || nameInput.trim() === '') {
    throw new SubnetworkError('name must be a non-empty string');
  }

  const trimmed = nameInput.trim();
  if (trimmed.length > config.subnetwork.maxNameLength) {
    throw new SubnetworkError(
      `name is limited to ${config.subnetwork.maxNameLength} characters`
    );
  }

  const normalized = trimmed.toLowerCase().endsWith('.csv')
    ? `${trimmed.slice(0, -4)}.csv`
    : `${trimmed}.csv`;
  if (!/^[A-Za-z0-9_-][A-Za-z0-9_.-]*\.(?:csv)$/i.test(normalized)) {
    throw new SubnetworkError(
      'name may contain only letters, numbers, ".", "_" and "-", and must resolve to a .csv filename'
    );
  }

  if (normalized === '.' || normalized === '..' || normalized.includes('/') || normalized.includes('\\')) {
    throw new SubnetworkError('name must be a plain filename, not a path');
  }

  return normalized;
}

function normalizeIndexName(indexName) {
  const requestedIndex = typeof indexName === 'string' && indexName.trim() ? indexName.trim() : 'eu';
  const allowedIndexes = getAllowedIndexPrefixes();
  if (!allowedIndexes.includes(requestedIndex)) {
    throw new SubnetworkError(
      `index must be one of: ${allowedIndexes.join(', ')}`
    );
  }

  return requestedIndex;
}

function normalizeMaxNodes(value) {
  if (value === undefined || value === null || value === '') {
    return config.subnetwork.defaultMaxNodes;
  }

  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 1) {
    throw new SubnetworkError('maxNodes must be a positive integer');
  }

  if (parsed > config.subnetwork.maxNodesLimit) {
    throw new SubnetworkError(
      `maxNodes is limited to ${config.subnetwork.maxNodesLimit}`
    );
  }

  return parsed;
}

function parseMessages(stdout, stderr) {
  const rawStdoutLines = stdout.split(/\r?\n/).map(line => line.trim()).filter(Boolean);
  const stderrLines = stderr.split(/\r?\n/).map(line => line.trim()).filter(Boolean);
  const stdoutLines = [...rawStdoutLines];
  const warnings = [];
  const missingSeeds = [];
  let emptySubnetwork = false;
  let summary = null;

  for (let i = stdoutLines.length - 1; i >= 0; i -= 1) {
    const candidate = stdoutLines[i];
    if (!candidate.startsWith(JSON_SENTINEL)) {
      continue;
    }
    try {
      summary = JSON.parse(candidate.slice(JSON_SENTINEL.length));
      stdoutLines.splice(i, 1);
      break;
    } catch (_err) {
      continue;
    }
  }

  for (const line of stderrLines) {
    warnings.push(line);

    const missingMatch = line.match(/WARNING:\s+\d+\s+input seed\(s\) were not found in the network:\s+(.+)$/);
    if (missingMatch) {
      for (const token of missingMatch[1].split(',')) {
        const seed = token.trim();
        if (seed) missingSeeds.push(seed);
      }
    }

    if (line === 'WARNING: Generated subnetwork is empty.') {
      emptySubnetwork = true;
    }
  }

  return {
    stdoutLines,
    stderrLines,
    warnings,
    missingSeeds: summary?.missingSeeds || missingSeeds,
    emptySubnetwork: summary?.emptySubnetwork ?? emptySubnetwork,
    summary,
  };
}

function listAvailableIndexPrefixes() {
  if (!fs.existsSync(config.indexesPath)) {
    return [];
  }

  const files = fs.readdirSync(config.indexesPath);
  const prefixes = new Map();

  for (const filename of files) {
    if (filename.endsWith('.adj.bin')) {
      const prefix = filename.slice(0, -'.adj.bin'.length);
      prefixes.set(prefix, {
        ...(prefixes.get(prefix) || {}),
        adj: true,
      });
    } else if (filename.endsWith('.adj.index.bin')) {
      const prefix = filename.slice(0, -'.adj.index.bin'.length);
      prefixes.set(prefix, {
        ...(prefixes.get(prefix) || {}),
        idx: true,
      });
    } else if (filename.endsWith('.node_ids.tsv')) {
      const prefix = filename.slice(0, -'.node_ids.tsv'.length);
      prefixes.set(prefix, {
        ...(prefixes.get(prefix) || {}),
        nodeIds: true,
      });
    }
  }

  return Array.from(prefixes.entries())
    .filter(([, parts]) => parts.adj && parts.idx && parts.nodeIds)
    .map(([prefix]) => prefix)
    .sort();
}

function getAllowedIndexPrefixes() {
  return listAvailableIndexPrefixes();
}

function getSubnetworkLimits() {
  return {
    allowedIndexes: getAllowedIndexPrefixes(),
    maxSeedCount: config.subnetwork.maxSeedCount,
    maxInputLength: config.subnetwork.maxInputLength,
    maxNameLength: config.subnetwork.maxNameLength,
    defaultMaxNodes: config.subnetwork.defaultMaxNodes,
    maxNodesLimit: config.subnetwork.maxNodesLimit,
  };
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
    if (err.code === 'ENOENT') {
      return false;
    }
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
    if (err.code !== 'ENOENT') {
      throw err;
    }
  }
}

async function tryReserveOutputName(finalDir, finalBasename) {
  const canonicalBasename = finalBasename.toLowerCase();
  const lockPath = path.join(finalDir, `.${canonicalBasename}.lock`);

  await clearStaleReservationLock(lockPath, finalDir, canonicalBasename);

  let handle;
  try {
    handle = await fs.promises.open(lockPath, 'wx');
    try {
      const existingFiles = await fs.promises.readdir(finalDir);
      const conflictingFile = existingFiles.find(name =>
        !name.endsWith('.lock') && name.toLowerCase() === canonicalBasename
      );

      if (conflictingFile) {
        throw new SubnetworkError(
          `A network named "${conflictingFile}" already exists`,
          409
        );
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
    if (err.code === 'EEXIST') {
      return null;
    }
    if (err instanceof SubnetworkError && err.status === 409) {
      return null;
    }
    throw err;
  }
}

async function reserveOutputName(requestedOutputName) {
  const finalDir = config.dataPath;
  const { basename, ext } = splitOutputFilename(requestedOutputName);

  for (let suffix = 0; ; suffix += 1) {
    const candidateName = suffix === 0 ? requestedOutputName : `${basename}_${suffix}${ext}`;
    const reservation = await tryReserveOutputName(finalDir, candidateName);
    if (reservation) {
      return reservation;
    }
  }
}

function ensureExtractionPrerequisites(indexName) {
  const indexPrefix = path.join(config.indexesPath, indexName);
  const requiredPaths = [
    config.subnetwork.scriptPath,
    `${indexPrefix}.adj.bin`,
    `${indexPrefix}.adj.index.bin`,
    `${indexPrefix}.node_ids.tsv`,
  ];

  for (const requiredPath of requiredPaths) {
    if (!fs.existsSync(requiredPath)) {
      throw new SubnetworkError(
        `Extraction prerequisite not found: ${requiredPath}`,
        500
      );
    }
  }

  return indexPrefix;
}

function runExtractionProcess({ indexPrefix, seeds, outputPath, maxNodes }) {
  return new Promise((resolve, reject) => {
    const args = [
      config.subnetwork.scriptPath,
      '--json',
      '--max_nodes',
      String(maxNodes),
      '-o',
      outputPath,
      indexPrefix,
      '--',
      ...seeds,
    ];

    const child = spawn(config.subnetwork.pythonCommand, args, {
      stdio: ['ignore', 'pipe', 'pipe'],
    });

    let stdout = '';
    let stderr = '';
    let timedOut = false;
    let killTimer = null;

    const timeout = setTimeout(() => {
      timedOut = true;
      child.kill('SIGTERM');
      killTimer = setTimeout(() => child.kill('SIGKILL'), 5000);
    }, config.subnetwork.timeoutMs);

    child.stdout.on('data', chunk => {
      stdout += chunk.toString();
    });

    child.stderr.on('data', chunk => {
      stderr += chunk.toString();
    });

    child.on('error', err => {
      clearTimeout(timeout);
      if (killTimer) clearTimeout(killTimer);
      reject(new SubnetworkError(
        `Failed to start extraction process: ${err.message}`,
        500,
        parseMessages(stdout, stderr)
      ));
    });

    child.on('close', (code, signal) => {
      clearTimeout(timeout);
      if (killTimer) clearTimeout(killTimer);

      const parsedMessages = parseMessages(stdout, stderr);

      if (timedOut) {
        return reject(new SubnetworkError(
          `Subnetwork extraction exceeded ${config.subnetwork.timeoutMs}ms`,
          504,
          parsedMessages
        ));
      }

      if (code !== 0) {
        const errorMessage = parsedMessages.summary?.error
          || parsedMessages.stderrLines[parsedMessages.stderrLines.length - 1]
          || `Subnetwork extraction failed with exit code ${code}${signal ? ` (${signal})` : ''}`;

        return reject(new SubnetworkError(errorMessage, 400, parsedMessages));
      }

      resolve({
        stdout,
        stderr,
        parsedMessages,
      });
    });
  });
}

async function readEdgeCount(filePath) {
  const content = await fs.promises.readFile(filePath, 'utf8');
  return content.split(/\r?\n/).filter(Boolean).length;
}

async function createSubnetwork({ seeds, name, index = 'eu', maxNodes } = {}) {
  if (activeJobCount >= config.subnetwork.maxConcurrentJobs) {
    throw new SubnetworkError('Too many subnetwork extraction jobs are already running', 429);
  }

  const normalizedSeeds = normalizeSeeds(seeds);
  const outputFilename = normalizeOutputName(name);
  const normalizedIndex = normalizeIndexName(index);
  const normalizedMaxNodes = normalizeMaxNodes(maxNodes);
  const indexPrefix = ensureExtractionPrerequisites(normalizedIndex);

  activeJobCount += 1;
  let tempDir = null;
  let outputReservation = null;

  try {
    await fs.promises.mkdir(config.dataPath, { recursive: true });
    await fs.promises.mkdir(config.subnetwork.jobTempPath, { recursive: true });
    outputReservation = await reserveOutputName(outputFilename);
    const { outputFilename: reservedOutputFilename, finalOutputPath } = outputReservation;

    tempDir = await fs.promises.mkdtemp(path.join(config.subnetwork.jobTempPath, 'job-'));
    const tempOutputPath = path.join(tempDir, reservedOutputFilename);

    logger.info(
      `Starting subnetwork extraction for ${reservedOutputFilename} ` +
      `(${normalizedSeeds.length} seeds, index=${normalizedIndex}, maxNodes=${normalizedMaxNodes})`
    );

    const { parsedMessages } = await runExtractionProcess({
      indexPrefix,
      seeds: normalizedSeeds,
      outputPath: tempOutputPath,
      maxNodes: normalizedMaxNodes,
    });

    if (!fs.existsSync(tempOutputPath)) {
      throw new SubnetworkError('Extraction finished without producing an output file', 500, parsedMessages);
    }

    const edgeCount = await readEdgeCount(tempOutputPath);
    if (edgeCount === 0) {
      throw new SubnetworkError(
        'Generated subnetwork is empty',
        422,
        {
          ...parsedMessages,
          emptySubnetwork: true,
          edgeCount,
        }
      );
    }

    await fs.promises.rename(tempOutputPath, finalOutputPath);

    try {
      await ingestNetworks(reservedOutputFilename);
    } catch (err) {
      logger.debug(
        `Subnetwork ingest failed for ${reservedOutputFilename}. ` +
        `stdout=${JSON.stringify(parsedMessages.stdoutLines)} ` +
        `stderr=${JSON.stringify(parsedMessages.stderrLines)} ` +
        `summary=${JSON.stringify(parsedMessages.summary)}`
      );
      await fs.promises.rm(finalOutputPath, { force: true });
      throw new SubnetworkError(`Generated CSV could not be ingested: ${err.message}`, 500, parsedMessages);
    }

    return {
      network: reservedOutputFilename,
      viewerUrl: `/viewer.html?network=${encodeURIComponent(reservedOutputFilename)}`,
      index: normalizedIndex,
      maxNodes: normalizedMaxNodes,
      resolvedSeedCount: parsedMessages.summary?.resolvedSeedCount ?? normalizedSeeds.length,
      inputSeedCount: parsedMessages.summary?.inputSeedCount ?? normalizedSeeds.length,
      edgeCount,
      elapsedMs: parsedMessages.summary?.elapsedMs,
      ...parsedMessages,
    };
  } finally {
    activeJobCount = Math.max(0, activeJobCount - 1);
    if (outputReservation) {
      await outputReservation.handle.close().catch(err => {
        logger.warn(`Failed to close subnetwork reservation handle: ${err.message}`);
      });
      await fs.promises.rm(outputReservation.lockPath, { force: true });
    }
    if (tempDir) {
      await fs.promises.rm(tempDir, { recursive: true, force: true });
    }
  }
}

module.exports = {
  SubnetworkError,
  createSubnetwork,
  getSubnetworkLimits,
  normalizeSeeds,
  normalizeOutputName,
  normalizeIndexName,
  normalizeMaxNodes,
  parseMessages,
  getAllowedIndexPrefixes,
};
