const config = require('../config/config');
const logger = require('../utils/logger');
const axios = require('axios');

const uniprotConfig = config.uniprotApi;
const CACHE_EXPIRY_MS = uniprotConfig.cacheExpiry;
const CACHE_MAX_ENTRIES = uniprotConfig.cacheMaxEntries;
const CONCURRENCY_LIMIT = uniprotConfig.concurrencyLimit;
const availabilityCache = new Map();

function getCachedAvailability(accession) {
  const cached = availabilityCache.get(accession);
  if (!cached) return null;

  if (Date.now() - cached.cachedAt > CACHE_EXPIRY_MS) {
    availabilityCache.delete(accession);
    return null;
  }

  availabilityCache.delete(accession);
  availabilityCache.set(accession, cached);
  return cached.data;
}

function cacheAvailability(accession, data) {
  if (availabilityCache.has(accession)) {
    availabilityCache.delete(accession);
  }

  availabilityCache.set(accession, { data, cachedAt: Date.now() });

  while (availabilityCache.size > CACHE_MAX_ENTRIES) {
    const oldestKey = availabilityCache.keys().next().value;
    availabilityCache.delete(oldestKey);
  }
}

async function mapWithConcurrency(items, limit, mapper) {
  const results = new Array(items.length);
  let nextIndex = 0;

  async function worker() {
    while (nextIndex < items.length) {
      const currentIndex = nextIndex++;
      results[currentIndex] = await mapper(items[currentIndex], currentIndex);
    }
  }

  const workerCount = Math.min(limit, items.length);
  await Promise.all(Array.from({ length: workerCount }, worker));
  return results;
}

async function getProteinAvailability(accession) {
  try {
    const cached = getCachedAvailability(accession);
    if (cached) return cached;

    const url = `${config.uniprotApi.baseUrl}${accession}.json`;
    logger.info(`Checking UniProt availability from: ${url}`);

    const response = await axios.get(url, {
      timeout: config.uniprotApi.timeout,
      validateStatus: () => true,
    });

    const result = {
      accession,
      available: response.status === 200,
    };

    if (response.status === 200 || response.status === 400 || response.status === 404) {
      cacheAvailability(accession, result);
    } else {
      logger.warn(`Skipping UniProt availability cache for ${accession}: upstream status ${response.status}`);
    }
    return result;
  } catch (err) {
    logger.error(`Error checking UniProt availability for ${accession}: ${err.message}`);
    return {
      accession,
      available: false,
    };
  }
}

async function getBatchProteinAvailability(accessions) {
  return mapWithConcurrency(accessions, CONCURRENCY_LIMIT, getProteinAvailability);
}

module.exports = {
  getProteinAvailability,
  getBatchProteinAvailability,
};
