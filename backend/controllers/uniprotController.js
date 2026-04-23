const config = require('../config/config');
const logger = require('../utils/logger');
const axios = require('axios');

const uniprotConfig = config.uniprotApi || {};
const CACHE_EXPIRY_MS = uniprotConfig.cacheExpiry || 86400000;
const CACHE_MAX_ENTRIES = uniprotConfig.cacheMaxEntries || 1000;
const BATCH_LIMIT = uniprotConfig.batchLimit || 100;
const CONCURRENCY_LIMIT = uniprotConfig.concurrencyLimit || 5;
const proteinCache = new Map();

function getCachedProtein(accession) {
  const cached = proteinCache.get(accession);
  if (!cached) return null;

  if (Date.now() - cached.cachedAt > CACHE_EXPIRY_MS) {
    proteinCache.delete(accession);
    return null;
  }

  // Refresh recency for LRU eviction.
  proteinCache.delete(accession);
  proteinCache.set(accession, cached);
  return cached.data;
}

function cacheProtein(accession, data) {
  if (proteinCache.has(accession)) {
    proteinCache.delete(accession);
  }

  proteinCache.set(accession, { data, cachedAt: Date.now() });

  while (proteinCache.size > CACHE_MAX_ENTRIES) {
    const oldestKey = proteinCache.keys().next().value;
    proteinCache.delete(oldestKey);
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

function validateAccessions(accessions) {
  if (!Array.isArray(accessions)) {
    throw new Error('accessions must be an array');
  }

  if (accessions.length > BATCH_LIMIT) {
    throw new Error(`accessions is limited to ${BATCH_LIMIT} items`);
  }

  if (!accessions.every(accession => typeof accession === 'string' && accession.trim() !== '')) {
    throw new Error('accessions must contain only non-empty strings');
  }
}

/**
 * Get protein data from UniProt by accession number
 * @param {string} accession - UniProt accession number
 * @returns {Promise<Object>} Protein data
 */
async function getProteinData(accession) {
  try {
    const cached = getCachedProtein(accession);
    if (cached) return cached;

    const url = `${config.uniprotApi.baseUrl}${accession}`;
    logger.info(`Fetching protein data from: ${url}`);
    
    const response = await axios.get(url, {
      params: { format: 'json' },
      timeout: config.uniprotApi.timeout
    });
    
    cacheProtein(accession, response.data);
    return response.data;
  } catch (err) {
    logger.error(`Error fetching protein data for ${accession}: ${err.message}`);
    throw new Error(`Failed to fetch protein data for: ${accession}`);
  }
}

/**
 * Get protein data for multiple accession numbers
 * @param {Array<string>} accessions - Array of UniProt accession numbers
 * @returns {Promise<Array<Object>>} Array of protein data
 */
async function getBatchProteinData(accessions) {
  try {
    validateAccessions(accessions);
    return await mapWithConcurrency(accessions, CONCURRENCY_LIMIT, getProteinData);
  } catch (err) {
    logger.error(`Error fetching batch protein data: ${err.message}`);
    if (err.message.includes('accessions')) throw err;
    throw new Error('Failed to fetch batch protein data');
  }
}

/**
 * Get specific fields for a protein from UniProt
 * @param {string} accession - UniProt accession number
 * @param {string} fields - Comma-separated list of fields to retrieve
 * @returns {Promise<Object>} Protein field data
 */
async function getProteinFields(accession, fields) {
  try {
    const url = `${config.uniprotApi.baseUrl}${accession}`;
    logger.info(`Fetching protein fields from: ${url}`);
    
    const response = await axios.get(url, {
      params: { 
        format: 'json',
        fields: fields
      },
      timeout: config.uniprotApi.timeout
    });
    
    return response.data;
  } catch (err) {
    logger.error(`Error fetching protein fields for ${accession}: ${err.message}`);
    throw new Error(`Failed to fetch protein fields for: ${accession}`);
  }
}

/**
 * Get proteins filtered by species taxonomy ID
 * @param {string} taxId - Taxonomy ID
 * @returns {Promise<Array<Object>>} Array of protein data
 */
async function getProteinsBySpecies(taxId) {
  try {
    const url = `${config.uniprotApi.baseUrl}search`;
    logger.info(`Searching proteins by taxonomy ID: ${taxId}`);
    
    const response = await axios.get(url, {
      params: {
        query: `organism_id:${taxId}`,
        format: 'json'
      },
      timeout: config.uniprotApi.timeout
    });
    
    return response.data;
  } catch (err) {
    logger.error(`Error fetching proteins for taxonomy ID ${taxId}: ${err.message}`);
    throw new Error(`Failed to fetch proteins for taxonomy ID: ${taxId}`);
  }
}

module.exports = {
  getProteinData,
  getBatchProteinData,
  getProteinFields,
  getProteinsBySpecies,
  validateAccessions,
};
