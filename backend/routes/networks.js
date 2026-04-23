const express = require('express');
const router = express.Router();
const networkController = require('../controllers/networkController');
const logger = require('../utils/logger');
const {
  validateString,
  validateStringArray,
  sendValidationError,
} = require('../utils/requestValidation');

const MAX_SEARCH_ITEMS = 500;

/**
 * @route   GET /api/networks
 * @desc    List all available networks
 * @access  Public
 */
router.get('/', async (req, res, next) => {
  try {
    logger.info('Fetching list of available networks');
    const networks = await networkController.listNetworks();
    res.json(networks);
  } catch (err) {
    logger.error(`Error listing networks: ${err.message}`);
    next(err);
  }
});

/**
 * @route   POST /api/networks/search
 * @desc    Search for proteins in a network
 * @access  Public
 */
router.post('/search', async (req, res) => {
  try {
    const { network, accessions } = req.body;
    const validationError =
      validateString(network, 'network') ||
      validateStringArray(accessions, 'accessions', MAX_SEARCH_ITEMS);

    if (validationError) return sendValidationError(res, validationError);

    logger.info(`Searching proteins in ${network}: ${accessions.join(', ')}`);
    const result = await networkController.searchProteins(network, accessions);
    res.json(result);
  } catch (err) {
    logger.error(`Error searching proteins: ${err.message}`);
    res.status(500).json({ error: err.message });
  }
});

/**
 * @route   POST /api/networks/search-species
 * @desc    Search for proteins by species in a network
 * @access  Public
 */
router.post('/search-species', async (req, res) => {
  try {
    const { network, speciesIds } = req.body;
    const validationError =
      validateString(network, 'network') ||
      validateStringArray(speciesIds, 'speciesIds', MAX_SEARCH_ITEMS);

    if (validationError) return sendValidationError(res, validationError);

    logger.info(`Searching proteins by species in ${network}: ${speciesIds.join(', ')}`);
    const result = await networkController.searchBySpecies(network, speciesIds);
    res.json(result);
  } catch (err) {
    logger.error(`Error searching proteins by species: ${err.message}`);
    res.status(500).json({ error: err.message });
  }
});

/**
 * @route   GET /api/networks/:filename
 * @desc    Get network data from a specific file
 * @access  Public
 */
router.get('/:filename', async (req, res, next) => {
  try {
    const { filename } = req.params;
    logger.info(`Fetching network data for file: ${filename}`);
    const networkData = await networkController.getNetworkData(filename);
    res.json(networkData);
  } catch (err) {
    logger.error(`Error fetching network data: ${err.message}`);

    if (err.message.includes('not found') || err.message.includes('ENOENT')) {
      return res.status(404).json({ error: 'Network not found' });
    }

    next(err);
  }
});


module.exports = router;
