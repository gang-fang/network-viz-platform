const express = require('express');
const router = express.Router();
const uniprotController = require('../controllers/uniprotController');
const logger = require('../utils/logger');
const config = require('../config/config');
const {
  validateStringArray,
  sendValidationError,
} = require('../utils/requestValidation');

const MAX_UNIPROT_BATCH = (config.uniprotApi && config.uniprotApi.batchLimit) || 100;

/**
 * @route   GET /api/uniprot/:accession
 * @desc    Get protein data from UniProt by accession number
 * @access  Public
 */
router.get('/:accession', async (req, res, next) => {
  try {
    const { accession } = req.params;
    logger.info(`Fetching UniProt data for accession: ${accession}`);
    const proteinData = await uniprotController.getProteinData(accession);
    res.json(proteinData);
  } catch (err) {
    logger.error(`Error fetching UniProt data: ${err.message}`);
    next(err);
  }
});

/**
 * @route   POST /api/uniprot/batch
 * @desc    Get protein data for multiple accession numbers
 * @access  Public
 */
router.post('/batch', async (req, res, next) => {
  try {
    const { accessions } = req.body;
    const validationError = validateStringArray(accessions, 'accessions', MAX_UNIPROT_BATCH);
    if (validationError) return sendValidationError(res, validationError);

    logger.info(`Fetching batch UniProt data for ${accessions.length} accessions`);
    const proteinsData = await uniprotController.getBatchProteinData(accessions);
    res.json(proteinsData);
  } catch (err) {
    logger.error(`Error fetching batch UniProt data: ${err.message}`);
    next(err);
  }
});

/**
 * @route   GET /api/uniprot/:accession/fields
 * @desc    Get specific fields for a protein from UniProt
 * @access  Public
 */
router.get('/:accession/fields', async (req, res, next) => {
  try {
    const { accession } = req.params;
    const { fields } = req.query;
    logger.info(`Fetching specific UniProt fields for accession: ${accession}`);
    const fieldData = await uniprotController.getProteinFields(accession, fields);
    res.json(fieldData);
  } catch (err) {
    logger.error(`Error fetching UniProt fields: ${err.message}`);
    next(err);
  }
});

/**
 * @route   GET /api/uniprot/species/:taxId
 * @desc    Get proteins filtered by species taxonomy ID
 * @access  Public
 */
router.get('/species/:taxId', async (req, res, next) => {
  try {
    const { taxId } = req.params;
    logger.info(`Fetching UniProt data for species with taxonomy ID: ${taxId}`);
    const speciesData = await uniprotController.getProteinsBySpecies(taxId);
    res.json(speciesData);
  } catch (err) {
    logger.error(`Error fetching species-specific UniProt data: ${err.message}`);
    next(err);
  }
});

module.exports = router;
