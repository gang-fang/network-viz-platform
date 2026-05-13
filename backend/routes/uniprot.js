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
 * @route   POST /api/uniprot/availability
 * @desc    Check whether UniProt accession numbers currently resolve to entries
 * @access  Public
 */
router.post('/availability', async (req, res, next) => {
  try {
    const { accessions } = req.body;
    const validationError = validateStringArray(accessions, 'accessions', MAX_UNIPROT_BATCH);
    if (validationError) return sendValidationError(res, validationError);

    logger.info(`Checking UniProt availability for ${accessions.length} accessions`);
    const availability = await uniprotController.getBatchProteinAvailability(accessions);
    res.json({ results: availability });
  } catch (err) {
    logger.error(`Error checking UniProt availability: ${err.message}`);
    next(err);
  }
});

module.exports = router;
