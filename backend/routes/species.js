const express = require('express');
const router = express.Router();
const fs = require('fs').promises;
const { parse } = require('csv-parse/sync');
const logger = require('../utils/logger');
const config = require('../config/config');

/**
 * @route   GET /api/species-names
 * @desc    Get NCBI taxonomy ID to species name mappings
 * @access  Public
 */
router.get('/', async (req, res) => {
  try {
    const filePath = config.speciesPath;
    logger.info(`Reading species names from: ${filePath}`);
    
    const fileData = await fs.readFile(filePath, 'utf8');
    
    // Parse CSV data
    const records = parse(fileData, {
      columns: ['ncbi_txid', 'species_name'],
      skip_empty_lines: true,
      trim: true,
      skip_records_with_error: true
    });
    
    logger.info(`Loaded ${records.length} species name mappings`);
    res.json(records);
  } catch (error) {
    logger.error(`Error reading species names: ${error.message}`);
    res.status(404).json({ error: 'Species names file not found' });
  }
});

module.exports = router; 