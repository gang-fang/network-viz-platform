const logger = require('../utils/logger');
const subnetworkService = require('../services/subnetworkService');

function getSubnetworkLimits(req, res) {
  res.json(subnetworkService.getSubnetworkLimits());
}

async function createSubnetwork(req, res, next) {
  try {
    const result = await subnetworkService.createSubnetwork(req.body);
    res.status(201).json(result);
  } catch (err) {
    if (err && err.status) {
      logger.warn(`Subnetwork extraction rejected: ${err.message}`);
      return res.status(err.status).json({
        error: err.message,
        ...(err.details || {}),
      });
    }

    logger.error(`Unexpected subnetwork extraction failure: ${err.message}`);
    next(err);
  }
}

module.exports = {
  getSubnetworkLimits,
  createSubnetwork,
};
