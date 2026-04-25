const express = require('express');
const router = express.Router();
const subnetworkController = require('../controllers/subnetworkController');

router.get('/limits', subnetworkController.getSubnetworkLimits);
router.post('/', subnetworkController.createSubnetwork);

module.exports = router;
