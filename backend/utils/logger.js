const winston = require('winston');
const path = require('path');
const config = require('../config/config');

// Define log format
const logFormat = winston.format.combine(
  winston.format.timestamp(),
  winston.format.errors({ stack: true }),
  winston.format.json()
);

// Create logger instance
const logger = winston.createLogger({
  level: config.logging.level,
  format: logFormat,
  transports: [
    // Write logs to console
    new winston.transports.Console({
      format: winston.format.combine(
        winston.format.colorize(),
        winston.format.simple()
      )
    }),
    // Write logs to file
    new winston.transports.File({
      filename: path.join(__dirname, '..', config.logging.file),
      maxsize: 5242880, // 5MB
      maxFiles: 5,
      tailable: true
    })
  ],
  // Handle uncaught exceptions and unhandled rejections
  exceptionHandlers: [
    new winston.transports.File({
      filename: path.join(__dirname, '..', 'logs/exceptions.log')
    })
  ],
  rejectionHandlers: [
    new winston.transports.File({
      filename: path.join(__dirname, '..', 'logs/rejections.log')
    })
  ]
});

// Add request context middleware
logger.requestContext = (req, res, next) => {
  logger.defaultMeta = {
    requestId: req.id,
    path: req.path,
    method: req.method
  };
  next();
};

module.exports = logger;