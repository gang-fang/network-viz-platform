const express = require('express');
const cors = require('cors');
const morgan = require('morgan');
const path = require('path');
const config = require('./config/config');
const logger = require('./utils/logger');

// Import routes
const networksRouter = require('./routes/networks');
const uniprotRouter = require('./routes/uniprot');
const speciesRouter = require('./routes/species');

// Initialize express app
const app = express();

// Middleware
app.use(cors({ origin: config.corsOrigin }));
app.use(express.json({ limit: '1mb' }));
app.use(express.urlencoded({ extended: true, limit: '1mb' }));
app.use(morgan('dev'));

// Health check endpoint (before API routes so it's always reachable)
app.get('/health', (req, res) => {
  const db = require('./config/database');
  db.get('SELECT 1', (err) => {
    if (err) {
      return res.status(503).json({ status: 'error', db: 'disconnected', uptime: Math.round(process.uptime()) });
    }
    res.json({ status: 'ok', db: 'connected', uptime: Math.round(process.uptime()) });
  });
});

// API routes
app.use('/api/networks', networksRouter);
app.use('/api/uniprot', uniprotRouter);
app.use('/api/species-names', speciesRouter);

// Root route for API testing - this needs to be before static file serving
app.get('/', (req, res, next) => {
  // Check if the request is expecting JSON (like in tests)
  const acceptsJson = req.accepts('json');
  if (acceptsJson) {
    return res.status(200).send('Network Visualization Platform API');
  }
  // For browser requests or HTML accepts, pass to next handler (static files)
  next();
});

// Static files - after API routes
app.use(express.static(path.join(__dirname, '../frontend')));

// Error handling middleware
app.use((err, req, res, next) => {
  logger.error(`${err.status || 500} - ${err.message} - ${req.originalUrl} - ${req.method} - ${req.ip}`);
  res.status(err.status || 500).json({
    message: err.message,
    error: process.env.NODE_ENV === 'development' ? err : {}
  });
});

// Start server only if not in test environment or explicitly required
if (process.env.NODE_ENV !== 'test') {
  const startServer = (port) => {
    return new Promise((resolve, reject) => {
      // Create server instance but don't start listening yet
      const server = app.listen(port)
        .on('error', (err) => {
          if (err.code === 'EADDRINUSE') {
            logger.warn(`Port ${port} is already in use, trying next port...`);
            server.close();
            // Try the next port
            resolve(false);
          } else {
            // For other errors, reject with the error
            reject(err);
          }
        })
        .on('listening', () => {
          // Successfully bound to this port
          resolve(true);
        });
    });
  };

  // Try to start server with port from config, or find available port
  const findAvailablePort = async () => {
    let PORT = config.port || 3000;
    const MAX_PORT_ATTEMPTS = 10; // Try up to 10 ports before giving up

    for (let attempt = 0; attempt < MAX_PORT_ATTEMPTS; attempt++) {
      try {
        const success = await startServer(PORT);
        if (success) {
          // Port was available and server started
          const memUsage = process.memoryUsage();
          logger.info(`✅ Server running on port ${PORT}`);
          logger.info(`🔗 Access the app at http://localhost:${PORT}`);
          logger.info(`📂 Current working directory: ${process.cwd()}`);
          logger.info(`📁 Data path: ${config.dataPath}`);
          logger.info(`💾 Memory usage: RSS ${Math.round(memUsage.rss / 1024 / 1024)}MB, Heap ${Math.round(memUsage.heapUsed / 1024 / 1024)}MB`);

          // Display in console with highlighting for better visibility
          console.log('\x1b[36m%s\x1b[0m', `\n╔════════════════════════════════════════════════════════╗`);
          console.log('\x1b[36m%s\x1b[0m', `║                                                        ║`);
          console.log('\x1b[36m%s\x1b[0m', `║  SJI Network Visualization Platform                    ║`);
          console.log('\x1b[36m%s\x1b[0m', `║                                                        ║`);
          console.log('\x1b[36m%s\x1b[0m', `║  Server running at:                                    ║`);
          console.log('\x1b[36m%s\x1b[0m', `║  \x1b[33mhttp://localhost:${PORT}\x1b[36m${' '.repeat(36 - PORT.toString().length)}║`);
          console.log('\x1b[36m%s\x1b[0m', `║                                                        ║`);
          console.log('\x1b[36m%s\x1b[0m', `╚════════════════════════════════════════════════════════╝\n`);

          return true;
        }
        // If we get here, the port was unavailable, try the next one
        PORT++;
      } catch (error) {
        logger.error(`Error starting server: ${error.message}`);
        return false;
      }
    }

    // If we tried MAX_PORT_ATTEMPTS ports and none worked
    logger.error(`Could not find available port after ${MAX_PORT_ATTEMPTS} attempts`);
    return false;
  };

  // Start server on available port
  findAvailablePort().catch(err => {
    logger.error(`Failed to start server: ${err.message}`);
    process.exit(1);
  });
}

module.exports = app; // For testing purposes
