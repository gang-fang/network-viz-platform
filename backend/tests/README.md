# Backend Testing

## Structure

```
backend/tests/
├── unit/                  # Unit tests
├── integration/
│   ├── routes/            # API endpoint integration tests
│   ├── ingestData.integration.test.js
│   └── networkController.integration.test.js
├── fixtures/              # Test data files
└── setup/                 # Test configuration
```

## Running Tests

```bash
# From project root
npm run test:backend

# Specific backend tests
npx jest "backend/tests/**/*.js"
```

## Current Tests

- ✅ `unit/ingestData.test.js` - Attribute ingestion unit tests (validation, reconcile SQL, argument checks, file-watcher filtering)
- ✅ `unit/server_startup.test.js` - Server startup and route configuration
- ✅ `unit/server.test.js` - Basic server routes (GET /, 404 handling, GET /api/networks)
- ✅ `integration/ingestData.integration.test.js` - Attribute ingestion (`ingestNodeAttributes`) and network ingestion (`ingestNetworks`) against real in-memory SQLite
- ✅ `integration/networkController.integration.test.js` - Network controller (getNetworkData, searchProteins) against real in-memory SQLite
- ✅ `integration/routes/networks.test.js` - Network API endpoints (GET /api/networks, GET /api/networks/:filename)
