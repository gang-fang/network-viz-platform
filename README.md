# Network Visualization Platform

A powerful and flexible visualization platform for rendering complex network data with physics-based layouts and advanced clustering capabilities.

## Architecture Overview

The platform uses a clean separation between a backend data pipeline and a frontend visualization layer.

### Frontend Core (`frontend/js/core/`)

- **`app.js`** — Application entry point; bootstraps state, modules, and the D3 adapter.
- **`state.js`** (`AppState`) — Centralizes all application state: the core graph, expanded cluster set, node colours, highlight layers, hidden nodes, and the currently loaded network name.
- **`graph.js`** (`Graph`) — Framework-agnostic graph data structure (nodes Map, edges Map, adjacency Map). The canonical in-memory representation of the network topology and attributes.
- **`graph-view.js`** (`GraphView`) — Computes the *visible* graph from the core graph and the set of expanded clusters. Implements drill-down/roll-up: collapsed clusters are represented by a single cluster node; expanded clusters show individual protein nodes.
- **`module-system.js`** (`ModuleSystem`) — Plugin-style module loader. Modules register themselves and receive lifecycle calls (init, network-load, selection-change, etc.).

### Frontend Adapters & Modules

- **`adapters/d3-adapter.js`** — Bridges `AppState`/`GraphView` to D3.js force simulation and SVG rendering.
- **`modules/`** — Self-contained feature modules: `species-selector`, `search-highlight`, `clear-highlights`, `uniprot-tooltip`, `export-panel`.
- **`config/modules.js`** — Declares which modules are active for a given deployment.

### Backend

- **`scripts/ingestData.js`** — Data ingestion pipeline. Reads `.nodes.attr` files and network CSVs into SQLite. Node attribute ingestion is transactional: reconcile (clear all non-empty protein rows) then rewrite from the current file set.
- **`services/fileWatcher.js`** — Chokidar-based hot-reload watcher. Re-triggers attribute ingestion when a configured `.nodes.attr` file changes.
- **`controllers/networkController.js`** — Query layer over the SQLite DB; exposes network lists, node/edge data with attributes, protein search by accession or species.
- **`config/database.js`** — Opens the SQLite connection and initialises the schema (`nodes`, `edges` tables; `attribute_source` migration).

## Key Features

- **Attribute-based clustering**: Nodes are grouped by `NH_ID`. Clusters can be expanded/collapsed individually or in bulk.
- **Module system**: Feature modules (species selector, UniProt tooltip, search highlight, export) are loaded declaratively via `config/modules.js` and can be added or removed without touching core code.
- **Transactional ingestion**: Node attributes are ingested atomically — reconcile clears all existing protein attribute rows, then rewrites from the current file set. A mid-write failure rolls back to the previous state.
- **Hot-reload**: `fileWatcher.js` detects changes to configured `.nodes.attr` files and re-triggers ingestion automatically.

## Development

### Project Structure

```
frontend/
├── js/
│   ├── app.js                    # Application bootstrap
│   ├── config/
│   │   └── modules.js            # Active module declarations
│   ├── core/
│   │   ├── graph.js              # Graph data structure (nodes, edges, adjacency)
│   │   ├── graph-view.js         # Visible-graph computation (drill-down/roll-up)
│   │   ├── module-system.js      # Module loader and lifecycle
│   │   └── state.js              # Centralised application state
│   ├── adapters/
│   │   └── d3-adapter.js         # D3.js rendering bridge
│   └── modules/                  # Self-contained feature modules
│       ├── clear-highlights.js
│       ├── export-panel.js
│       ├── search-highlight.js
│       ├── species-selector.js
│       └── uniprot-tooltip.js
backend/
├── controllers/
│   ├── networkController.js     # Network and attribute data handling
│   └── uniprotController.js     # UniProt API integration
├── routes/
│   ├── networks.js              # Network data API endpoints
│   ├── species.js               # NCBI taxonomy name mappings
│   └── uniprot.js               # UniProt API endpoints
├── scripts/
│   ├── ingestData.js            # Data ingestion pipeline
│   └── configureAttrs.js        # Interactive NODE_ATTRIBUTE_FILES helper
├── services/
│   └── fileWatcher.js           # Hot-reload watcher for data files
└── entrypoint.js                # Startup mode router
```

### Running Locally

```bash
# 1. Install dependencies
npm install

# 2. Place your data files (see Data Files section below)

# 3. Configure environment
cp .env.example .env
# Edit .env as needed (PORT, DB_PATH, DATA_PATH, etc.)

# 4. Select which .nodes.attr files to load
npm run configure:attrs         # interactive prompt — updates NODE_ATTRIBUTE_FILES in .env

# 5. Ingest data files into the database
npm run ingest

# 6. Start the server
npm start

# Or combine steps 5 and 6:
npm run start:ingest-and-serve
```

> **Upgrading D3**: `frontend/vendor/d3.min.js` is a committed vendor file. If you bump `d3` in `package.json`, refresh it manually:
> ```bash
> cp node_modules/d3/dist/d3.min.js frontend/vendor/d3.min.js
> ```

### Data Files

Place your data files in the following directories before running ingestion.
All paths are configurable via environment variables (see Configuration below).

| Directory | Env var | File format |
|---|---|---|
| `data/networks/` | `DATA_PATH` | `*.csv` — one edge per line: `node1,node2,weight` |
| `data/indexes/` | `INDEXES_PATH` | Preprocessed graph index triplets such as `eu.adj.bin`, `eu.adj.index.bin`, `eu.node_ids.tsv` |
| `data/nodes_attr/` | `NODE_ATTRIBUTES_PATH` | `*.nodes.attr` — comma-separated, with a header row: `node_id`, `NCBI_txID`, `NH_ID`, `NH_Size`, … |
| `data/NCBI_txID/NCBI_txID.csv` | `SPECIES_PATH` | Two columns: `ncbi_txid,species_name` |

### Configuration

Copy `.env.example` to `.env` and override as needed. Key variables:

| Variable | Default | Description |
|---|---|---|
| `START_MODE` | `serve` | `serve` · `ingest` · `ingest-and-serve` |
| `DB_PATH` | `./data/network_viz.db` | SQLite file path — point to a volume mount in Docker |
| `DATA_PATH` | `./data/networks` | Directory containing network CSV files |
| `INDEXES_PATH` | `./data/indexes` | Directory containing preprocessed extraction indexes |
| `NODE_ATTRIBUTES_PATH` | `./data/nodes_attr` | Directory containing `.nodes.attr` files |
| `NODE_ATTRIBUTE_FILES` | *(required)* | Comma-separated list of `.nodes.attr` filenames to ingest, e.g. `e.nodes.attr,p.nodes.attr` |
| `SPECIES_PATH` | `./data/NCBI_txID/NCBI_txID.csv` | NCBI taxonomy mapping CSV |
| `PYTHON_COMMAND` | `python3` | Python executable used for subnetwork extraction |
| `SUBNETWORK_SCRIPT_PATH` | `./tools/extract_subnetwork.py` | Extraction CLI path |
| `SUBNETWORK_JOB_TEMP_PATH` | `./data/tmp/subnetwork-jobs` | Controlled temp directory for extraction jobs |
| `SUBNETWORK_TIMEOUT_MS` | `120000` | Extraction timeout in milliseconds |
| `FILE_WATCH_ENABLED` | `true` | Set to `false` to disable hot-reload of data files |
| `PORT` | `3000` | HTTP port |
| `CORS_ORIGIN` | `*` | Allowed origin — restrict in production |

### API Endpoints

| Endpoint | Description |
|---|---|
| `GET /health` | Liveness check — returns `{status, db, uptime}` |
| `GET /api/networks` | List ingested network sources |
| `GET /api/networks/:filename` | Fetch nodes and edges for a network |
| `POST /api/networks/search` | Find nodes by UniProt accession |
| `POST /api/networks/search-species` | Find nodes by NCBI taxonomy ID |
| `GET /api/subnetworks/limits` | Return client-facing extraction limits and available discovered indexes |
| `POST /api/subnetworks` | Run `tools/extract_subnetwork.py`, write a generated CSV to `data/networks`, and return a `/viewer.html?network=...` link |
| `GET /api/species-names` | NCBI taxonomy ID → species name mappings |
| `GET /api/uniprot/:accession` | UniProt protein data |

### Key Design Principles

1. **Single Source of Truth**: Each data type has one authoritative storage location
2. **Exact Matching**: All node operations use exact string matching only
3. **Event-Driven Architecture**: Components communicate through a centralized event bus
4. **Performance First**: Optimized for large networks (1000+ nodes)
5. **Zero Redundancy**: No duplicate code, data structures, or processing paths

## Testing

Backend data pipeline tests (unit + integration against a real in-memory SQLite DB):

```bash
npm test
```

Coverage includes: attribute file validation, reconcile SQL shape, argument validation, file-watcher filtering, legacy-row migration, NH-row cleanup, removed/moved protein scenarios, full-selection atomicity (rollback on mid-write failure); network edge ingestion (stale-edge removal, FK-safe node insertion, mid-write rollback, malformed-row skipping).

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

When contributing:
1. Follow the zero-redundancy principle
2. Use exact string matching for node operations
3. Maintain the three-tier data architecture
4. Add appropriate tests for new features
5. Update documentation for architectural changes

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
 
