# SJI Network Explorer

SJI Network Explorer is a local web application for extracting, visualizing, editing, and exporting protein similarity subnetworks from pre-built Signal Jaccard Index (SJI) networks.

The package includes a Node.js/Express server, a D3-based interactive network viewer, a fast Python subnetwork extractor, and preprocessing scripts for users who want to attach their own proteins or build custom SJI networks.

## What Is the Signal Jaccard Index?

The **Signal Jaccard Index (SJI)** is a protein-similarity metric based on shared homolog neighborhoods. Instead of comparing two proteins only by direct pairwise sequence similarity, SJI compares the sets of **signal proteins** associated with each protein.

For a given seed protein, top similar proteins are retrieved across many proteomes and placed on a two-dimensional plot. The x-axis represents length ratio relative to the seed protein, and the y-axis represents full-length sequence similarity. In most cases, homologs separate into two groups: a closer **signal** group and a more distant **noise** group. The boundary is not a fixed similarity cutoff. It is inferred from the local density structure of each plot, so each protein has its own protein-specific signal/noise separation.

Given two proteins A and B, SJI is the Jaccard index of their signal sets:

```text
SJI(A, B) = |signal(A) ∩ signal(B)| / |signal(A) ∪ signal(B)|
```

A high SJI means two proteins share much of the same signal homolog neighborhood across proteomes. This makes SJI sensitive to broader evolutionary and genomic context, including cases where one-to-one orthology or a single pairwise alignment score is not enough.

SJI was developed to address inconsistencies among ortholog databases. When different ortholog databases annotate the same sequences differently, downstream functional interpretation can diverge. The SJI framework treats those inconsistencies as informative signals about protein evolutionary space. In this view, stable orthology predictions tend to form a network core, while proteins with more inconsistent database assignments often appear toward the network periphery.

## What This Package Provides

- **Pre-built SJI exploration**: extract focused subnetworks from a large pre-built SJI network by entering UniProt accessions.
- **Interactive network viewer**: load saved networks, expand or collapse neighborhood clusters, zoom, pan, and inspect local topology.
- **Species-aware filtering**: use NCBI taxonomy mappings and the common tree to highlight or filter proteins by species or clade.
- **Protein highlighting**: locate seed proteins or proteins of interest by UniProt accession.
- **Network editing**: remove selected proteins, remove edges by SJI threshold, and save edited networks as new CSV files.
- **Export and downstream analysis**: export views and protein lists for additional structural, functional, or evolutionary analysis.
- **Advanced workflows**: use `topN` and Python preprocessing scripts to attach user-supplied proteins/proteomes or build custom SJI networks.

## Currently Available Data

The current pre-built release covers **406 UniProt reference proteomes**:

- **51 eukaryotic reference proteomes**
- **355 bacterial reference proteomes**

The installed data bundle is organized into network CSV files, binary extraction indexes, node attributes, and taxonomy files. The current taxonomy files are:

- `data/NCBI_txID/NCBI_txID.csv` — two-column mapping: `ncbi_txid,species_name`
- `data/NCBI_txID/commontree.txt` — NCBI common tree used by the species selector

The pre-built network represents proteins by UniProt accession. Queries against the pre-built network and subnetwork extraction are therefore accession-based.

Data are distributed separately from the Docker image. Download and organize the release assets from:

```text
https://github.com/gang-fang/network-viz-platform/releases/tag/qfo-reference-proteomes-data-2026
```

The tutorial gives the step-by-step data setup workflow, including `install_data.sh`:

```text
frontend/docs/Tutorial.html
```

## Basic Workflow

1. Install the application with Docker or from a cloned repository.
2. Download the pre-built data release and run `install_data.sh` to create the required `data/` directory structure.
3. Start the server.
4. Open `http://localhost:3000` in Chrome or Firefox.
5. Extract a subnetwork by entering up to 10 UniProt accessions, selecting `Bacteria` or `Eukaryotes`, and choosing a maximum node count.
6. Explore the resulting network in the viewer.
7. Expand neighborhood clusters to inspect individual proteins.
8. Highlight proteins by accession or by species.
9. Remove less relevant proteins or weak SJI edges, then save edited networks under new names.
10. Export protein lists or network views for downstream analysis.

## Installation Options

### Docker

Install Docker Desktop:

```text
https://www.docker.com/products/docker-desktop/
```

Pull the Docker image:

```bash
docker pull ghcr.io/gang-fang/sji-network-explorer:latest
```

After downloading and organizing the data files, run the server from the folder that contains the `data/` directory:

```bash
docker run --rm \
  --name sji-network-explorer \
  -p 3000:3000 \
  -v "$PWD/data:/app/data" \
  ghcr.io/gang-fang/sji-network-explorer:latest
```

Then open:

```text
http://localhost:3000
```

Stop the server with:

```bash
docker stop sji-network-explorer
```

### Git Clone

Clone the repository if you want the source code, advanced preprocessing scripts, or a local development setup:

```bash
git clone https://github.com/gang-fang/network-viz-platform.git
cd network-viz-platform
```

The advanced workflows described in `frontend/docs/Tutorial.html` run outside the Docker container and use scripts under `tools/preprocessing` and `tools/bin`.

## Running Locally

```bash
# 1. Install dependencies
npm install

# 2. Install Python dependencies
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 3. Build topN for preprocessing workflows
cd tools/preprocessing/topN_cpp
make
cd ../../..

# 4. Download and organize the release data
# Follow frontend/docs/Tutorial.html#first-run:
# download all .gz release assets plus install_data.sh, then run:
chmod 755 install_data.sh
./install_data.sh

# 5. Configure environment
cp .env.example .env
# Edit .env as needed (PORT, DB_PATH, DATA_PATH, PYTHON_COMMAND, etc.)
# If using the virtual environment above, set:
# PYTHON_COMMAND=.venv/bin/python

# 6. Ingest data files into the database
npm run ingest

# 7. Start the server
npm start

# Or combine steps 6 and 7:
npm run start:ingest-and-serve
```

## Data Files

Place your data files in the following directories before running ingestion.
All paths are configurable via environment variables (see Configuration below).
For the published data release, these directories and files are created by `install_data.sh`.

| Directory | Env var | File format |
|---|---|---|
| `data/networks/` | `DATA_PATH` | `*.csv` — one edge per line: `node1,node2,weight` |
| `data/indexes/` | `INDEXES_PATH` | Preprocessed graph index triplets such as `eu.adj.bin`, `eu.adj.index.bin`, `eu.node_ids.tsv` |
| `data/nodes_attr/` | `NODE_ATTRIBUTES_PATH` | Exactly one `*.nodes.attr` file — comma-separated, with a header row: `node_id`, `NCBI_txID`, `NH_ID`, `NH_Size`, … |
| `data/NCBI_txID/NCBI_txID.csv` | `TAXON_NAMES_PATH` | Two columns: `ncbi_txid,species_name` |

## Configuration

Copy `.env.example` to `.env` and override as needed. Key variables:

| Variable | Default | Description |
|---|---|---|
| `START_MODE` | `serve` | `serve` · `ingest` · `ingest-and-serve` |
| `DB_PATH` | `./data/network_viz.db` | SQLite file path — point to a volume mount in Docker |
| `DATA_PATH` | `./data/networks` | Directory containing network CSV files |
| `INDEXES_PATH` | `./data/indexes` | Directory containing preprocessed extraction indexes |
| `NODE_ATTRIBUTES_PATH` | `./data/nodes_attr` | Directory containing exactly one `.nodes.attr` file |
| `TAXON_NAMES_PATH` | `./data/NCBI_txID/NCBI_txID.csv` | NCBI taxonomy mapping CSV. `SPECIES_PATH` is still accepted as a backward-compatible alias. |
| `PYTHON_COMMAND` | `python3` | Python executable used for subnetwork extraction |
| `SUBNETWORK_SCRIPT_PATH` | `./tools/runtime/extract_subnetwork.py` | Extraction CLI path |
| `SUBNETWORK_JOB_TEMP_PATH` | `./data/tmp/subnetwork-jobs` | Controlled temp directory for extraction jobs |
| `SUBNETWORK_TIMEOUT_MS` | `120000` | Extraction timeout in milliseconds |
| `FILE_WATCH_ENABLED` | `true` | Set to `false` to disable hot-reload of data files |
| `PORT` | `3000` | HTTP port |
| `CORS_ORIGIN` | `*` | Allowed origin — restrict in production |

## Architecture Overview

The platform uses a clean separation between a backend data pipeline and a frontend visualization layer.

### Frontend Core (`frontend/js/core/`)

- **`app.js`** — Application entry point; bootstraps state, modules, and the D3 adapter.
- **`state.js`** (`AppState`) — Centralizes all application state: the core graph, expanded cluster set, node colours, highlight layers, hidden nodes, and the currently loaded network name.
- **`graph.js`** (`Graph`) — Framework-agnostic graph data structure (nodes Map, edges Map, adjacency Map). The canonical in-memory representation of the network topology and attributes.
- **`graph-view.js`** (`GraphView`) — Computes the *visible* graph from the core graph and the set of expanded clusters. Implements drill-down/roll-up: collapsed clusters are represented by a single cluster node; expanded clusters show individual protein nodes.
- **`module-system.js`** (`ModuleSystem`) — Plugin-style module loader. Modules register themselves and receive lifecycle calls (init, network-load, selection-change, etc.).

### Frontend Adapters, Components, and Modules

- **`adapters/d3-adapter.js`** — Bridges `AppState`/`GraphView` to D3.js force simulation and SVG rendering.
- **`components/species-tree-view.js`** — Renders the species tree UI used by taxonomy filtering.
- **`modules/`** — Self-contained feature modules: species selection, search highlighting, clear highlights, UniProt tooltip, export panel, and network editing.
- **`config/modules.js`** — Declares which modules are active for a given deployment.

### Backend

- **`scripts/ingestData.js`** — Data ingestion pipeline. Reads the single `.nodes.attr` file and network CSVs into SQLite. Node attribute ingestion is transactional: reconcile (clear all non-empty protein rows) then rewrite from the current attribute file.
- **`services/`** — Runtime services for file watching, subnetwork extraction, edited-network saving, and grouped exports.
- **`controllers/`** — Query and orchestration layer for network data, subnetwork jobs, and UniProt lookups.
- **`routes/`** — HTTP API routes for networks, subnetworks, species names, species tree data, and UniProt records.
- **`config/database.js`** — Opens the SQLite connection and initialises the schema (`nodes`, `edges` tables; `attribute_source` migration).

## Development

### Project Structure

```
frontend/
├── js/
│   ├── app.js                    # Application bootstrap
│   ├── landing.js                # Landing page behaviour
│   ├── config/
│   │   └── modules.js            # Active module declarations
│   ├── components/
│   │   └── species-tree-view.js  # Species tree UI component
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
│       ├── network-editor.js
│       ├── search-highlight.js
│       ├── species-selector.js
│       └── uniprot-tooltip.js
backend/
├── controllers/
│   ├── networkController.js     # Network and attribute data handling
│   ├── subnetworkController.js  # Subnetwork extraction API handling
│   └── uniprotController.js     # UniProt API integration
├── routes/
│   ├── networks.js              # Network data API endpoints
│   ├── subnetworks.js           # Subnetwork extraction endpoints
│   ├── species-tree.js          # Taxonomy tree endpoint
│   ├── species.js               # NCBI taxonomy name mappings
│   └── uniprot.js               # UniProt API endpoints
├── scripts/
│   ├── ingestData.js            # Data ingestion pipeline
├── services/
│   ├── fileWatcher.js           # Hot-reload watcher for data files
│   ├── groupExportService.js    # Grouped protein export writer
│   ├── networkEditService.js    # Edited-network save workflow
│   └── subnetworkService.js     # Subnetwork job orchestration
├── utils/
│   ├── requestValidation.js     # Shared API validation helpers
│   └── taxon-tree-parser.js     # NCBI common tree parser
└── entrypoint.js                # Startup mode router
```

## API Endpoints

| Endpoint | Description |
|---|---|
| `GET /health` | Liveness check — returns `{status, db, uptime}` |
| `GET /api/networks` | List ingested network sources |
| `GET /api/networks/:filename` | Fetch nodes and edges for a network |
| `POST /api/networks/search` | Find nodes by UniProt accession |
| `POST /api/networks/search-species` | Find nodes by NCBI taxonomy ID |
| `GET /api/subnetworks/limits` | Return client-facing extraction limits and available discovered indexes |
| `POST /api/subnetworks` | Run `tools/runtime/extract_subnetwork.py`, write a generated CSV to `data/networks`, and return a `/viewer.html?network=...` link |
| `GET /api/species-names` | NCBI taxonomy ID → species name mappings |
| `GET /api/uniprot/:accession` | UniProt protein data |

## Key Design Principles

1. **Single Source of Truth**: Each data type has one authoritative storage location
2. **Exact Matching**: All node operations use exact string matching only
3. **Event-Driven Architecture**: Components communicate through a centralized event bus
4. **Performance First**: Optimized for large networks (1000+ nodes)
5. **Zero Redundancy**: No duplicate code, data structures, or processing paths

## Testing

The Git repository includes backend unit and integration tests under `backend/tests/`.

```bash
npm test
```

Coverage includes data ingestion, route behavior, network editing, subnetwork job handling, species-tree parsing, frontend state logic that is tested in Node, and server startup behavior.

## Contributing

Contributions are welcome. Please keep changes aligned with the existing architecture and update documentation when behavior or setup steps change.

When contributing:
1. Use exact string matching for node operations.
2. Keep data ownership and runtime paths unambiguous.
3. Add appropriate tests for new behavior.
4. Update documentation for architectural or setup changes.

## License

This project is licensed under the ISC license, as declared in `package.json`.
 
