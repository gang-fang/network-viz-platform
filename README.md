# SJI Network Explorer

SJI Network Explorer is a local web application for extracting, visualizing, editing, and exporting protein-similarity subnetworks from pre-built Signal Jaccard Index (SJI) networks.

The main use case is to start from one or more UniProt accessions, extract a focused subnetwork from the pre-built SJI networks, and then interactively study network topology, species composition, SJI edge structure, and selected protein groups. The package includes a Node.js/Express server, a D3-based network viewer, a fast Python subnetwork extractor, and preprocessing scripts for users who want to attach their own proteins or build custom SJI networks.

> **Required data folder**
>
> The Docker image and source repository do **not** contain the pre-built SJI networks. Before running the application, you must create a local folder named `data/`. This folder contains the pre-built SJI network CSV files, extraction indexes, node attributes, and taxonomy files. If `data/` is missing or incomplete, the application will not be usable.

## What Is the Signal Jaccard Index?

The **Signal Jaccard Index (SJI)** is a protein-similarity metric based on shared homolog neighborhoods. Instead of comparing two proteins only by direct pairwise sequence similarity, SJI compares the sets of **signal proteins** associated with each protein.

For a given seed protein, similar proteins are retrieved across many proteomes and placed on a two-dimensional plot. The x-axis represents protein-length ratio relative to the seed protein, and the y-axis represents full-length sequence similarity. Homologs often separate into two regions: a closer **signal** group and a more distant **noise** group. The boundary is not a universal similarity cutoff. It is inferred from the local density structure of each protein-specific plot, so each seed protein has its own signal/noise separation.

Given two proteins A and B, SJI is the Jaccard index of their signal sets:

```text
SJI(A, B) = |signal(A) ∩ signal(B)| / |signal(A) ∪ signal(B)|
```

A high SJI means that two proteins share much of the same signal homolog neighborhood across proteomes. This makes SJI sensitive to broader evolutionary and genomic context, including cases where one-to-one orthology or a single pairwise alignment score is not sufficient.

## Why Is SJI Needed?

SJI was developed to address a practical problem in ortholog-based functional annotation. When different ortholog databases are used to annotate the same sequences, such as enriched reads from metagenomic data, they may assign those sequences to different ortholog groups. Downstream functional annotations can therefore diverge, affecting comparative genomics, pan-genome analysis, metagenomic interpretation, and tests of the ortholog conjecture.

![SJI framework overview](frontend/docs/aFig1.png)

**Figure 1. SJI framework overview.** Panel A illustrates a schematic fitness landscape with three major peaks. Panel B shows how these peaks can appear as blocks in a multiple sequence alignment: within a block, both local sequence similarity and full-length similarity are high; across blocks, both decrease. Panel C shows the two-dimensional signal/noise plot, where signal proteins cluster above the noise cloud. Panel D shows SJI as the Jaccard index between two signal sets. Panel E shows the network-level interpretation: proteins with relatively stable orthology predictions tend to form the network core, whereas proteins with database-dependent orthology assignments tend to appear toward the periphery. In the reference-proteome analysis, these peripheral proteins account for more than 50% of the proteins in UniProt reference proteomes.

![SJI signal and noise concept](frontend/docs/aFig2.png)

**Figure 2. Signal/noise separation.** Sequence-alignment tools such as BLAST report similarity scores with well-defined statistical measures, but downstream cutoff choices are often researcher-defined and may vary across studies. SJI avoids a single universal cutoff. For each seed protein, the signal set is inferred from its own similarity-versus-length-ratio distribution using spectral clustering, producing an objective protein-specific separation between signal and noise homologs. The method is described in more detail in the [SJI reference](https://bmcbioinformatics.biomedcentral.com/articles/10.1186/s12859-024-06023-x).

## What This Package Provides

- **Pre-built SJI exploration**: extract focused subnetworks from large pre-built SJI networks by entering UniProt accessions.
- **Interactive network viewer**: load saved networks, expand or collapse neighborhood clusters, zoom, pan, and inspect network topology.
- **Species-aware filtering**: use NCBI taxonomy mappings and the common tree to highlight or filter proteins by species or clade.
- **Protein and edge highlighting**: locate proteins by UniProt accession, highlight proteins by species, and highlight SJI edges within a chosen value range.
- **Network editing**: remove selected proteins or edges by SJI threshold, then save edited networks as new CSV files.
- **Export and downstream analysis**: export protein lists, grouped UniProt accession files, and selected network views for additional structural, functional, or evolutionary analysis.
- **Advanced workflows**: use `topN` and Python preprocessing scripts to attach user-supplied proteins/proteomes or build custom SJI networks.

The tutorial includes examples for extracting subnetworks around seed proteins, expanding and collapsing neighborhood clusters, highlighting proteins by species, highlighting SJI edges above or within a threshold range, editing and saving cleaned subnetworks, and using the Batch & Comparative Analysis panel to prepare grouped UniProt accession lists.

For the full guide, open:

```text
frontend/docs/Tutorial.html
```

## Data Bundle

The current pre-built release covers **406 UniProt reference proteomes**:

- **51 eukaryotic reference proteomes**
- **355 bacterial reference proteomes**

The installed data bundle is organized into network CSV files, binary extraction indexes, node-attribute files, and taxonomy files. The current taxonomy files are:

- `data/NCBI_txID/NCBI_txID.csv` — two-column mapping: `ncbi_txid,species_name`
- `data/NCBI_txID/commontree.txt` — NCBI common tree used by the species selector

The pre-built networks represent proteins by UniProt accession. Queries against the pre-built networks and subnetwork extraction are therefore accession-based.

### Recommended Data Download: Zenodo

The easiest setup is to download `data.tgz` from the [Zenodo SJI data record](https://zenodo.org/records/20097975?preview=1&token=eyJhbGciOiJIUzUxMiJ9.eyJpZCI6IjViZjgwZDRiLWQzYTYtNDU1Yi1iNGIxLTY1MmM5ZTVkMGM0OSIsImRhdGEiOnt9LCJyYW5kb20iOiI4ZmNlYjc3OTE3NWQ4YmZkMWIwMzA2ZDhiM2Y2MjFiMiJ9.QJA7D8Mzu1V9wWKzebWBqb_BXkdQmr2Bn_ZeGL1oDlNL_Q440G_lFRpzkUAWJLRcRR0YDJdBfG-1rThd5LFStQ).

Unpack it in the directory where you will run Docker:

```bash
# Put data.tgz in your working directory, then run:
tar -xzf data.tgz

# Confirm this exists before starting the container:
ls data
```

After unpacking, the current directory must contain:

```text
data/
```

### Alternative Data Download: GitHub Release

The GitHub data release remains available for users who prefer the original release assets and `install_data.sh` workflow:

```text
https://github.com/gang-fang/network-viz-platform/releases/tag/qfo-reference-proteomes-data-2026
```

Download the required `.gz` assets and `install_data.sh`, then run:

```bash
chmod 755 install_data.sh
./install_data.sh
```

This workflow also creates the required `data/` folder.

## Quick Start With Docker

This is the recommended path for most users.

Install Docker Desktop:

```text
https://www.docker.com/products/docker-desktop/
```

Pull the Docker image:

```bash
docker pull ghcr.io/gang-fang/sji-network-explorer:latest
```

Download `data.tgz` from the [Zenodo SJI data record](https://zenodo.org/records/20097975?preview=1&token=eyJhbGciOiJIUzUxMiJ9.eyJpZCI6IjViZjgwZDRiLWQzYTYtNDU1Yi1iNGIxLTY1MmM5ZTVkMGM0OSIsImRhdGEiOnt9LCJyYW5kb20iOiI4ZmNlYjc3OTE3NWQ4YmZkMWIwMzA2ZDhiM2Y2MjFiMiJ9.QJA7D8Mzu1V9wWKzebWBqb_BXkdQmr2Bn_ZeGL1oDlNL_Q440G_lFRpzkUAWJLRcRR0YDJdBfG-1rThd5LFStQ), then unpack it:

```bash
tar -xzf data.tgz
ls data
```

Run the server from the same directory that contains `data/`:

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

If the app opens but no networks or subnetworks are available, check the volume mount in the Docker command. The argument `-v "$PWD/data:/app/data"` requires `$PWD/data` to exist on your computer before the container starts.

Stop the server with:

```bash
docker stop sji-network-explorer
```

## Basic Workflow

1. Create the required `data/` folder from `data.tgz` or the GitHub data release.
2. Start the application with Docker or from a local clone.
3. Open `http://localhost:3000` in Chrome or Firefox.
4. Enter one or more UniProt accessions and extract a focused subnetwork.
5. Open the extracted network in the viewer.
6. Expand neighborhood clusters to inspect individual proteins.
7. Highlight proteins by UniProt accession or by species.
8. Highlight SJI edges within a chosen SJI range to inspect strong or weak connections.
9. Remove less relevant proteins or weak SJI edges, then save edited networks under new names.
10. Export protein lists or grouped accession files for downstream analysis.

## Local Source Installation

Use this path if you want the source code, advanced preprocessing scripts, or a local development setup:

```bash
git clone https://github.com/gang-fang/network-viz-platform.git
cd network-viz-platform
```

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

# 4. Download and organize the release data.
# Recommended: download data.tgz from Zenodo and unpack it here.
tar -xzf data.tgz
ls data
#
# Alternative: use the GitHub release assets plus install_data.sh:
# chmod 755 install_data.sh
# ./install_data.sh

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

The advanced workflows described in `frontend/docs/Tutorial.html` run outside the Docker container and use scripts under `tools/preprocessing` and `tools/bin`.

## Data Files

Place data files in the following directories before running ingestion. All paths are configurable via environment variables; see [Configuration](#configuration). For the published data release, these directories and files are created by `data.tgz` or by the `install_data.sh` workflow.

| Directory | Env var | File format |
|---|---|---|
| `data/networks/` | `DATA_PATH` | `*.csv` — one edge per line: `node1,node2,weight` |
| `data/exports/` | `EXPORTS_PATH` | Batch-analysis group exports written as `.txt` files |
| `data/indexes/` | `INDEXES_PATH` | Preprocessed graph index triplets such as `eu.adj.bin`, `eu.adj.index.bin`, `eu.node_ids.tsv` |
| `data/nodes_attr/` | `NODE_ATTRIBUTES_PATH` | Exactly one `*.nodes.attr` file — comma-separated, with a header row: `node_id`, `NCBI_txID`, `NH_ID`, `NH_Size`, … |
| `data/NCBI_txID/NCBI_txID.csv` | `TAXON_NAMES_PATH` | Two columns: `ncbi_txid,species_name` |
| `data/NCBI_txID/commontree.txt` | `TAXON_TREE_PATH` | NCBI Common Tree ASCII taxonomy tree |

## Configuration

Copy `.env.example` to `.env` and override values only when needed. Key variables:

| Variable | Default | Description |
|---|---|---|
| `START_MODE` | `serve` | `serve` · `ingest` · `ingest-and-serve` |
| `DB_PATH` | `./data/network_viz.db` | SQLite file path — point to a volume mount in Docker |
| `DATA_PATH` | `./data/networks` | Directory containing network CSV files |
| `EXPORTS_PATH` | `./data/exports` | Directory for grouped UniProt export `.txt` files |
| `INDEXES_PATH` | `./data/indexes` | Directory containing preprocessed extraction indexes |
| `NODE_ATTRIBUTES_PATH` | `./data/nodes_attr` | Directory containing exactly one `.nodes.attr` file |
| `TAXON_NAMES_PATH` | `./data/NCBI_txID/NCBI_txID.csv` | NCBI taxonomy mapping CSV. `SPECIES_PATH` is still accepted as a backward-compatible alias. |
| `TAXON_TREE_PATH` | `./data/NCBI_txID/commontree.txt` | NCBI Common Tree ASCII taxonomy tree |
| `PYTHON_COMMAND` | `python3` | Python executable used for subnetwork extraction |
| `SUBNETWORK_SCRIPT_PATH` | `./tools/runtime/extract_subnetwork.py` | Extraction CLI path |
| `SUBNETWORK_JOB_TEMP_PATH` | `./data/tmp/subnetwork-jobs` | Controlled temp directory for extraction jobs |
| `SUBNETWORK_TIMEOUT_MS` | `120000` | Extraction timeout in milliseconds |
| `FILE_WATCH_ENABLED` | `true` | Set to `false` to disable hot-reload of data files |
| `PORT` | `3000` | HTTP port |
| `CORS_ORIGIN` | `*` | Allowed origin — restrict in production |

## Advanced Topics and Additional Data

The quick-start Docker workflow is sufficient for extracting, viewing, editing, and exporting focused subnetworks from the pre-built SJI networks. Advanced workflows are available for users who want to go beyond interactive exploration, including:

- preparing new protein or proteome inputs for SJI-style analysis
- using `topN` and preprocessing scripts to generate nearest-neighbor and signal data
- extracting subnetworks from command-line workflows
- comparing selected communities with protein 2D plots and seed-protein signal sets
- rebuilding or extending SJI network resources for custom datasets

The [Zenodo SJI data record](https://zenodo.org/records/20097975?preview=1&token=eyJhbGciOiJIUzUxMiJ9.eyJpZCI6IjViZjgwZDRiLWQzYTYtNDU1Yi1iNGIxLTY1MmM5ZTVkMGM0OSIsImRhdGEiOnt9LCJyYW5kb20iOiI4ZmNlYjc3OTE3NWQ4YmZkMWIwMzA2ZDhiM2Y2MjFiMiJ9.QJA7D8Mzu1V9wWKzebWBqb_BXkdQmr2Bn_ZeGL1oDlNL_Q440G_lFRpzkUAWJLRcRR0YDJdBfG-1rThd5LFStQ) also hosts additional data for these advanced workflows.

In addition to `data.tgz`, the record includes separate gzip tarballs for:

- protein 2D plots for bacteria
- protein 2D plots for eukaryotes
- signal sets for seed proteins in bacteria
- signal sets for seed proteins in eukaryotes

These advanced tarballs are not required for the basic Docker viewer workflow. They are intended for users who want to inspect protein-specific signal/noise plots, reproduce or extend preprocessing steps, or perform additional analyses outside the web viewer.

For step-by-step examples, see:

```text
frontend/docs/Tutorial.html
```

For the conceptual background on SJI, see:

```text
frontend/docs/About SJI.html
```

For implementation details and preprocessing scripts, start with:

```text
tools/preprocessing/
```

## Architecture Overview

The platform separates the backend data pipeline from the frontend visualization layer.

### Frontend Core (`frontend/js/core/`)

- **`app.js`** — Application entry point; bootstraps state, modules, and the D3 adapter.
- **`state.js`** (`AppState`) — Centralizes application state: the core graph, expanded cluster set, node colors, highlight layers, hidden nodes, and the currently loaded network name.
- **`graph.js`** (`Graph`) — Framework-agnostic graph data structure (nodes Map, edges Map, adjacency Map). The canonical in-memory representation of the network topology and attributes.
- **`graph-view.js`** (`GraphView`) — Computes the *visible* graph from the core graph and the set of expanded clusters. Implements drill-down/roll-up: collapsed clusters are represented by a single cluster node; expanded clusters show individual protein nodes.
- **`module-system.js`** (`ModuleSystem`) — Plugin-style module loader. Modules register themselves and receive lifecycle calls (init, network-load, selection-change, etc.).

### Frontend Adapters, Components, and Modules

- **`adapters/d3-adapter.js`** — Bridges `AppState`/`GraphView` to D3.js force simulation and SVG rendering.
- **`components/species-tree-view.js`** — Renders the species tree UI used by taxonomy filtering.
- **`modules/`** — Self-contained feature modules: species selection, search highlighting, clear highlights, UniProt tooltip, export panel, and network editing.
- **`config/modules.js`** — Declares which modules are active for a given deployment.

### Backend

- **`scripts/ingestData.js`** — Data ingestion pipeline. Reads the single `.nodes.attr` file and network CSVs into SQLite. Node attribute ingestion is transactional: reconcile (clear all non-empty rows) then rewrite from the current attribute file.
- **`services/`** — Runtime services for file watching, subnetwork extraction, edited-network saving, and grouped exports.
- **`controllers/`** — Query and orchestration layer for network data, subnetwork jobs, and UniProt lookups.
- **`routes/`** — HTTP API routes for networks, subnetworks, species names, species tree data, and UniProt availability checks.
- **`config/database.js`** — Opens the SQLite connection and initializes the schema (`nodes`, `edges`, and `network_nodes` tables).

## Development

### Project Structure

```
frontend/
├── js/
│   ├── app.js                    # Application bootstrap
│   ├── landing.js                # Landing page behavior
│   ├── config/
│   │   └── modules.js            # Active module declarations
│   ├── components/
│   │   └── species-tree-view.js  # Species tree UI component
│   ├── core/
│   │   ├── graph.js              # Graph data structure (nodes, edges, adjacency)
│   │   ├── graph-view.js         # Visible-graph computation (drill-down/roll-up)
│   │   ├── module-system.js      # Module loader and lifecycle
│   │   └── state.js              # Centralized application state
│   ├── adapters/
│   │   └── d3-adapter.js         # D3.js rendering bridge
│   └── modules/                  # Self-contained feature modules
│       ├── clear-highlights.js
│       ├── export-panel.js
│       ├── network-editor.js
│       ├── search-highlight.js
│       ├── sji-edge-highlight.js
│       ├── species-selector.js
│       └── uniprot-tooltip.js
backend/
├── config/
│   ├── config.js                # Environment-backed runtime configuration
│   ├── database.js              # SQLite connection and schema initialization
│   └── dbMethods.js             # Promise-based SQLite helper methods
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
│   ├── fileReservation.js       # Filename suffix reservation helpers
│   ├── logger.js                # Winston logger configuration
│   ├── networkErrors.js         # Shared network-domain errors
│   ├── networkQueries.js        # Shared network SQL fragments and count queries
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
| `POST /api/networks/edited` | Save the current edited network as a new reusable network |
| `POST /api/networks/group-exports` | Save grouped UniProt accession exports as `.txt` files |
| `GET /api/networks/:filename/status` | Return readiness and node/edge counts for a network |
| `GET /api/subnetworks/limits` | Return client-facing extraction limits and available discovered indexes |
| `POST /api/subnetworks` | Run `tools/runtime/extract_subnetwork.py`, write a generated CSV to `data/networks`, and return a `/viewer.html?network=...` link |
| `GET /api/species-names` | NCBI taxonomy ID → species name mappings |
| `GET /api/species-tree` | Return the annotated NCBI taxonomy tree |
| `POST /api/uniprot/availability` | Check whether UniProt accessions currently resolve to entries |

## Key Design Principles

1. **Single source of truth**: Each data type has one authoritative storage location.
2. **Exact matching**: Node operations use exact string matching.
3. **Event-driven architecture**: Components communicate through a centralized event bus.
4. **Performance first**: Runtime paths are optimized for large networks.
5. **Low redundancy**: Shared behavior is centralized where it reduces maintenance risk.

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

## Authorship and Code Generation

The full package was authored and assembled by Gang Fang (`fang.fg@gmail.com`).

Most of the implementation code, including the C++ `topN` code and the Python and shell preprocessing scripts, was generated with AI coding agents based on Claude and Codex under Gang Fang's direction and review.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE).
 
