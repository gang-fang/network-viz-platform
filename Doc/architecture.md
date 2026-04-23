# Network Visualization Platform — Architecture

This document describes the current architecture. Update it whenever structural changes are made.

---

## Overview

The platform has two independent layers that share no code:

- **Backend** — Node.js/Express server, SQLite database, data ingestion pipeline.
- **Frontend** — Vanilla JS visualization loaded as static files from the same server.

---

## Backend

### Entry point and startup modes

`backend/entrypoint.js` reads `START_MODE` from the environment and branches:

| Mode | Behaviour |
|---|---|
| `serve` (default) | Auto-ingests on first run if the DB is empty, then starts the server |
| `ingest` | Runs ingestion and exits |
| `ingest-and-serve` | Ingests, then starts the server |

### Data ingestion (`backend/scripts/ingestData.js`)

Two independent pipelines share a single module:

**Network ingestion** (`ingestNetworks`): reads `*.csv` files from `DATA_PATH` and writes edges + placeholder nodes into SQLite. Format: `node1,node2,weight` per line, no header.

**Attribute ingestion** (`ingestNodeAttributes`): reads the files listed in `NODE_ATTRIBUTE_FILES` from `NODE_ATTRIBUTES_PATH`. The pipeline is:

1. **Preflight** (`validateNodeAttributeFiles`) — scans all selected files and rejects the run if any UniProt AC appears more than once across the set.
2. **Reconcile** — inside a single transaction, clears `attributes_json` and `attribute_source` on every protein row that has non-empty attributes. This ensures removed or moved proteins don't carry stale data.
3. **Write** — re-inserts attributes from each file. Existing `{}` placeholder rows (from network ingestion) are updated in place; new proteins get `INSERT`.
4. **Commit or rollback** — a mid-write failure rolls back the entire transaction, including the reconcile, leaving the DB in its prior state.

Batch memory management uses SQLite `SAVEPOINT`/`RELEASE` within the outer transaction.

### File watcher (`backend/services/fileWatcher.js`)

Chokidar watches `DATA_PATH` and `NODE_ATTRIBUTES_PATH`:

- **`.csv` added/changed** → `ingestNetworks(filename)` for that file only.
- **`.csv` deleted** → edges for that source removed from DB.
- **`.nodes.attr` changed** → if the filename is in `NODE_ATTRIBUTE_FILES`, re-runs the full `ingestNodeAttributes` for the complete configured set (preflight requires the full set).
- **`.nodes.attr` not in `NODE_ATTRIBUTE_FILES`** → ignored with a log message.

### Database schema (`backend/config/database.js`)

```sql
CREATE TABLE nodes (
  id               TEXT PRIMARY KEY,
  kind             TEXT NOT NULL,        -- 'protein'
  attributes_json  TEXT,                 -- JSON object; '{}' = placeholder
  attribute_source TEXT                  -- filename that wrote attributes
);

CREATE TABLE edges (
  id           TEXT NOT NULL,            -- canonical: node1|node2
  node1        TEXT NOT NULL,
  node2        TEXT NOT NULL,
  weight       REAL,
  source       TEXT NOT NULL,            -- source CSV filename
  attributes_json TEXT,
  PRIMARY KEY (id, source),
  FOREIGN KEY(node1) REFERENCES nodes(id),
  FOREIGN KEY(node2) REFERENCES nodes(id)
);
```

`attribute_source` is added via `ALTER TABLE` migration for databases created before the column existed.

### API routes

| Endpoint | Handler | Description |
|---|---|---|
| `GET /api/networks` | `networkController.listNetworks` | Distinct `source` values from edges |
| `GET /api/networks/:filename` | `networkController.getNetworkData` | Nodes (with attributes) + edges for one network |
| `POST /api/networks/search` | `networkController.searchProteins` | Find nodes by UniProt accession |
| `POST /api/networks/search-species` | `networkController.searchBySpecies` | Find nodes by NCBI taxonomy ID |
| `GET /api/species-names` | `routes/species.js` | NCBI txID → species name (read from CSV on every request) |
| `GET /api/uniprot/:accession` | `uniprotController` | Proxied UniProt REST API call |
| `GET /health` | inline | Liveness check |

---

## Frontend

The frontend is plain ES5-compatible JavaScript served as static files. There is no build step.

### Core (`frontend/js/core/`)

| File | Class | Role |
|---|---|---|
| `graph.js` | `Graph` | Framework-agnostic graph data structure. Stores nodes and edges in `Map`s plus an adjacency `Map`. |
| `state.js` | `AppState` | Centralises all application state: the core `Graph`, the current view `Graph`, `expandedClusters` Set, node colours, highlight layers, hidden nodes, current network name. |
| `graph-view.js` | `GraphView` | Computes the *visible* graph from `AppState`. Collapsed clusters are represented by a single cluster node; expanded clusters show individual protein nodes (drill-down/roll-up). |
| `module-system.js` | `ModuleSystem` | Plugin loader. Modules register themselves and receive lifecycle calls (`init`, `onNetworkLoad`, `onSelectionChange`, …). |

### Adapter (`frontend/js/adapters/`)

`d3-adapter.js` — bridges `AppState`/`GraphView` to D3.js force simulation and SVG rendering. Holds no business logic; translates graph state changes into D3 operations.

### Modules (`frontend/js/modules/`)

Self-contained feature modules registered via `frontend/js/config/modules.js`:

| Module | Purpose |
|---|---|
| `species-selector.js` | Filter visible nodes by NCBI taxonomy ID |
| `search-highlight.js` | Highlight nodes matching a search query |
| `clear-highlights.js` | Reset all highlight layers |
| `uniprot-tooltip.js` | Fetch and display UniProt data on node hover |
| `export-panel.js` | Export the current view as an image or data file |

To add a new feature, create a module file and register it in `config/modules.js`. No core files need to change.

### Entry point

`frontend/js/app.js` — creates `AppState`, instantiates `ModuleSystem`, loads the configured modules, and hands off to the D3 adapter.

---

## File tree (abbreviated)

```
backend/
├── config/
│   ├── config.js            # Env-var driven configuration
│   └── database.js          # SQLite connection + schema init
├── controllers/
│   ├── networkController.js # Query layer over SQLite
│   └── uniprotController.js # UniProt REST proxy
├── routes/
│   ├── networks.js          # Network API endpoints
│   ├── species.js           # Species name endpoint
│   └── uniprot.js           # UniProt endpoint
├── scripts/
│   ├── ingestData.js        # Ingestion pipeline
│   └── configureAttrs.js    # Interactive NODE_ATTRIBUTE_FILES helper
├── services/
│   └── fileWatcher.js       # Chokidar hot-reload watcher
└── entrypoint.js            # Startup mode router

frontend/
├── js/
│   ├── app.js
│   ├── config/modules.js
│   ├── core/
│   │   ├── graph.js
│   │   ├── graph-view.js
│   │   ├── module-system.js
│   │   └── state.js
│   ├── adapters/d3-adapter.js
│   └── modules/
│       ├── clear-highlights.js
│       ├── export-panel.js
│       ├── search-highlight.js
│       ├── species-selector.js
│       └── uniprot-tooltip.js
└── index.html
```

---

## Key design principles

1. **Transactional ingestion** — attribute ingestion is all-or-nothing. Partial writes never persist.
2. **Explicit file selection** — `NODE_ATTRIBUTE_FILES` is required; the system never silently ingests whatever it finds.
3. **Provenance tracking** — `attribute_source` records which file wrote each protein's attributes, enabling correct reconcile and conflict detection.
4. **NH nodes are frontend-only** — cluster (NH) nodes are derived at render time by `GraphView` from protein `NH_ID` attributes; they are never stored in the database.
5. **Module isolation** — frontend features are self-contained modules with no cross-dependencies; adding or removing one does not affect others.
