# Testing Summary

## How to run

```bash
npm test              # run the full test suite
npm run test:backend  # same — currently the only suite
```

## What is tested

All tests are backend-only. There are no frontend tests.

### Suite breakdown

| Suite | Type | File |
|---|---|---|
| Attribute ingestion — unit | Unit | `backend/tests/unit/ingestData.test.js` |
| Data ingestion — integration | Integration (real SQLite) | `backend/tests/integration/ingestData.integration.test.js` |
| Network controller — integration | Integration (real SQLite) | `backend/tests/integration/networkController.integration.test.js` |
| Network routes | Integration (supertest) | `backend/tests/integration/routes/networks.test.js` |
| Server startup | Unit | `backend/tests/unit/server_startup.test.js` |
| Server basic routes | Integration (supertest) | `backend/tests/unit/server.test.js` |

### Attribute ingestion — unit (`ingestData.test.js`)

**`validateNodeAttributeFiles`**
- Single clean file passes
- Two clean files with no overlapping UniProt ACs pass
- Within-file duplicate throws, naming the file and both line numbers
- Cross-file duplicate throws, naming both files
- Header row `node_id` is not treated as a protein entry

**Reconcile SQL shape**
- The reconcile `UPDATE` clears all non-empty protein rows unconditionally (`attributes_json != '{}'`, no per-file filtering)

**Argument validation**
- Empty array, `null`, `undefined` all throw with a message mentioning `NODE_ATTRIBUTE_FILES`

**File-watcher attr-file filtering**
- A `.nodes.attr` file not listed in `NODE_ATTRIBUTE_FILES` is ignored
- A configured file change triggers re-ingestion of the full configured set

### Data ingestion — integration (`ingestData.integration.test.js`)

Uses a real in-memory SQLite database (no mocks for DB behaviour).

**`ingestNodeAttributes`**

| Test | What it verifies |
|---|---|
| Legacy NULL-source rows cleared | Protein rows with non-empty attrs but `attribute_source = NULL` are cleared by reconcile |
| Legacy rows in current selection re-written | Cleared then re-written with correct data from the file |
| Protein removed from selected file | Row becomes `{}` placeholder after re-ingestion |
| Protein moved between selected files | Correctly reassigned to the new source file |
| No `kind='nh'` rows written | Ingestion never persists NH/cluster nodes |
| Pre-existing `kind='nh'` rows removed | Reconcile cleans up rows written by older code |
| Write-phase failure rolls back | A mid-write SQLite `RAISE(FAIL)` trigger causes full rollback; prior state is restored |

**`ingestNetworks`**

| Test | What it verifies |
|---|---|
| Stale edges removed on re-ingest | `DELETE … WHERE source = ?` before replay removes edges that were deleted from the CSV |
| FK enforcement succeeds when node IDs are new | Placeholder nodes are inserted before edges; no FK violation when `foreign_keys = ON` |
| Mid-write failure rolls back all edges for a source | A `TEMP TRIGGER RAISE(FAIL)` causes full rollback; canary edges from other sources are intact |
| Malformed rows are skipped | Rows with < 3 columns, blank node IDs, or non-finite weights produce no edges or placeholder nodes |

### Network controller — integration (`networkController.integration.test.js`)

Uses a real in-memory SQLite database (no mocks for DB behaviour).

| Test | What it verifies |
|---|---|
| Unknown source throws "Network not found" | Error message preserves the "not found" marker so the route maps it to 404 |
| Known source returns nodes and edges | Correct elements shape with node IDs and edge weight |
| `searchProteins` scopes to requested network | Accession present only in a different network source is not returned |
| `searchProteins` returns `nh_id` from attributes | `NH_ID` is correctly extracted from `attributes_json` |

### Network routes (`routes/networks.test.js`)

Supertest against the Express app with a mocked `networkController`.

- `GET /api/networks` — 200 with list; 500 on controller error
- `GET /api/networks/:filename` — 200 with elements; 404 on not-found

### Server tests

- Server file exists and starts without errors
- Required routes are configured
- `GET /` returns 200; unknown route returns 404; `GET /api/networks` returns 200

## Test fixtures

Located in `backend/tests/fixtures/`:

| File | Contents |
|---|---|
| `test-e.nodes.attr` | 3 proteins: node1 (NH001), node2 (NH002), node3 (NH003) |
| `test-p.nodes.attr` | 2 proteins: node4 (NH101), node5 (NH102) |
| `test-dup-within.nodes.attr` | DUPNODE1 appears twice (tests within-file duplicate detection) |
| `test-dup-cross-a.nodes.attr` | CROSSNODE1, CROSSNODE2 |
| `test-dup-cross-b.nodes.attr` | CROSSNODE2 (overlap), CROSSNODE3 (tests cross-file duplicate detection) |
| `test-network.csv` | 5 edges (headerless): node1-node2, node1-node3, node2-node4, node3-node4, node1-node5 |
| `malformed.csv` | Rows with < 3 columns, blank node IDs, non-numeric weights (`notanum`), partial-numeric weights (`1abc`), and non-finite weights (`Infinity`) — all must be skipped |
