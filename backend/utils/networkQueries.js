const { dbGet } = require('../config/dbMethods');

const SCOPED_NODE_IDS_SQL = `
  SELECT node_id AS id FROM network_nodes WHERE source = ?
  UNION
  SELECT node1 AS id FROM edges WHERE source = ?
  UNION
  SELECT node2 AS id FROM edges WHERE source = ?
`;

function getSourceParams(source) {
  return [source, source, source];
}

function scopedNodesCte() {
  return `WITH scoped_nodes(id) AS (${SCOPED_NODE_IDS_SQL})`;
}

function scopedNodeCountSql() {
  return `
    SELECT COUNT(*) AS count
    FROM (${SCOPED_NODE_IDS_SQL})
  `;
}

async function fetchNetworkCounts(filename) {
  const edgeRow = await dbGet(
    'SELECT COUNT(*) AS count FROM edges WHERE source = ?',
    [filename]
  );

  const nodeRow = await dbGet(
    scopedNodeCountSql(),
    getSourceParams(filename)
  );

  return {
    edgeCount: edgeRow?.count || 0,
    nodeCount: nodeRow?.count || 0,
  };
}

module.exports = {
  SCOPED_NODE_IDS_SQL,
  fetchNetworkCounts,
  getSourceParams,
  scopedNodeCountSql,
  scopedNodesCte,
};
