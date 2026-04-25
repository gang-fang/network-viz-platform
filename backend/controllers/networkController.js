const db = require('../config/database');
const logger = require('../utils/logger');
const util = require('util');
const networkEditService = require('../services/networkEditService');

// Promisify DB methods
const dbAll = util.promisify(db.all.bind(db));

/**
 * List all available network files (sources)
 * @returns {Promise<Array>} Array of network filenames
 */
async function listNetworks() {
  try {
    logger.info('Listing networks from database');
    const rows = await dbAll(`
      SELECT source FROM edges WHERE source IS NOT NULL
      UNION
      SELECT source FROM network_nodes WHERE source IS NOT NULL
      ORDER BY source
    `);
    return rows.map(row => row.source);
  } catch (err) {
    logger.error(`Error listing networks: ${err.message}`);
    throw new Error('Failed to list network files');
  }
}

/**
 * Get node attributes data for specific nodes
 * @param {Array} nodeIds - Array of node IDs to get attributes for
 * @returns {Promise<Object>} Node attributes mapped by node ID
 */
async function getNodeAttributes(nodeIds) {
  try {
    if (!nodeIds || nodeIds.length === 0) return {};

    // SQLite limit for variables is usually 999 or 32766 depending on version
    // We'll process in chunks to be safe
    const CHUNK_SIZE = 500;
    const nodeAttributesMap = {};

    for (let i = 0; i < nodeIds.length; i += CHUNK_SIZE) {
      const chunk = nodeIds.slice(i, i + CHUNK_SIZE);
      const placeholders = chunk.map(() => '?').join(',');

      const rows = await dbAll(
        `SELECT id, attributes_json FROM nodes WHERE id IN (${placeholders})`,
        chunk
      );

      rows.forEach(row => {
        try {
          const attrs = JSON.parse(row.attributes_json || '{}');
          nodeAttributesMap[row.id] = {
            node_id: row.id,
            ...attrs
          };
        } catch (e) {
          logger.warn(`Failed to parse attributes for node ${row.id}`);
        }
      });
    }

    logger.info(`Loaded attributes for ${Object.keys(nodeAttributesMap).length} nodes`);
    return nodeAttributesMap;
  } catch (err) {
    logger.error(`Error getting node attributes: ${err.message}`);
    return {};
  }
}

/**
 * Get network data from a specific source (file)
 * @param {string} filename - Name of the network file (source)
 * @returns {Promise<Object>} Network data
 */
async function getNetworkData(filename) {
  try {
    logger.info(`Fetching network data for source: ${filename}`);

    // Fetch edges
    const edges = await dbAll(
      "SELECT id, node1, node2, weight, attributes_json FROM edges WHERE source = ?",
      [filename]
    );

    // Fetch explicitly registered network members, then include all edge endpoints
    // as a fallback for databases that predate the network_nodes table.
    const nodeIds = new Set();
    const networkNodeRows = await dbAll(
      "SELECT node_id FROM network_nodes WHERE source = ?",
      [filename]
    );
    networkNodeRows.forEach(row => nodeIds.add(row.node_id));

    edges.forEach(e => {
      nodeIds.add(e.node1);
      nodeIds.add(e.node2);
    });

    if (nodeIds.size === 0) {
      throw new Error(`Network not found: ${filename}`);
    }

    // Fetch node attributes
    // We use the helper function but we need to format it for the response
    const nodesMap = await getNodeAttributes(Array.from(nodeIds));

    // Format for Cytoscape/D3
    const elements = {
      nodes: Array.from(nodeIds).map(id => {
        const attrs = nodesMap[id] || {};
        return {
          data: {
            id: id,
            ...attrs
          }
        };
      }),
      edges: edges.map(e => {
        let attrs = {};
        try {
          attrs = JSON.parse(e.attributes_json || '{}');
        } catch (err) { }

        return {
          data: {
            id: e.id,
            source: e.node1,
            target: e.node2,
            weight: e.weight,
            ...attrs
          }
        };
      })
    };

    return { elements };
  } catch (err) {
    // Re-throw not-found errors as-is so the route can map them to 404.
    if (err.message.startsWith('Network not found:')) throw err;
    logger.error(`Error reading network ${filename}: ${err.message}`);
    throw new Error(`Failed to read network: ${filename}`);
  }
}

/**
 * Search for proteins by Accession
 * @param {string} networkName - Network source file
 * @param {Array} accessions - List of accessions to search for
 * @returns {Promise<Object>} Found nodes with their cluster IDs
 */
async function searchProteins(networkName, accessions) {
  try {
    if (!accessions || accessions.length === 0) return { matches: [] };

    // Restrict to nodes that participate in edges from the requested network so
    // that accessions present in a different ingested network are not returned.
    const placeholders = accessions.map(() => '?').join(',');
    const query = `
      WITH scoped_nodes(id) AS (
        SELECT node_id FROM network_nodes WHERE source = ?
        UNION
        SELECT node1 FROM edges WHERE source = ?
        UNION
        SELECT node2 FROM edges WHERE source = ?
      )
      SELECT id, attributes_json
      FROM nodes
      WHERE id IN (${placeholders})
        AND id IN (SELECT id FROM scoped_nodes)
    `;

    const rows = await dbAll(query, [networkName, networkName, networkName, ...accessions]);

    const matches = rows.map(row => {
      let nhId = null;
      try {
        const attrs = JSON.parse(row.attributes_json || '{}');
        nhId = attrs.NH_ID;
      } catch (e) { }

      return {
        id: row.id,
        nh_id: nhId
      };
    });

    return { matches };

  } catch (err) {
    logger.error(`Error searching proteins: ${err.message}`);
    throw new Error('Search failed');
  }
}

/**
 * Search for nodes by Species (NCBI_txID)
 * @param {string} networkName - Network source file
 * @param {Array} speciesIds - List of NCBI_txIDs to search for
 * @returns {Promise<Object>} Found nodes with their cluster IDs
 */
async function searchBySpecies(networkName, speciesIds) {
  try {
    if (!speciesIds || speciesIds.length === 0) return { matches: [] };

    const placeholders = speciesIds.map(() => '?').join(',');
    const query = `
      WITH scoped_nodes(id) AS (
        SELECT node_id FROM network_nodes WHERE source = ?
        UNION
        SELECT node1 FROM edges WHERE source = ?
        UNION
        SELECT node2 FROM edges WHERE source = ?
      )
      SELECT
        n.id,
        json_extract(n.attributes_json, '$.NH_ID') AS nh_id
      FROM nodes n
      INNER JOIN scoped_nodes nn ON nn.id = n.id
      WHERE CAST(json_extract(n.attributes_json, '$.NCBI_txID') AS TEXT) IN (${placeholders})
    `;

    const rows = await dbAll(query, [networkName, networkName, networkName, ...speciesIds.map(String)]);
    const matches = rows.map(row => ({
      id: row.id,
      nh_id: row.nh_id
    }));

    return { matches };

  } catch (err) {
    logger.error(`Error searching by species: ${err.message}`);
    throw new Error('Species search failed');
  }
}

module.exports = {
  listNetworks,
  getNetworkData,
  searchProteins,
  searchBySpecies,
  createEditedNetwork: networkEditService.createEditedNetwork
};
