const fs = require('fs');
const path = require('path');
const config = require('../config/config');

const MAX_GROUP_NAME_LENGTH = 16;
const MAX_GROUP_COUNT = 100;
const GROUP_NAME_PATTERN = /^[A-Za-z0-9_.-]+$/;

class GroupExportError extends Error {
  constructor(message, status = 400, details = {}) {
    super(message);
    this.name = 'GroupExportError';
    this.status = status;
    this.details = details;
  }
}

function parseUniProtTokens(value) {
  return String(value || '')
    .split(/[\s,;]+/)
    .map(token => token.replace(/[^A-Za-z0-9]/g, '').trim())
    .filter(Boolean);
}

function normalizeGroups(groups) {
  if (!Array.isArray(groups)) {
    throw new GroupExportError('groups must be an array');
  }

  if (groups.length === 0) {
    throw new GroupExportError('At least one group is required');
  }

  if (groups.length > MAX_GROUP_COUNT) {
    throw new GroupExportError(`groups is limited to ${MAX_GROUP_COUNT} items`);
  }

  const seenNames = new Set();

  return groups.map((group, index) => {
    if (!group || typeof group !== 'object' || Array.isArray(group)) {
      throw new GroupExportError(`groups[${index}] must be an object`);
    }

    const rawName = typeof group.name === 'string' ? group.name.trim() : '';
    if (!rawName) {
      throw new GroupExportError(`Group ${index + 1} name is required`);
    }

    if (rawName.length > MAX_GROUP_NAME_LENGTH) {
      throw new GroupExportError(`Group "${rawName}" exceeds ${MAX_GROUP_NAME_LENGTH} characters`);
    }

    if (!GROUP_NAME_PATTERN.test(rawName)) {
      throw new GroupExportError(
        `Group "${rawName}" may contain only letters, numbers, "_", "-", and "."`
      );
    }

    const canonicalName = rawName.toLowerCase();
    if (seenNames.has(canonicalName)) {
      throw new GroupExportError(`Group name "${rawName}" must be unique`);
    }
    seenNames.add(canonicalName);

    const accessions = parseUniProtTokens(group.accessions);
    if (accessions.length === 0) {
      throw new GroupExportError(`Group "${rawName}" must contain at least one UniProt accession`);
    }

    return {
      name: rawName,
      accessions,
    };
  });
}

async function saveGroupExports(groups) {
  const normalizedGroups = normalizeGroups(groups);
  await fs.promises.mkdir(config.exportsPath, { recursive: true });

  const savedFiles = [];
  for (const group of normalizedGroups) {
    const filename = `${group.name}.txt`;
    const outputPath = path.join(config.exportsPath, filename);
    const content = `${group.accessions.join('\n')}\n`;
    await fs.promises.writeFile(outputPath, content, 'utf8');
    savedFiles.push({
      name: group.name,
      filename,
      accessionCount: group.accessions.length,
    });
  }

  return {
    savedFiles,
    exportDir: config.exportsPath,
  };
}

module.exports = {
  GroupExportError,
  MAX_GROUP_NAME_LENGTH,
  parseUniProtTokens,
  normalizeGroups,
  saveGroupExports,
};
