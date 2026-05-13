const fs = require('fs');

jest.mock('../../config/config', () => ({
  exportsPath: require('path').join(require('os').tmpdir(), 'nvp-group-exports-test'),
  groupExport: {
    maxSuffixAttempts: 1000,
  },
}));

const path = require('path');

const {
  normalizeGroups,
  parseUniProtTokens,
  saveGroupExports,
} = require('../../services/groupExportService');

describe('groupExportService', () => {
  afterEach(async () => {
    const config = require('../../config/config');
    config.groupExport.maxSuffixAttempts = 1000;
    await fs.promises.rm(config.exportsPath, { recursive: true, force: true });
  });

  test('parseUniProtTokens strips punctuation and keeps one token per accession', () => {
    expect(parseUniProtTokens('P12345,\nQ67890; A0A1-234.')).toEqual([
      'P12345',
      'Q67890',
      'A0A1234',
    ]);
  });

  test('normalizeGroups enforces unique valid group names', () => {
    expect(() => normalizeGroups([
      { name: 'group1', accessions: 'P12345' },
      { name: 'Group1', accessions: 'Q67890' },
    ])).toThrow('Group name "Group1" must be unique');
  });

  test('normalizeGroups rejects names longer than 16 characters', () => {
    expect(() => normalizeGroups([
      { name: 'abcdefghijklmnopq', accessions: 'P12345' },
    ])).toThrow('Group "abcdefghijklmnopq" exceeds 16 characters');
  });

  test('normalizeGroups rejects invalid group-name characters', () => {
    expect(() => normalizeGroups([
      { name: 'my group', accessions: 'P12345' },
    ])).toThrow('Group "my group" may contain only letters, numbers, "_", "-", and "."');
  });

  test('normalizeGroups rejects an empty groups array', () => {
    expect(() => normalizeGroups([])).toThrow('At least one group is required');
  });

  test('normalizeGroups rejects groups without any parsed accessions', () => {
    expect(() => normalizeGroups([
      { name: 'group1', accessions: ' , ; \n ' },
    ])).toThrow('Group "group1" must contain at least one UniProt accession');
  });

  test('saveGroupExports writes one accession per line', async () => {
    const result = await saveGroupExports([
      { name: 'group1', accessions: 'P12345, Q67890\nA0A123' },
    ]);

    const outputPath = path.join(result.exportDir, 'group1.txt');
    const content = await fs.promises.readFile(outputPath, 'utf8');

    expect(result.savedFiles).toEqual([
      { name: 'group1', filename: 'group1.txt', accessionCount: 3 },
    ]);
    expect(content).toBe('P12345\nQ67890\nA0A123\n');
  });

  test('saveGroupExports appends suffixes for existing export names', async () => {
    const config = require('../../config/config');
    await fs.promises.mkdir(config.exportsPath, { recursive: true });
    await fs.promises.writeFile(path.join(config.exportsPath, 'group1.txt'), 'old\n', 'utf8');
    await fs.promises.writeFile(path.join(config.exportsPath, 'group1_1.txt'), 'old\n', 'utf8');

    const result = await saveGroupExports([
      { name: 'group1', accessions: 'P12345 Q67890' },
    ]);

    const outputPath = path.join(result.exportDir, 'group1_2.txt');
    const content = await fs.promises.readFile(outputPath, 'utf8');

    expect(result.savedFiles).toEqual([
      { name: 'group1', filename: 'group1_2.txt', accessionCount: 2 },
    ]);
    expect(await fs.promises.readFile(path.join(config.exportsPath, 'group1.txt'), 'utf8')).toBe('old\n');
    expect(await fs.promises.readFile(path.join(config.exportsPath, 'group1_1.txt'), 'utf8')).toBe('old\n');
    expect(content).toBe('P12345\nQ67890\n');
  });

  test('saveGroupExports treats export filename collisions case-insensitively', async () => {
    const config = require('../../config/config');
    await fs.promises.mkdir(config.exportsPath, { recursive: true });
    await fs.promises.writeFile(path.join(config.exportsPath, 'Group1.txt'), 'old\n', 'utf8');

    const result = await saveGroupExports([
      { name: 'group1', accessions: 'P12345' },
    ]);

    expect(result.savedFiles).toEqual([
      { name: 'group1', filename: 'group1_1.txt', accessionCount: 1 },
    ]);
    expect(await fs.promises.readFile(path.join(config.exportsPath, 'Group1.txt'), 'utf8')).toBe('old\n');
    expect(await fs.promises.readFile(path.join(config.exportsPath, 'group1_1.txt'), 'utf8')).toBe('P12345\n');
  });

  test('saveGroupExports returns 503 when no suffix candidate is available', async () => {
    const config = require('../../config/config');
    config.groupExport.maxSuffixAttempts = 2;
    await fs.promises.mkdir(config.exportsPath, { recursive: true });
    await fs.promises.writeFile(path.join(config.exportsPath, 'group1.txt'), 'old\n', 'utf8');
    await fs.promises.writeFile(path.join(config.exportsPath, 'group1_1.txt'), 'old\n', 'utf8');

    await expect(saveGroupExports([
      { name: 'group1', accessions: 'P12345' },
    ])).rejects.toMatchObject({
      name: 'GroupExportError',
      status: 503,
    });
  });
});
