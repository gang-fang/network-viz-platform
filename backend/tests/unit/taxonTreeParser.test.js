const fs = require('fs');
const os = require('os');
const path = require('path');

jest.mock('../../utils/logger', () => ({
  info: jest.fn(),
  warn: jest.fn(),
  error: jest.fn(),
  debug: jest.fn(),
}));

const logger = require('../../utils/logger');
const { parseTaxonTree } = require('../../utils/taxon-tree-parser');

function writeTempFile(dir, name, contents) {
  const filePath = path.join(dir, name);
  fs.writeFileSync(filePath, contents, 'utf8');
  return filePath;
}

describe('taxon-tree-parser', () => {
  let tempDir;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'taxon-tree-parser-'));
    jest.clearAllMocks();
  });

  afterEach(() => {
    fs.rmSync(tempDir, { recursive: true, force: true });
  });

  test('parses a standard multi-level tree across supported branch indicators', async () => {
    const treePath = writeTempFile(tempDir, 'commontree.txt', [
      'root',
      '+-Alpha',
      '| +-Beta',
      '| | ++Gamma',
      '| \\-Delta',
      '  \\+Epsilon',
      '---',
      '',
    ].join('\n'));
    const csvPath = writeTempFile(tempDir, 'NCBI_txID.csv', [
      'ncbi_txid,species_name',
      '1,Alpha',
      '2,Gamma',
      '3,Delta',
      '4,Epsilon',
    ].join('\n'));

    const { root, stats } = await parseTaxonTree(treePath, csvPath, new Set(['2', '4']));

    expect(root.name).toBe('root');
    expect(root.children.map(node => node.name)).toEqual(['Alpha']);
    expect(root.children[0].children.map(node => node.name)).toEqual(['Beta', 'Delta', 'Epsilon']);
    expect(root.children[0].children[0].children.map(node => node.name)).toEqual(['Gamma']);
    expect(root.children[0].taxid).toBe('1');
    expect(root.children[0].children[0].children[0].isDbSpecies).toBe(true);
    expect(root.children[0].children[2].isDbSpecies).toBe(true);
    expect(stats).toMatchObject({ totalNodes: 6, annotatedNodes: 4, dbSpeciesNodes: 2 });
  });

  test('warns and skips orphan nodes that have no parent at the previous depth', async () => {
    const treePath = writeTempFile(tempDir, 'commontree.txt', [
      'root',
      '| | ++Orphan',
    ].join('\n'));
    const csvPath = writeTempFile(tempDir, 'NCBI_txID.csv', [
      'ncbi_txid,species_name',
      '1,Orphan',
    ].join('\n'));

    const { root } = await parseTaxonTree(treePath, csvPath, new Set());

    expect(root.name).toBe('root');
    expect(root.children).toEqual([]);
    expect(logger.warn).toHaveBeenCalledWith(expect.stringContaining('orphan node "Orphan"'));
  });

  test('warns when duplicate lowercase names collide in the name map', async () => {
    const treePath = writeTempFile(tempDir, 'commontree.txt', [
      'root',
      '+-Alpha',
    ].join('\n'));
    const csvPath = writeTempFile(tempDir, 'NCBI_txID.csv', [
      'ncbi_txid,species_name',
      '1,Alpha',
      '2,alpha',
    ].join('\n'));

    const { root } = await parseTaxonTree(treePath, csvPath, new Set(['2']));

    expect(root.children[0].taxid).toBe('2');
    expect(root.children[0].isDbSpecies).toBe(true);
    expect(logger.warn).toHaveBeenCalledWith(expect.stringContaining('duplicate lowercase species name "alpha"'));
  });
});
