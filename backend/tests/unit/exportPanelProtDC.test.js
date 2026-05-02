const ExportPanelModule = require('../../../frontend/js/modules/export-panel');

function makeGraph(nodes, edges) {
  return {
    nodes: new Map(nodes.map(node => [node.id, node])),
    edges: new Map(edges.map(edge => [edge.id, edge])),
  };
}

function makeModule(graph, hiddenProteinIds = [], hiddenEdgeIds = []) {
  return {
    ...ExportPanelModule,
    context: {
      getGraph: () => graph,
      getGraphRevision: () => 1,
      getHiddenProteinIds: () => hiddenProteinIds,
      getHiddenEdgeIds: () => hiddenEdgeIds,
    },
  };
}

function makeInput(value, selectionStart, selectionEnd = selectionStart) {
  return {
    value,
    selectionStart,
    selectionEnd,
    setSelectionRange(start, end) {
      this.selectionStart = start;
      this.selectionEnd = end;
    },
  };
}

describe('ExportPanelModule ProtDC calculation', () => {
  beforeEach(() => {
    ExportPanelModule.protDcCache = null;
  });

  test('sums same-neighborhood SJI weights and normalizes by NH_Size', () => {
    const graph = makeGraph(
      [
        { id: 'P1', NH_ID: 'NH1', NH_Size: 3 },
        { id: 'P2', NH_ID: 'NH1', NH_Size: 3 },
        { id: 'P3', NH_ID: 'NH1', NH_Size: 3 },
        { id: 'P4', NH_ID: 'NH2', NH_Size: 2 },
      ],
      [
        { id: 'P1|P2', source: 'P1', target: 'P2', weight: 0.6 },
        { id: 'P1|P3', source: 'P1', target: 'P3', weight: 0.3 },
        { id: 'P1|P4', source: 'P1', target: 'P4', weight: 0.9 },
      ]
    );

    const protDc = makeModule(graph).calculateProtDCMap();

    expect(protDc.get('P1')).toBeCloseTo(0.3);
    expect(protDc.get('P2')).toBeCloseTo(0.2);
    expect(protDc.get('P3')).toBeCloseTo(0.1);
    expect(protDc.get('P4')).toBe(0);
  });

  test('reports zero for missing attributes, single-member neighborhoods, and hidden edges', () => {
    const graph = makeGraph(
      [
        { id: 'P1', NH_ID: 'NH1', NH_Size: 2 },
        { id: 'P2', NH_ID: 'NH1', NH_Size: 2 },
        { id: 'P3', NH_ID: 'NH2', NH_Size: 1 },
        { id: 'P4' },
      ],
      [
        { id: 'P1|P2', source: 'P1', target: 'P2', weight: 0.8 },
        { id: 'P3|P4', source: 'P3', target: 'P4', weight: 0.5 },
      ]
    );

    const protDc = makeModule(graph, [], ['P1|P2']).calculateProtDCMap();

    expect(protDc.get('P1')).toBe(0);
    expect(protDc.get('P2')).toBe(0);
    expect(protDc.get('P3')).toBe(0);
    expect(protDc.get('P4')).toBe(0);
  });

  test('formats very small values to two decimals', () => {
    const module = makeModule(makeGraph([], []));

    expect(module.formatProtDC(0.001)).toBe('0.00');
    expect(module.formatProtDC(1 / 3)).toBe('0.33');
  });

  test('memoizes ProtDC by graph revision', () => {
    const graph = makeGraph(
      [
        { id: 'P1', NH_ID: 'NH1', NH_Size: 2 },
        { id: 'P2', NH_ID: 'NH1', NH_Size: 2 },
      ],
      [
        { id: 'P1|P2', source: 'P1', target: 'P2', weight: 0.8 },
      ]
    );
    const module = makeModule(graph);

    const first = module.calculateProtDCMap();
    const second = module.calculateProtDCMap();

    expect(first).toBe(second);
  });

  test('preserves caret position when normalizing batch-analysis input', () => {
    const module = makeModule(makeGraph([], []));
    const input = makeInput('Alpha Beta.Gamma', 10);

    module.applyNormalizedInputValue(input, value => value.replace(/\s+/g, ''));

    expect(input.value).toBe('AlphaBeta.Gamma');
    expect(input.selectionStart).toBe(9);
    expect(input.selectionEnd).toBe(9);
  });

  test('keeps selection unchanged when normalization does not alter the value', () => {
    const module = makeModule(makeGraph([], []));
    const input = makeInput('A0A0H0PPY1', 4, 8);

    module.applyNormalizedInputValue(input, value => value.replace(/\s+/g, ''));

    expect(input.value).toBe('A0A0H0PPY1');
    expect(input.selectionStart).toBe(4);
    expect(input.selectionEnd).toBe(8);
  });

  test('preserves range selection when normalization removes characters before and within the range', () => {
    const module = makeModule(makeGraph([], []));
    const input = makeInput('AA  BB  CC', 2, 8);

    module.applyNormalizedInputValue(input, value => value.replace(/\s+/g, ''));

    expect(input.value).toBe('AABBCC');
    expect(input.selectionStart).toBe(2);
    expect(input.selectionEnd).toBe(4);
  });
});
