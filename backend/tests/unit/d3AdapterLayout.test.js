const D3Adapter = require('../../../frontend/js/adapters/d3-adapter');

function createAdapter({ width = 1000, height = 500, topologyRevision = 1 } = {}) {
  const adapter = Object.create(D3Adapter.prototype);
  adapter.width = width;
  adapter.height = height;
  adapter.appState = { topologyRevision };
  adapter.layoutRunId = 0;
  adapter.pinnedNodePositions = new Map();
  adapter.componentLayoutCache = { revision: null, width: null, height: null, centers: new Map() };
  adapter.componentMeasurementCache = { revision: null, radiusBySignature: new Map() };
  return adapter;
}

describe('D3Adapter layout helpers', () => {
  test('computeComponents returns connected components without Array.shift', () => {
    const adapter = createAdapter();
    const nodes = [{ id: 'A' }, { id: 'B' }, { id: 'C' }, { id: 'D' }];
    const edges = [
      { id: 'A|B', source: 'A', target: 'B' },
      { id: 'B|C', source: 'B', target: 'C' },
    ];
    const shiftSpy = jest.spyOn(Array.prototype, 'shift');

    const components = adapter.computeComponents(nodes, edges);
    const shiftCallCount = shiftSpy.mock.calls.length;
    shiftSpy.mockRestore();

    expect(components.map(component => component.map(node => node.id))).toEqual([
      ['A', 'B', 'C'],
      ['D'],
    ]);
    expect(shiftCallCount).toBe(0);
  });

  test('getCollisionRadius honours the legacy floor for small nodes and grows for oversized clusters', () => {
    const adapter = createAdapter();

    // Proteins (radius 5) and the smallest clusters (radius 7) must hit the
    // legacy floor of 15 so cluster-only "collapse all" layouts stay tight
    // enough to fit inside the viewport at the default zoom.
    expect(adapter.getCollisionRadius({})).toBe(15);
    expect(adapter.getCollisionRadius({ _isCluster: true, size: 1 })).toBe(15);

    // Larger clusters grow past the floor, but only proportional to their
    // own geometric radius — no flat 10-px inflation for every node.
    const big = adapter.getCollisionRadius({ _isCluster: true, size: 1000 });
    expect(big).toBeGreaterThan(15);
    // 1000 → getNodeRadius = 7 + ln(1000)*3 ≈ 27.7; with padding 4, ≈ 31.7.
    // The old size-aware formula was getNodeRadius + 10 ≈ 37.7. The new
    // value must stay materially below that to recover the compact layout.
    expect(big).toBeLessThan(34);
  });

  test('getStaticTickCount scales with node count and respects caps', () => {
    const adapter = createAdapter();

    expect(adapter.getStaticTickCount(1, true)).toBeLessThan(adapter.getStaticTickCount(100, true));
    expect(adapter.getStaticTickCount(10000, true)).toBe(260);
    expect(adapter.getStaticTickCount(10000, false)).toBe(120);
  });

  test('assignComponentTargets reuses cached centers for the same topology revision and viewport', () => {
    const adapter = createAdapter({ width: 1000, height: 500, topologyRevision: 7 });
    const nodes = [{ id: 'A' }, { id: 'B' }];
    const edges = [];

    adapter.assignComponentTargets(nodes, edges, true, 7);
    const cachedCenters = nodes.map(node => ({ x: node._componentCenterX, y: node._componentCenterY }));
    expect(adapter.componentLayoutCache.revision).toBe(7);

    const nextNodes = [{ id: 'A' }, { id: 'B' }];
    const computeSpy = jest.spyOn(adapter, 'computeComponents');
    adapter.assignComponentTargets(nextNodes, edges, false, 7);

    expect(computeSpy).not.toHaveBeenCalled();
    expect(nextNodes.map(node => ({ x: node._componentCenterX, y: node._componentCenterY }))).toEqual(cachedCenters);
  });

  test('assignComponentTargets invalidates cached centers when viewport size changes', () => {
    const adapter = createAdapter({ width: 1000, height: 500, topologyRevision: 7 });
    const nodes = [{ id: 'A' }, { id: 'B' }];
    const edges = [];

    adapter.assignComponentTargets(nodes, edges, true, 7);
    const originalCenterX = nodes[0]._componentCenterX;

    adapter.width = 2000;
    const nextNodes = [{ id: 'A' }, { id: 'B' }];
    adapter.assignComponentTargets(nextNodes, edges, false, 7);

    expect(nextNodes[0]._componentCenterX).not.toBe(originalCenterX);
  });

  test('assignComponentTargets keeps disconnected components inside a more compact central grid', () => {
    const adapter = createAdapter({ width: 1000, height: 500, topologyRevision: 7 });
    const nodes = [{ id: 'A' }, { id: 'B' }, { id: 'C' }, { id: 'D' }];
    const edges = [];

    adapter.assignComponentTargets(nodes, edges, true, 7);

    const xs = nodes.map(node => node._componentCenterX);
    const ys = nodes.map(node => node._componentCenterY);
    expect(Math.min(...xs)).toBeGreaterThan(100);
    expect(Math.max(...xs)).toBeLessThan(900);
    expect(Math.min(...ys)).toBeGreaterThan(50);
    expect(Math.max(...ys)).toBeLessThan(450);
  });

  test('estimateComponentPlacementRadius grows with component size', () => {
    const adapter = createAdapter();

    expect(adapter.estimateComponentPlacementRadius([{ id: 'A' }])).toBe(20);
    expect(adapter.estimateComponentPlacementRadius(new Array(4).fill(null))).toBeGreaterThan(50);
    expect(adapter.estimateComponentPlacementRadius(new Array(100).fill(null))).toBeGreaterThan(
      adapter.estimateComponentPlacementRadius(new Array(25).fill(null))
    );
  });

  test('estimateComponentPlacementRadius grows sublinearly for very large components', () => {
    const adapter = createAdapter();

    const medium = adapter.estimateComponentPlacementRadius(new Array(100).fill(null));
    const large = adapter.estimateComponentPlacementRadius(new Array(500).fill(null));

    expect(medium).toBeLessThan(170);
    expect(large).toBeLessThan(320);
    expect(large).toBeGreaterThan(medium);
  });

  test('estimateComponentPlacementRadius floors its large-component reduction term', () => {
    const adapter = createAdapter();

    expect(adapter.estimateComponentPlacementRadius(new Array(10000).fill(null))).toBe(824);
  });

  test('buildComponentEdgeMap groups edges by disconnected component', () => {
    const adapter = createAdapter();
    const components = [
      [{ id: 'A' }, { id: 'B' }],
      [{ id: 'C' }],
    ];
    const edges = [
      { id: 'A|B', source: 'A', target: 'B' },
      { id: 'A|C', source: 'A', target: 'C' },
    ];

    const grouped = adapter.buildComponentEdgeMap(components, edges);

    expect(grouped).toHaveLength(2);
    expect(grouped[0]).toEqual([{ id: 'A|B', source: 'A', target: 'B' }]);
    expect(grouped[1]).toEqual([]);
  });

  test('measureComponentPlacementRadius caches fallback results by topology revision', () => {
    const adapter = createAdapter({ topologyRevision: 7 });
    const component = [{ id: 'A' }, { id: 'B' }];
    const estimateSpy = jest.spyOn(adapter, 'estimateComponentPlacementRadius').mockReturnValue(77);

    const previousD3 = global.d3;
    global.d3 = undefined;

    try {
      expect(adapter.measureComponentPlacementRadius(component, [], 7)).toBe(77);
      expect(adapter.measureComponentPlacementRadius(component, [], 7)).toBe(77);
      expect(estimateSpy).toHaveBeenCalledTimes(1);
    } finally {
      global.d3 = previousD3;
    }
  });

  test('packComponentsByEstimatedSize uses measured radii when available', () => {
    const adapter = createAdapter({ width: 1000, height: 500 });
    adapter.getLayoutViewportBounds = () => ({ left: 40, top: 40, width: 860, height: 420 });
    const measureSpy = jest.spyOn(adapter, 'measureComponentPlacementRadius')
      .mockReturnValueOnce(200)
      .mockReturnValueOnce(20);

    const components = [
      new Array(50).fill(null).map((_, index) => ({ id: `L${index}` })),
      [{ id: 'S1' }],
    ];
    const componentEdges = [[], []];

    const packed = adapter.packComponentsByEstimatedSize(components, adapter.getLayoutViewportBounds(), componentEdges, 1);

    expect(measureSpy).toHaveBeenNthCalledWith(1, components[0], componentEdges[0], 1);
    expect(measureSpy).toHaveBeenNthCalledWith(2, components[1], componentEdges[1], 1);
    expect(packed[0].r).toBeGreaterThan(packed[1].r);
  });

  test('packComponentsByEstimatedSize keeps small components closer than a uniform grid would', () => {
    const adapter = createAdapter({ width: 1000, height: 500 });
    adapter.getLayoutViewportBounds = () => ({ left: 40, top: 40, width: 860, height: 420 });
    const components = [
      new Array(100).fill(null).map((_, index) => ({ id: `L${index}` })),
      [{ id: 'S1' }],
      [{ id: 'S2' }],
      [{ id: 'S3' }],
      [{ id: 'S4' }],
      [{ id: 'S5' }],
    ];
    const componentEdges = components.map(() => []);

    const packed = adapter.packComponentsByEstimatedSize(components, adapter.getLayoutViewportBounds(), componentEdges, 1);
    const large = packed[0];
    const smalls = packed.slice(1);
    const maxSmallDistanceFromLarge = Math.max(
      ...smalls.map(circle => Math.hypot(circle.x - large.x, circle.y - large.y))
    );

    expect(maxSmallDistanceFromLarge).toBeLessThan(260);
    packed.forEach(circle => {
      expect(circle.x - circle.r).toBeGreaterThanOrEqual(40);
      expect(circle.x + circle.r).toBeLessThanOrEqual(900);
      expect(circle.y - circle.r).toBeGreaterThanOrEqual(40);
      expect(circle.y + circle.r).toBeLessThanOrEqual(460);
    });
  });

  test('getLayoutViewportBounds reserves space for the right-side overlay controls', () => {
    const adapter = createAdapter({ width: 1000, height: 500 });
    adapter.container = {
      getBoundingClientRect: () => ({ left: 0, top: 0, right: 1000, bottom: 500 }),
    };
    const previousDocument = global.document;
    global.document = {
      getElementById: (id) => {
        if (id === 'zoom-controls') {
          return { getBoundingClientRect: () => ({ left: 940, right: 980, top: 300, bottom: 460 }) };
        }
        if (id === 'export-controls') {
          return { getBoundingClientRect: () => ({ left: 920, right: 980, top: 20, bottom: 120 }) };
        }
        return null;
      },
    };

    try {
      const bounds = adapter.getLayoutViewportBounds();
      expect(bounds.left).toBeGreaterThanOrEqual(36);
      expect(bounds.top).toBeGreaterThanOrEqual(36);
      expect(bounds.right).toBeLessThan(920);
      expect(bounds.width).toBeLessThan(900);
    } finally {
      global.document = previousDocument;
    }
  });

  test('buildExpandedProteinMembership + updateExpandedGroupBounds estimate expanded protein cloud bounds', () => {
    const adapter = createAdapter();
    const nodes = [
      { id: 'P1', NH_ID: 'NH1', x: 0, y: 0 },
      { id: 'P2', NH_ID: 'NH1', x: 20, y: 0 },
      { id: 'P3', NH_ID: 'NH1', x: 0, y: 20 },
      { id: 'P4', NH_ID: 'NH1', x: 20, y: 20 },
      { id: 'NH2', _isCluster: true, x: 10, y: 10, size: 5 },
      { id: 'Lone', x: 100, y: 100 },
    ];

    const { groups, clusterNodes } = adapter.buildExpandedProteinMembership(nodes);
    expect(groups).toHaveLength(1);
    expect(groups[0].id).toBe('NH1');
    expect(groups[0].members).toHaveLength(4);
    expect(clusterNodes.map(n => n.id)).toEqual(['NH2']);

    adapter.updateExpandedGroupBounds(groups);
    expect(groups[0].centerX).toBe(10);
    expect(groups[0].centerY).toBe(10);
    expect(groups[0].radius).toBeGreaterThan(40);
  });

  test('buildExpandedProteinMembership skips proteins whose cluster is not expanded', () => {
    const adapter = createAdapter();
    adapter.appState = {
      topologyRevision: 1,
      expandedClusters: new Set(['NH1']),
    };
    const nodes = [
      { id: 'P1', NH_ID: 'NH1', x: 0, y: 0 },
      { id: 'P2', NH_ID: 'NH1', x: 1, y: 0 },
      { id: 'P3', NH_ID: 'NH2', x: 100, y: 100 },
      { id: 'P4', NH_ID: 'NH2', x: 101, y: 100 },
    ];

    const { groups } = adapter.buildExpandedProteinMembership(nodes);
    expect(groups).toHaveLength(1);
    expect(groups[0].id).toBe('NH1');
  });

  test('applyExpandedGroupClusterPush pushes collapsed clusters outside expanded protein clouds', () => {
    const adapter = createAdapter();
    const groups = [{ id: 'NH1', centerX: 10, centerY: 10, radius: 45, members: [] }];
    const clusterNodes = [
      { id: 'NH2', _isCluster: true, x: 12, y: 10, vx: 0, vy: 0, size: 2 },
      { id: 'NH3', _isCluster: true, x: 200, y: 200, vx: 0, vy: 0, size: 2 },
    ];

    adapter.applyExpandedGroupClusterPush(clusterNodes, groups, 1);

    expect(clusterNodes[0].vx).toBeGreaterThan(0);
    expect(clusterNodes[0].vy).toBe(0);
    expect(clusterNodes[1]).toMatchObject({ vx: 0, vy: 0 });
  });

  test('applyExpandedGroupPairPush pushes overlapping expanded groups apart through their members', () => {
    const adapter = createAdapter();
    const memberA = { id: 'P1', vx: 0, vy: 0 };
    const memberB = { id: 'P2', vx: 0, vy: 0 };
    const groups = [
      { id: 'NH1', centerX: 0, centerY: 0, radius: 40, members: [memberA] },
      { id: 'NH2', centerX: 30, centerY: 0, radius: 40, members: [memberB] },
    ];

    adapter.applyExpandedGroupPairPush(groups, 1);

    expect(memberA.vx).toBeLessThan(0);
    expect(memberB.vx).toBeGreaterThan(0);
    expect(memberA.vy).toBe(0);
    expect(memberB.vy).toBe(0);
  });

  test('updateExpandedGroupBounds disables groups whose members are spread across the viewport', () => {
    const adapter = createAdapter({ width: 1000, height: 600 });
    adapter.getNodeRadius = () => 5;
    // Members on opposite sides of the viewport — typical when intra-cluster
    // edges have been filtered out and the proteins drift apart.
    const fragmentedGroup = {
      id: 'NH-frag',
      members: [
        { id: 'P1', x: -400, y: 0 },
        { id: 'P2', x: 400, y: 0 },
      ],
      centerX: 0, centerY: 0, radius: 0, active: true,
    };
    const tightGroup = {
      id: 'NH-tight',
      members: [
        { id: 'P3', x: 0, y: 0 },
        { id: 'P4', x: 10, y: 5 },
      ],
      centerX: 0, centerY: 0, radius: 0, active: true,
    };

    adapter.updateExpandedGroupBounds([fragmentedGroup, tightGroup]);

    expect(fragmentedGroup.active).toBe(false);
    expect(tightGroup.active).toBe(true);
  });

  test('updateExpandedGroupBounds keeps large dense groups active so collapsed neighbors can be pushed out', () => {
    const adapter = createAdapter({ width: 1000, height: 600 });
    adapter.getNodeRadius = () => 5;
    const denseLargeGroup = {
      id: 'NH-dense',
      members: Array.from({ length: 120 }, (_, index) => ({
        id: `P${index}`,
        x: Math.cos(index * 0.4) * (80 + (index % 5) * 6),
        y: Math.sin(index * 0.4) * (80 + (index % 5) * 6),
      })),
      centerX: 0, centerY: 0, radius: 0, active: true,
    };

    adapter.updateExpandedGroupBounds([denseLargeGroup]);

    expect(denseLargeGroup.radius).toBeGreaterThan(100);
    expect(denseLargeGroup.active).toBe(true);
  });

  test('inactive groups are skipped by both cluster and pair push forces', () => {
    const adapter = createAdapter();
    const memberA = { id: 'P1', vx: 0, vy: 0 };
    const memberB = { id: 'P2', vx: 0, vy: 0 };
    const cluster = { id: 'NH-X', _isCluster: true, x: 5, y: 0, vx: 0, vy: 0, size: 2 };
    const groups = [
      { id: 'NH1', centerX: 0, centerY: 0, radius: 400, members: [memberA], active: false },
      { id: 'NH2', centerX: 30, centerY: 0, radius: 400, members: [memberB], active: false },
    ];

    adapter.applyExpandedGroupClusterPush([cluster], groups, 1);
    adapter.applyExpandedGroupPairPush(groups, 1);

    expect(cluster).toMatchObject({ vx: 0, vy: 0 });
    expect(memberA).toMatchObject({ vx: 0, vy: 0 });
    expect(memberB).toMatchObject({ vx: 0, vy: 0 });
  });

  test('applyExpandedGroupPairPush leaves non-overlapping groups untouched', () => {
    const adapter = createAdapter();
    const memberA = { id: 'P1', vx: 0, vy: 0 };
    const memberB = { id: 'P2', vx: 0, vy: 0 };
    const groups = [
      { id: 'NH1', centerX: 0, centerY: 0, radius: 20, members: [memberA] },
      { id: 'NH2', centerX: 500, centerY: 0, radius: 20, members: [memberB] },
    ];

    adapter.applyExpandedGroupPairPush(groups, 1);

    expect(memberA).toMatchObject({ vx: 0, vy: 0 });
    expect(memberB).toMatchObject({ vx: 0, vy: 0 });
  });

  test('expanded group force initialize caches membership and skips per-tick rebuilds', () => {
    const adapter = createAdapter();
    adapter.getNodeRadius = jest.fn(node => (node._isCluster ? 8 : 5));
    const buildSpy = jest.spyOn(adapter, 'buildExpandedProteinMembership');
    const boundsSpy = jest.spyOn(adapter, 'updateExpandedGroupBounds');

    const force = adapter.createExpandedGroupSeparationForce();
    const nodes = [
      { id: 'P1', NH_ID: 'NH1', x: 0, y: 0, vx: 0, vy: 0 },
      { id: 'P2', NH_ID: 'NH1', x: 20, y: 0, vx: 0, vy: 0 },
      { id: 'NH2', _isCluster: true, x: 12, y: 0, vx: 0, vy: 0, size: 2 },
    ];

    force.initialize(nodes);
    expect(buildSpy).toHaveBeenCalledTimes(1);

    force(1);
    force(0.8);
    force(0.5);

    expect(buildSpy).toHaveBeenCalledTimes(1);
    expect(boundsSpy).toHaveBeenCalledTimes(3);
  });

  test('pinned node positions can be restored after reset layout', () => {
    const adapter = createAdapter({ topologyRevision: 7 });
    adapter.simulation = {
      nodes: () => [
        { id: 'A', x: 10, y: 20, fx: 10, fy: 20 },
        { id: 'B', x: 100, y: 200, fx: null, fy: null },
      ],
    };

    const pinnedPositions = adapter.getPinnedNodePositions();
    const nodes = [{ id: 'A' }, { id: 'B' }];
    adapter.assignComponentTargets(nodes, [], true, 7);
    adapter.restorePinnedNodePositions(nodes, pinnedPositions);

    expect(nodes[0]).toMatchObject({ x: 10, y: 20, fx: 10, fy: 20, vx: 0, vy: 0 });
    expect(nodes[1].fx).toBeNull();
    expect(nodes[1].fy).toBeNull();
  });

  test('pins survive while a node is temporarily absent from the visible view', () => {
    const adapter = createAdapter({ topologyRevision: 7 });
    adapter.pinnedNodePositions.set('A', { x: 10, y: 20, fx: 10, fy: 20 });
    adapter.simulation = { nodes: () => [] };

    const pinnedPositions = adapter.getPinnedNodePositions();
    adapter.restorePinnedNodePositions([{ id: 'B' }], pinnedPositions);
    expect(adapter.pinnedNodePositions.get('A')).toEqual({ x: 10, y: 20, fx: 10, fy: 20 });

    const visibleAgain = [{ id: 'A' }];
    adapter.restorePinnedNodePositions(visibleAgain, adapter.getPinnedNodePositions());
    expect(visibleAgain[0]).toMatchObject({ x: 10, y: 20, fx: 10, fy: 20, vx: 0, vy: 0 });
  });

  test('preserveNodePositions seeds expanded cluster members around the previous cluster center', () => {
    const adapter = createAdapter({ topologyRevision: 7 });
    adapter.simulation = {
      nodes: () => [
        { id: 'e.73.0', _isCluster: true, size: 557, x: 100, y: 200, vx: 3, vy: -2 },
        { id: 'e.73.1', _isCluster: true, size: 365, x: 260, y: 200, vx: 0, vy: 0 },
      ],
    };

    const visibleNodes = [
      { id: 'P3', NH_ID: 'e.73.0' },
      { id: 'P1', NH_ID: 'e.73.0' },
      { id: 'P2', NH_ID: 'e.73.0' },
      { id: 'e.73.1', _isCluster: true },
    ];

    adapter.preserveNodePositions(visibleNodes);

    const proteins = visibleNodes.filter(node => node.NH_ID === 'e.73.0');
    proteins.forEach(node => {
      expect(node.vx).toBe(3);
      expect(node.vy).toBe(-2);
      expect(Math.hypot(node.x - 100, node.y - 200)).toBeGreaterThan(20);
    });
    expect(visibleNodes[3]).toMatchObject({ x: 260, y: 200 });
  });

  test('updateVisualization uses explicit preservePins flag instead of reason text', async () => {
    const adapter = createAdapter({ topologyRevision: 7 });
    const previousGraphView = global.GraphView;
    global.GraphView = { compute: jest.fn(() => ({ nodes: [], edges: [] })) };

    try {
      adapter.appState = {
        graph: {},
        expandedClusters: new Set(),
        hiddenNodes: new Set(),
        hiddenEdges: new Set(),
        topologyRevision: 7,
        setViewGraph: jest.fn(),
      };
      adapter.render = jest.fn().mockResolvedValue();

      await adapter.updateVisualization({ layoutReset: true, reason: 'load' });
      expect(adapter.render.mock.calls[0][2]).toMatchObject({ preservePins: true });

      await adapter.updateVisualization({ layoutReset: true, reason: 'load', preservePins: false });
      expect(adapter.render.mock.calls[1][2]).toMatchObject({ preservePins: false });
    } finally {
      global.GraphView = previousGraphView;
    }
  });

  test('updateVisualization only fits the viewport when explicitly requested', async () => {
    const adapter = createAdapter({ topologyRevision: 7 });
    const previousGraphView = global.GraphView;
    global.GraphView = { compute: jest.fn(() => ({ nodes: [], edges: [] })) };

    try {
      adapter.appState = {
        graph: {},
        expandedClusters: new Set(),
        hiddenNodes: new Set(),
        hiddenEdges: new Set(),
        topologyRevision: 7,
        setViewGraph: jest.fn(),
      };
      adapter.render = jest.fn().mockResolvedValue();

      await adapter.updateVisualization({ layoutReset: true, reason: 'expandAll' });
      expect(adapter.render.mock.calls[0][2]).toMatchObject({ fitView: false });

      await adapter.updateVisualization({ layoutReset: true, fitView: true, reason: 'collapseAll' });
      expect(adapter.render.mock.calls[1][2]).toMatchObject({ fitView: true });
    } finally {
      global.GraphView = previousGraphView;
    }
  });

  test('updateVisualization passes explicit viewport reset requests through to render', async () => {
    const adapter = createAdapter({ topologyRevision: 7 });
    const previousGraphView = global.GraphView;
    global.GraphView = { compute: jest.fn(() => ({ nodes: [], edges: [] })) };

    try {
      adapter.appState = {
        graph: {},
        expandedClusters: new Set(),
        hiddenNodes: new Set(),
        hiddenEdges: new Set(),
        topologyRevision: 7,
        setViewGraph: jest.fn(),
      };
      adapter.render = jest.fn().mockResolvedValue();

      await adapter.updateVisualization({ layoutReset: true, fitView: true, resetViewport: true, preservePins: false, reason: 'load' });
      expect(adapter.render.mock.calls[0][2]).toMatchObject({ fitView: true, resetViewport: true, preservePins: false });
    } finally {
      global.GraphView = previousGraphView;
    }
  });
});
