const Graph = require('../../../frontend/js/core/graph');

global.Graph = Graph;
const AppState = require('../../../frontend/js/core/state');

function createState() {
  const state = new AppState();
  state.setGraphData(
    [{ id: 'P1' }, { id: 'P2' }, { id: 'P3' }, { id: 'P4' }],
    [
      { source: 'P1', target: 'P2', weight: 0.2 },
      { source: 'P2', target: 'P3', weight: 0.4 },
      { source: 'P3', target: 'P4', weight: 0.8 },
    ],
    'source.csv'
  );
  return state;
}

describe('AppState edge edit bookkeeping', () => {
  test('bulk expand and collapse request fresh layout, with collapse fitting the viewport', () => {
    const state = new AppState();
    const graphUpdatedDetails = [];

    state.on('graphUpdated', detail => graphUpdatedDetails.push(detail));
    state.setGraphData(
      [
        { id: 'P1', NH_ID: 'NH1' },
        { id: 'P2', NH_ID: 'NH1' },
        { id: 'P3', NH_ID: 'NH2' },
      ],
      [
        { source: 'P1', target: 'P3', weight: 0.5 },
      ],
      'source.csv'
    );

    state.expandAll();
    state.collapseAll();

    expect(graphUpdatedDetails[0]).toMatchObject({ layoutReset: true, fitView: true, resetViewport: true, reason: 'load', preservePins: false });
    expect(graphUpdatedDetails[1]).toMatchObject({ layoutReset: true, fitView: true, preservePins: false, resetViewport: true, reason: 'expandAll' });
    expect(graphUpdatedDetails[2]).toMatchObject({ layoutReset: true, fitView: true, preservePins: false, resetViewport: true, reason: 'collapseAll' });
  });

  test('validates SJI threshold boundaries', () => {
    const state = createState();

    expect(() => state.getEdgeIdsBelowWeight(0)).toThrow('greater than 0 and less than 1');
    expect(() => state.getEdgeIdsBelowWeight(1)).toThrow('greater than 0 and less than 1');
    expect(() => state.getEdgeIdsBelowWeight(-0.1)).toThrow('greater than 0 and less than 1');
    expect(() => state.getEdgeIdsBelowWeight(Number.NaN)).toThrow('greater than 0 and less than 1');
    expect(state.getEdgeIdsBelowWeight('0.5')).toEqual(['P1|P2', 'P2|P3']);
  });

  test('canonicalizes direct edge IDs and returns booleans', () => {
    const state = createState();

    expect(state.hideEdgeIds(['P2|P1'])).toBe(true);
    expect(state.hideEdgeIds(['P1|P2'])).toBe(false);
    expect(state.getHiddenEdgeIds()).toEqual(['P1|P2']);
    expect(state.showEdgeIds(['P2|P1'])).toBe(true);
    expect(state.showEdgeIds(['P2|P1'])).toBe(false);
  });

  test('merges hidden edge weight ranges', () => {
    const state = createState();

    state.addHiddenEdgeWeightRange(0.4, 0.6);
    state.addHiddenEdgeWeightRange(0.1, 0.2);
    state.addHiddenEdgeWeightRange(0.2, 0.3);
    state.addHiddenEdgeWeightRange(0.45, 0.55);

    expect(state.getHiddenEdgeWeightRanges()).toEqual([
      { min: 0.1, max: 0.3 },
      { min: 0.4, max: 0.6 },
    ]);
  });

  test('splits and trims hidden edge weight ranges when removing a sub-range', () => {
    const state = createState();

    state.addHiddenEdgeWeightRange(0, 0.8);
    state.removeHiddenEdgeWeightRange(0.2, 0.5);
    expect(state.getHiddenEdgeWeightRanges()).toEqual([
      { min: 0, max: 0.2 },
      { min: 0.5, max: 0.8 },
    ]);

    state.removeHiddenEdgeWeightRange(0.4, 0.7);
    expect(state.getHiddenEdgeWeightRanges()).toEqual([
      { min: 0, max: 0.2 },
      { min: 0.7, max: 0.8 },
    ]);
  });

  test('compacts hidden edge payloads into ranges plus extra explicit edges', () => {
    const state = createState();

    expect(state.getHiddenEdgeEditPayload()).toEqual({
      hiddenEdgeIds: [],
      hiddenEdgeWeightRanges: [],
    });

    state.hideEdgesByWeightBelow(0.5);
    expect(state.getHiddenEdgeEditPayload()).toEqual({
      hiddenEdgeIds: [],
      hiddenEdgeWeightRanges: [{ min: 0, max: 0.5 }],
    });

    state.showEdgeIds(['P1|P2']);
    expect(state.getHiddenEdgeEditPayload()).toEqual({
      hiddenEdgeIds: ['P2|P3'],
      hiddenEdgeWeightRanges: [],
    });

    state.hideEdgesByWeightBelow(0.5);
    state.hideEdgeIds(['P3|P4']);
    expect(state.getHiddenEdgeEditPayload()).toEqual({
      hiddenEdgeIds: ['P3|P4'],
      hiddenEdgeWeightRanges: [{ min: 0, max: 0.5 }],
    });
  });

  test('range restore above threshold keeps remaining ranges consistent', () => {
    const state = createState();

    state.hideEdgesByWeightBelow(0.5);
    state.showEdgesByWeightAbove(0.3);

    expect(state.getHiddenEdgeEditPayload()).toEqual({
      hiddenEdgeIds: [],
      hiddenEdgeWeightRanges: [{ min: 0, max: 0.3 }],
    });
  });
});
