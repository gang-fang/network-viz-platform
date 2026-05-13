const UniprotTooltipModule = require('../../../frontend/js/modules/uniprot-tooltip');

function createFakeElement() {
  const classes = new Set();
  let classNameValue = '';

  return {
    id: '',
    style: {},
    innerHTML: '',
    get className() {
      return classNameValue;
    },
    set className(value) {
      classNameValue = value;
      classes.clear();
      String(value).split(/\s+/).filter(Boolean).forEach(cls => classes.add(cls));
    },
    classList: {
      add: (cls) => classes.add(cls),
      remove: (cls) => classes.delete(cls),
      contains: (cls) => classes.has(cls),
    },
  };
}

function installFakeDocument() {
  const elements = new Map();

  global.document = {
    body: {
      appendChild(element) {
        elements.set(element.id, element);
      },
    },
    createElement: () => createFakeElement(),
    getElementById: (id) => elements.get(id) || null,
  };
}

function resetModuleState() {
  UniprotTooltipModule.cache = new Map();
  UniprotTooltipModule.tooltipElement = null;
  UniprotTooltipModule.activeHoverKey = null;
  UniprotTooltipModule.hoverRequestToken = 0;
  UniprotTooltipModule.inflightSummaryRequests = new Map();
}

describe('UniprotTooltipModule', () => {
  beforeEach(() => {
    resetModuleState();
    installFakeDocument();
  });

  afterEach(() => {
    delete global.document;
  });

  test('dismisses active tooltip when graph topology changes', () => {
    const handlers = {};
    const context = {
      on: (event, callback) => {
        handlers[event] = callback;
      },
    };

    UniprotTooltipModule.cache.set('P1', {
      accession: 'P1',
      protein_name: 'Protein 1',
      organism_name: 'Test species',
    });

    UniprotTooltipModule.init(context);
    handlers.nodeHover({
      nodeId: 'P1',
      data: { id: 'P1' },
      x: 10,
      y: 20,
      type: 'mouseover',
    });

    expect(UniprotTooltipModule.tooltipElement.classList.contains('hidden')).toBe(false);
    expect(UniprotTooltipModule.activeHoverKey).toBe('protein:P1');

    handlers.clusterExpanded({ clusterId: 'NH1' });

    expect(UniprotTooltipModule.tooltipElement.classList.contains('hidden')).toBe(true);
    expect(UniprotTooltipModule.activeHoverKey).toBeNull();
  });
});
