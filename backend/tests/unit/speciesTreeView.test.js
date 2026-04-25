function createElementStub(tagName) {
  const classes = new Set();
  return {
    tagName,
    children: [],
    style: {},
    attributes: {},
    eventListeners: {},
    className: '',
    textContent: '',
    title: '',
    type: '',
    disabled: false,
    checked: false,
    indeterminate: false,
    value: '',
    appendChild(child) {
      this.children.push(child);
      return child;
    },
    setAttribute(name, value) {
      this.attributes[name] = String(value);
    },
    getAttribute(name) {
      return this.attributes[name];
    },
    addEventListener(event, callback) {
      this.eventListeners[event] = callback;
    },
    classList: {
      add(name) {
        classes.add(name);
      },
      remove(name) {
        classes.delete(name);
      },
      contains(name) {
        return classes.has(name);
      },
      toggle(name, force) {
        if (force) classes.add(name);
        else classes.delete(name);
      },
    },
    set innerHTML(value) {
      this.children = [];
      this._innerHTML = value;
    },
    get innerHTML() {
      return this._innerHTML || '';
    },
  };
}

function setupDocumentStub() {
  global.document = {
    head: createElementStub('head'),
    createElement: createElementStub,
  };
}

function makeTree() {
  return {
    name: 'root',
    taxid: null,
    isDbSpecies: false,
    children: [
      {
        name: 'Branch',
        taxid: '1',
        isDbSpecies: true,
        children: [
          {
            name: 'Needle',
            taxid: '2',
            isDbSpecies: true,
            children: [],
          },
        ],
      },
    ],
  };
}

describe('SpeciesTreeView', () => {
  let SpeciesTreeView;

  beforeEach(() => {
    jest.resetModules();
    setupDocumentStub();
    ({ SpeciesTreeView } = require('../../../frontend/js/components/species-tree-view'));
  });

  afterEach(() => {
    delete global.document;
  });

  test('uses instance-local node ids so filtering works after multiple trees are created', () => {
    const first = new SpeciesTreeView(createElementStub('div'));
    first.load(makeTree());

    const second = new SpeciesTreeView(createElementStub('div'));
    second.load(makeTree());
    second.setFilter('needle');

    expect(second._root._rowEl.classList.contains('stv-row-hidden')).toBe(false);
    expect(second._root.children[0]._rowEl.classList.contains('stv-row-hidden')).toBe(false);
    expect(second._root.children[0].children[0]._rowEl.classList.contains('stv-row-hidden')).toBe(false);
  });

  test('load clones the API tree instead of mutating the caller-owned object', () => {
    const inputTree = makeTree();
    const view = new SpeciesTreeView(createElementStub('div'));

    view.load(inputTree);

    expect(inputTree._id).toBeUndefined();
    expect(inputTree._rowEl).toBeUndefined();
    expect(inputTree.children[0]._selected).toBeUndefined();
  });

  test('selectAll includes DB species that are internal taxon nodes', () => {
    const view = new SpeciesTreeView(createElementStub('div'));
    view.load(makeTree());

    view.selectAll();

    expect(view.getSelectedSpecies()).toEqual([
      { taxid: '1', name: 'Branch' },
      { taxid: '2', name: 'Needle' },
    ]);
  });
});
