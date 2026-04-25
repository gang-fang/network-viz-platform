/**
 * SpeciesTreeView — vanilla JS reusable component
 *
 * Renders an interactive, expandable/collapsible taxonomic tree with
 * tri-state checkboxes.  Drop it into any container element.
 *
 * Usage
 * ─────
 *   const tv = new SpeciesTreeView(containerEl, {
 *       maxHeight: '300px',
 *       onChange:  (selected) => console.log(selected),
 *   });
 *   tv.load(rootNode);            // root node from GET /api/species-tree
 *
 * Public API
 * ──────────
 *   tv.load(rootNode)            Load (or reload) a tree
 *   tv.getSelectedSpecies()      → [{ taxid, name }, …]
 *   tv.clearSelection()
 *   tv.selectAll()
 *   tv.setFilter(text)           Programmatically set the search box
 *   tv.onChange(fn)              Register / replace the change callback
 *
 * Node shape expected from the API
 * ─────────────────────────────────
 *   {
 *     name:        string,       taxon name
 *     taxid:       string|null,  NCBI taxonomy ID (null if not in CSV)
 *     isDbSpecies: boolean,      true → proteins exist in the active DB
 *     children:    Node[]        empty array for leaf nodes
 *   }
 *
 * Tri-state rules
 * ───────────────
 *   Internal node checkbox:
 *     unchecked    — no DB-species descendant is selected
 *     indeterminate — some, but not all, DB-species descendants are selected
 *     checked      — every DB-species descendant is selected
 *
 *   Clicking an internal node checkbox:
 *     unchecked / indeterminate → check all descendants
 *     checked                  → uncheck all descendants
 *
 *   Expand / collapse never changes selection state.
 */

(function (global) {
    'use strict';

    // ── CSS — injected once into <head> ───────────────────────────────────────

    const CSS = `
.stv-wrap { font-size: 0.84rem; color: #202a33; }

.stv-search {
    display: block;
    width: 100%;
    box-sizing: border-box;
    padding: 5px 8px;
    margin-bottom: 5px;
    border: 1px solid #cad2db;
    border-radius: 4px;
    font-size: 0.84rem;
    color: #202a33;
    background: #fff;
}
.stv-search:focus { outline: none; border-color: #1f7a4d; }

.stv-toolbar {
    display: flex;
    gap: 5px;
    margin-bottom: 6px;
}
.stv-toolbar button {
    flex: 1;
    padding: 3px 4px;
    font-size: 0.76rem;
    border: 1px solid #cad2db;
    border-radius: 3px;
    background: #f5f7f8;
    color: #202a33;
    cursor: pointer;
    white-space: nowrap;
}
.stv-toolbar button:hover { background: #e5eaed; }

.stv-scroll {
    overflow-y: auto;
}

/* Tree lists */
.stv-scroll ul {
    list-style: none;
    margin: 0;
    padding: 0 0 0 15px;
}
.stv-scroll > ul { padding-left: 0; }
.stv-scroll ul[data-hidden="true"] { display: none; }
.stv-scroll li { margin: 0; padding: 0; }

/* Row */
.stv-row {
    display: flex;
    align-items: center;
    gap: 2px;
    padding: 1px 3px;
    border-radius: 3px;
    min-height: 21px;
}
.stv-row:hover { background: #f0f4f6; }
.stv-row-hidden { display: none !important; }

/* Caret */
.stv-caret {
    flex-shrink: 0;
    width: 15px;
    text-align: center;
    font-size: 0.62rem;
    color: #6b7d8a;
    cursor: pointer;
    padding: 2px 1px;
    border-radius: 2px;
    line-height: 1;
    transition: color 0.1s;
}
.stv-caret:hover { color: #1f7a4d; background: #e0ede6; }
.stv-caret-spacer { flex-shrink: 0; width: 15px; }

/* Checkbox */
.stv-cb {
    flex-shrink: 0;
    width: 13px;
    height: 13px;
    cursor: pointer;
    accent-color: #1f7a4d;
    margin: 0;
}
.stv-cb:disabled { cursor: default; opacity: 0.35; }

/* Labels */
.stv-label {
    flex: 1;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    line-height: 1.35;
    padding-left: 2px;
}
.stv-label-internal {
    font-style: italic;
    color: #3d5166;
    cursor: pointer;
}
.stv-label-leaf-db {
    font-weight: 500;
    color: #202a33;
    cursor: pointer;
}
.stv-label-leaf-none {
    color: #a0adb6;
    font-style: italic;
    cursor: default;
}

/* DB-species count badge on internal nodes */
.stv-badge {
    flex-shrink: 0;
    font-size: 0.7rem;
    color: #8a9aaa;
    margin-left: 3px;
}

/* Search highlight */
mark.stv-hl {
    background: #fff176;
    border-radius: 2px;
    padding: 0;
    color: inherit;
}

/* Empty state */
.stv-empty {
    padding: 8px 4px;
    font-size: 0.82rem;
    color: #8a9aaa;
    font-style: italic;
}
`;

    let _cssInjected = false;
    function injectCss() {
        if (_cssInjected || typeof document === 'undefined') return;
        const s = document.createElement('style');
        s.id = 'stv-stylesheet';
        s.textContent = CSS;
        document.head.appendChild(s);
        _cssInjected = true;
    }

    // ── Node preparation ──────────────────────────────────────────────────────
    // Walks the raw API tree once and attaches runtime fields to every node
    // so we never have to re-traverse for parent lookups.

    function prepareNode(node, parent, flatList) {
        node._id       = flatList.length;
        node._parent   = parent;
        node.isLeaf    = node.children.length === 0;
        node._state    = 0;       // 0=unchecked  1=indeterminate  2=checked
        node._selected = false;   // Selection of this taxon itself, separate from aggregate state.
        node._expanded = false;

        // DOM references (filled in during render)
        node._rowEl    = null;
        node._caretEl  = null;
        node._cbEl     = null;
        node._labelEl  = null;
        node._childUl  = null;

        flatList.push(node);

        for (const child of node.children) {
            prepareNode(child, node, flatList);
        }

        // Subtree DB-species count (used for badges + fast isEmpty checks).
        // Internal taxa may also be DB species, so count the node itself.
        node._dbSpeciesCount =
            (node.isDbSpecies ? 1 : 0) +
            node.children.reduce((s, c) => s + c._dbSpeciesCount, 0);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    function escHtml(str) {
        return String(str)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    function cloneTree(node) {
        if (!node || typeof node !== 'object') return null;
        return {
            name: node.name,
            taxid: node.taxid ?? null,
            isDbSpecies: Boolean(node.isDbSpecies),
            children: Array.isArray(node.children) ? node.children.map(cloneTree) : [],
        };
    }

    /** Collect every selectable DB species node in a subtree. */
    function dbSpeciesNodes(node, out = []) {
        if (node.isDbSpecies) out.push(node);
        for (const c of node.children) dbSpeciesNodes(c, out);
        return out;
    }

    // ── Component class ───────────────────────────────────────────────────────

    class SpeciesTreeView {

        constructor(containerEl, opts = {}) {
            this.container = containerEl;
            this.opts = Object.assign({
                maxHeight:   '300px',
                showSearch:  true,
                showToolbar: true,
                showBadges:  true,   // DB-species count on internal nodes
                onChange:    null,   // fn([{ taxid, name }, …])
            }, opts);

            this._root     = null;
            this._flatList = [];
            this._filterTm = null;

            injectCss();
            this._buildShell();
        }

        // ── Shell (search + toolbar + scroll area) ────────────────────────────

        _buildShell() {
            this.container.innerHTML = '';

            const wrap = document.createElement('div');
            wrap.className = 'stv-wrap';

            // Search input
            if (this.opts.showSearch) {
                this._searchEl = document.createElement('input');
                this._searchEl.type        = 'text';
                this._searchEl.className   = 'stv-search';
                this._searchEl.placeholder = 'Search taxa…';
                this._searchEl.addEventListener('input', () => {
                    clearTimeout(this._filterTm);
                    this._filterTm = setTimeout(() => this._applyFilter(), 180);
                });
                wrap.appendChild(this._searchEl);
            }

            // Toolbar buttons
            if (this.opts.showToolbar) {
                const tb = document.createElement('div');
                tb.className = 'stv-toolbar';

                const mkBtn = (label, fn) => {
                    const b = document.createElement('button');
                    b.type        = 'button';
                    b.textContent = label;
                    b.addEventListener('click', fn);
                    return b;
                };

                tb.appendChild(mkBtn('Select All',    () => this.selectAll()));
                tb.appendChild(mkBtn('Deselect All',  () => this.clearSelection()));
                tb.appendChild(mkBtn('Collapse All',  () => this._collapseAll()));
                wrap.appendChild(tb);
            }

            // Scrollable tree area
            this._scrollEl = document.createElement('div');
            this._scrollEl.className  = 'stv-scroll';
            this._scrollEl.style.maxHeight = this.opts.maxHeight;
            wrap.appendChild(this._scrollEl);

            this.container.appendChild(wrap);
        }

        // ── Load ──────────────────────────────────────────────────────────────

        load(rootNode) {
            // Clone the API tree so multiple consumers can reuse the same fetched
            // payload without sharing mutable UI runtime state.
            this._root     = cloneTree(rootNode);
            this._flatList = [];
            prepareNode(this._root, null, this._flatList);

            this._scrollEl.innerHTML = '';

            if (this._root._dbSpeciesCount === 0) {
                const msg = document.createElement('div');
                msg.className   = 'stv-empty';
                msg.textContent = 'No species with database entries found in this tree.';
                this._scrollEl.appendChild(msg);
                return;
            }

            const ul = this._renderUl([this._root], true);
            this._scrollEl.appendChild(ul);
        }

        // ── Render ────────────────────────────────────────────────────────────

        /** Render a list of sibling nodes into a <ul>. */
        _renderUl(nodes, isRoot) {
            const ul = document.createElement('ul');
            if (!isRoot) ul.setAttribute('data-hidden', 'true');
            for (const node of nodes) {
                ul.appendChild(this._renderLi(node));
            }
            return ul;
        }

        _renderLi(node) {
            const li = document.createElement('li');

            // ── Row ──────────────────────────────────────────────────────────
            const row = document.createElement('div');
            row.className = 'stv-row';
            node._rowEl   = row;

            // Caret (internal nodes only)
            if (!node.isLeaf) {
                const caret = document.createElement('span');
                caret.className   = 'stv-caret';
                caret.textContent = '▶';
                caret.title       = 'Expand / Collapse';
                caret.addEventListener('click', (e) => {
                    e.stopPropagation();
                    this._toggleExpand(node);
                });
                node._caretEl = caret;
                row.appendChild(caret);
            } else {
                const sp = document.createElement('span');
                sp.className = 'stv-caret-spacer';
                row.appendChild(sp);
            }

            // Checkbox
            const cb = document.createElement('input');
            cb.type      = 'checkbox';
            cb.className = 'stv-cb';
            // Nodes without any DB-species in their subtree cannot affect selection.
            cb.disabled  = node._dbSpeciesCount === 0;
            cb.addEventListener('change', (e) => {
                e.stopPropagation();
                if (node.isLeaf) {
                    node._selected = node.isDbSpecies && cb.checked;
                    this._recomputeState(node);
                } else {
                    // Tri-state click: unchecked / indeterminate → check all;
                    // checked → uncheck all.
                    this._setSubtree(node, node._state !== 2);
                }
                this._updateAncestors(node);
                this._notify();
            });
            node._cbEl = cb;
            row.appendChild(cb);

            // Label
            const lbl = document.createElement('span');
            lbl.className = node.isLeaf
                ? (node.isDbSpecies ? 'stv-label stv-label-leaf-db'
                                    : 'stv-label stv-label-leaf-none')
                : (node.isDbSpecies ? 'stv-label stv-label-leaf-db'
                                    : 'stv-label stv-label-internal');

            lbl.title       = node.taxid ? `TaxID: ${node.taxid}` : node.name;
            lbl.textContent = node.name;
            node._labelEl   = lbl;

            // Clicking the label of a leaf DB species toggles the checkbox;
            // clicking an internal label expands / collapses.
            if (!node.isLeaf) {
                lbl.addEventListener('click', () => this._toggleExpand(node));
            } else if (node.isDbSpecies) {
                lbl.addEventListener('click', () => {
                    if (!cb.disabled) cb.click();
                });
            }

            row.appendChild(lbl);

            // Badge: (N) DB species count on internal nodes
            if (!node.isLeaf && this.opts.showBadges && node._dbSpeciesCount > 0) {
                const badge = document.createElement('span');
                badge.className   = 'stv-badge';
                badge.textContent = `(${node._dbSpeciesCount})`;
                row.appendChild(badge);
            }

            li.appendChild(row);

            // Children <ul> (hidden by default)
            if (!node.isLeaf) {
                const childUl    = this._renderUl(node.children, false);
                node._childUl    = childUl;
                li.appendChild(childUl);
            }

            return li;
        }

        // ── Expand / Collapse ─────────────────────────────────────────────────

        _toggleExpand(node) {
            if (node.isLeaf || !node._childUl) return;
            node._expanded = !node._expanded;
            node._childUl.setAttribute('data-hidden', node._expanded ? 'false' : 'true');
            if (node._caretEl) {
                node._caretEl.textContent = node._expanded ? '▼' : '▶';
            }
        }

        /** Expand every ancestor of `node` so it becomes visible. */
        _revealNode(node) {
            let cur = node._parent;
            while (cur) {
                if (!cur._expanded) this._toggleExpand(cur);
                cur = cur._parent;
            }
        }

        _collapseAll() {
            for (const node of this._flatList) {
                if (!node.isLeaf && node._expanded) {
                    node._expanded = false;
                    if (node._childUl) node._childUl.setAttribute('data-hidden', 'true');
                    if (node._caretEl) node._caretEl.textContent = '▶';
                }
            }
        }

        // ── Checkbox state management ─────────────────────────────────────────

        /** Apply a checkbox visual for state 0/1/2 to a node's <input>. */
        _applyVisual(node, state) {
            const cb = node._cbEl;
            if (!cb) return;
            if (state === 2) {
                cb.indeterminate = false;
                cb.checked       = true;
            } else if (state === 1) {
                cb.indeterminate = true;
                cb.checked       = false;
            } else {
                cb.indeterminate = false;
                cb.checked       = false;
            }
            node._state = state;
        }

        /**
         * Recursively set the checked state of every DB-species node in a
         * subtree, and update the visual of every internal node along the way.
         */
        _setSubtree(node, checked) {
            if (node.isDbSpecies) node._selected = checked;
            for (const child of node.children) this._setSubtree(child, checked);
            this._recomputeState(node);
        }

        /** Recompute and apply the tri-state for a single node's subtree. */
        _recomputeState(node) {
            const selectableNodes = dbSpeciesNodes(node);
            if (selectableNodes.length === 0) {
                this._applyVisual(node, 0);
                return;
            }
            const checkedCount = selectableNodes.filter(n => n._selected).length;
            const state = checkedCount === 0 ? 0
                        : checkedCount === selectableNodes.length ? 2
                        : 1;
            this._applyVisual(node, state);
        }

        /** Walk ancestors bottom-up and update their tri-state. */
        _updateAncestors(startNode) {
            let cur = startNode._parent;
            while (cur) {
                this._recomputeState(cur);
                cur = cur._parent;
            }
        }

        // ── Filter / Search ───────────────────────────────────────────────────

        _applyFilter() {
            const q = (this._searchEl ? this._searchEl.value.trim() : '').toLowerCase();

            if (!q) {
                // Reset: restore plain text labels and show all rows
                for (const node of this._flatList) {
                    if (node._rowEl)   node._rowEl.classList.remove('stv-row-hidden');
                    if (node._labelEl) node._labelEl.textContent = node.name;
                }
                return;
            }

            // Bottom-up pass: mark which nodes have a match in their subtree
            const hasMatch = new Uint8Array(this._flatList.length); // indexed by _id

            for (let i = this._flatList.length - 1; i >= 0; i--) {
                const node = this._flatList[i];
                const self = node.name.toLowerCase().includes(q);
                const childHit = node.children.some(c => hasMatch[c._id]);
                const matched  = self || childHit;
                hasMatch[node._id] = matched ? 1 : 0;

                // Highlight matching text in label
                if (node._labelEl) {
                    if (self) {
                        const idx    = node.name.toLowerCase().indexOf(q);
                        const before = escHtml(node.name.slice(0, idx));
                        const match  = escHtml(node.name.slice(idx, idx + q.length));
                        const after  = escHtml(node.name.slice(idx + q.length));
                        node._labelEl.innerHTML =
                            `${before}<mark class="stv-hl">${match}</mark>${after}`;
                    } else {
                        node._labelEl.textContent = node.name;
                    }
                }

                if (node._rowEl) {
                    if (matched) {
                        node._rowEl.classList.remove('stv-row-hidden');
                        if (self) this._revealNode(node);
                    } else {
                        node._rowEl.classList.add('stv-row-hidden');
                    }
                }
            }
        }

        // ── Public API ────────────────────────────────────────────────────────

        /** Returns an array of all currently selected DB species. */
        getSelectedSpecies() {
            const out = [];
            for (const node of this._flatList) {
                if (node.isDbSpecies && node._selected) {
                    out.push({ taxid: node.taxid, name: node.name });
                }
            }
            return out;
        }

        /** Uncheck every node in the tree. */
        clearSelection() {
            if (this._root) {
                this._setSubtree(this._root, false);
                this._recomputeState(this._root);
            }
            this._notify();
        }

        /** Check every DB-species taxon in the tree. */
        selectAll() {
            if (this._root) {
                this._setSubtree(this._root, true);
                this._recomputeState(this._root);
            }
            this._notify();
        }

        /** Programmatically set the search field and apply the filter. */
        setFilter(text) {
            if (this._searchEl) {
                this._searchEl.value = text;
                this._applyFilter();
            }
        }

        /** Register or replace the change callback. */
        onChange(fn) {
            this.opts.onChange = fn;
        }

        _notify() {
            if (typeof this.opts.onChange === 'function') {
                this.opts.onChange(this.getSelectedSpecies());
            }
        }
    }

    // Expose globally so module scripts loaded in the same page can use it
    global.SpeciesTreeView = SpeciesTreeView;

}(typeof window !== 'undefined' ? window : this));
