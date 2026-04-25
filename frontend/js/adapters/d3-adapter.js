const LAYOUT_RESET_BASE_TICKS = 80;
const LAYOUT_UPDATE_BASE_TICKS = 40;
const LAYOUT_RESET_TICKS_PER_SQRT_NODE = 5;
const LAYOUT_UPDATE_TICKS_PER_SQRT_NODE = 3;
const LAYOUT_RESET_MAX_TICKS = 260;
const LAYOUT_UPDATE_MAX_TICKS = 120;
const LAYOUT_TICK_CHUNK = 25;
const GOLDEN_ANGLE = 2.399963229728653;
const FIT_PADDING = 40;
const FRAME_SAFE_MARGIN = 36;
const OVERLAY_SAFE_MARGIN = 16;
const FIT_SCALE_CAP = 2;
const FORCE_COMPONENT_STRENGTH = 0.04;
const COMPONENT_LAYOUT_FILL = 0.72;
const COMPONENT_PACK_PADDING = 2;
const COMPONENT_PACK_RADIUS_SCALE = 0.92;
const COMPONENT_PACK_ITERATIONS = 140;
const COMPONENT_RADIUS_MIN = 20;
const COMPONENT_RADIUS_BASE = 24;
const COMPONENT_RADIUS_PER_SQRT_NODE = 16;
const COMPONENT_RADIUS_LOG_REDUCTION = 2;
const COMPONENT_EXTENT_BASE_TICKS = 30;
const COMPONENT_EXTENT_TICKS_PER_SQRT_NODE = 4;
const COMPONENT_EXTENT_MAX_TICKS = 120;
const COMPONENT_SEED_BASE_RADIUS = 12;
const COMPONENT_SEED_SPACING = 16;
// Legacy floor on the collide radius. Keeping this at 15 preserves the tight
// cluster-only layout that the simulation produced before collide became
// size-aware; the viewport stays compact and "fit-to-view" lands at a sensible
// scale for collapsed networks. Larger clusters still grow past this floor via
// NODE_COLLISION_PADDING, but only when their geometric radius justifies it.
const NODE_COLLISION_BASE_RADIUS = 15;
const NODE_COLLISION_PADDING = 4;
const EXPANDED_GROUP_MIN_PROTEINS = 2;
const EXPANDED_GROUP_PADDING = 28;
const EXPANDED_GROUP_CLUSTER_MARGIN = 10;
const EXPANDED_GROUP_PAIR_MARGIN = 16;
const EXPANDED_GROUP_SEPARATION_STRENGTH = 0.3;
const EXPANDED_GROUP_MIN_DISTANCE = 1e-6;
const EXPANDED_GROUP_MIN_DENSITY = 0.12;
// When a group's member spread exceeds this fraction of the smaller viewport
// dimension, the proteins are no longer visually cohesive (typical when edges
// inside the cluster have been filtered out). Treating such a group as a
// "bubble" produces a pathologically large radius that flings clusters and
// other groups across the viewport, so we disable separation for it instead.
const EXPANDED_GROUP_MAX_SPREAD_FRACTION = 0.25;

/**
 * D3 Adapter
 * Bridges the Core Graph State to D3 Visualization.
 * Handles the logic of what to show (cluster expansion) and updates D3.
 */
class D3Adapter {
    constructor(appState, containerId) {
        this.appState = appState;
        this.container = document.getElementById(containerId);
        this.width = this.container.clientWidth;
        this.height = this.container.clientHeight;

        this.simulation = null;
        this.svg = null;
        this.g = null; // Main group
        this.layoutRunId = 0;
        this.layoutStatusEl = document.getElementById('layout-status');
        this.pendingDragNode = null;
        this.dragFrame = null;
        this.currentDragId = null;
        this.pinnedNodePositions = new Map();
        this.componentLayoutCache = { revision: null, width: null, height: null, centers: new Map() };
        this.componentMeasurementCache = { revision: null, radiusBySignature: new Map() };
        this.resizeTimer = null;

        // D3 selections
        this.linkSelection = null;
        this.nodeSelection = null;
        this.brushGroup = null; // Group for brush selection

        this.initD3();
        this.setupListeners();
    }

    // Zoom Controls
    zoomIn() {
        this.svg.transition().duration(300).call(this.zoomBehavior.scaleBy, 1.2);
    }

    zoomOut() {
        this.svg.transition().duration(300).call(this.zoomBehavior.scaleBy, 0.8);
    }

    resetZoom() {
        this.fitToView(750);
    }

    resetLayout() {
        this.updateVisualization({
            layoutReset: true,
            fitView: true,
            preservePins: false,
            resetViewport: true,
            topologyRevision: this.appState.topologyRevision
        });
    }

    fitToView(duration = 0) {
        // Calculate bounding box of all nodes
        const nodes = this.nodeSelection.data();
        if (nodes.length === 0) return;

        let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
        nodes.forEach(d => {
            if (d.x < minX) minX = d.x;
            if (d.x > maxX) maxX = d.x;
            if (d.y < minY) minY = d.y;
            if (d.y > maxY) maxY = d.y;
        });

        // If single node or no extent, default to center
        if (minX === Infinity) {
            this.applyZoomTransform(d3.zoomIdentity, duration);
            return;
        }

        const bounds = this.getLayoutViewportBounds();

        const dx = maxX - minX || 1; // Avoid divide by zero
        const dy = maxY - minY || 1;
        const x = (minX + maxX) / 2;
        const y = (minY + maxY) / 2;
        const availableWidth = Math.max(1, bounds.width - FIT_PADDING * 2);
        const availableHeight = Math.max(1, bounds.height - FIT_PADDING * 2);

        // Calculate scale to fit, capped at 2.0 to avoid excessive zoom on small graphs
        const scale = Math.min(FIT_SCALE_CAP, Math.min(availableWidth / dx, availableHeight / dy));

        const targetCenterX = bounds.left + bounds.width / 2;
        const targetCenterY = bounds.top + bounds.height / 2;
        const tx = targetCenterX - scale * x;
        const ty = targetCenterY - scale * y;

        this.applyZoomTransform(d3.zoomIdentity.translate(tx, ty).scale(scale), duration);
    }

    applyZoomTransform(transform, duration = 0) {
        this.svg.interrupt();
        if (duration > 0) {
            this.svg.transition().duration(duration).call(this.zoomBehavior.transform, transform);
            return;
        }
        this.svg.call(this.zoomBehavior.transform, transform);
    }

    getOccludingOverlayElements() {
        if (typeof document === 'undefined') return [];
        return ['export-controls', 'zoom-controls']
            .map(id => document.getElementById(id))
            .filter(Boolean);
    }

    getLayoutViewportBounds() {
        const width = this.width;
        const height = this.height;
        let left = FRAME_SAFE_MARGIN;
        let top = FRAME_SAFE_MARGIN;
        let right = Math.max(left + 1, width - FRAME_SAFE_MARGIN);
        let bottom = Math.max(top + 1, height - FRAME_SAFE_MARGIN);

        const containerRect = this.container && typeof this.container.getBoundingClientRect === 'function'
            ? this.container.getBoundingClientRect()
            : { left: 0, top: 0, right: width, bottom: height };

        this.getOccludingOverlayElements().forEach(element => {
            if (!element || typeof element.getBoundingClientRect !== 'function') return;
            const rect = element.getBoundingClientRect();
            const projectedLeft = rect.left - containerRect.left;
            const projectedRight = rect.right - containerRect.left;
            const projectedTop = rect.top - containerRect.top;
            const projectedBottom = rect.bottom - containerRect.top;

            if (!Number.isFinite(projectedLeft) || !Number.isFinite(projectedRight)) return;
            if (projectedBottom <= 0 || projectedTop >= height) return;

            right = Math.min(right, Math.max(left + 1, projectedLeft - OVERLAY_SAFE_MARGIN));
        });

        return {
            left,
            top,
            right,
            bottom,
            width: Math.max(1, right - left),
            height: Math.max(1, bottom - top),
        };
    }

    initD3() {
        // Basic D3 setup (simplified for now)
        this.zoomBehavior = d3.zoom().on("zoom", (event) => {
            this.g.attr("transform", event.transform);
        });

        this.svg = d3.select(this.container).append("svg")
            .attr("width", this.width)
            .attr("height", this.height)
            .call(this.zoomBehavior)
            .on("dblclick.zoom", null); // Disable double click zoom

        this.g = this.svg.append("g");

        this.simulation = d3.forceSimulation()
            .force("link", d3.forceLink().id(d => d.id).distance(50)) // Adjusted link distance
            .force("charge", d3.forceManyBody().strength(-150)) // Adjusted repulsion
            .force("collide", d3.forceCollide().radius(d => this.getCollisionRadius(d))) // Prevent direct node overlap, with a legacy floor
            .force("expandedGroupSeparation", this.createExpandedGroupSeparationForce())
            .force("x", d3.forceX(d => d._componentCenterX || this.width / 2).strength(FORCE_COMPONENT_STRENGTH))
            .force("y", d3.forceY(d => d._componentCenterY || this.height / 2).strength(FORCE_COMPONENT_STRENGTH))
            .force("center", d3.forceCenter(this.width / 2, this.height / 2));
        this.simulation.stop();

        this.linkSelection = this.g.append("g").attr("class", "links").selectAll("g.link");
        this.nodeSelection = this.g.append("g").attr("class", "nodes").selectAll("circle");

        // Brush group (initially hidden/inactive)
        // Insert as first child so it's BEHIND the graph content (nodes/links)
        // This allows nodes to capture clicks/hovers, while background captures brush.
        this.brushGroup = this.svg.insert("g", ":first-child")
            .attr("class", "brush")
            .style("display", "none");
    }

    setupListeners() {
        this.appState.on('graphUpdated', (detail) => this.updateVisualization(detail));
        this.appState.on('graphVisualsUpdated', () => this.updateNodeVisuals());
        this.appState.on('clusterExpanded', (detail) => this.updateVisualization(detail));
        this.appState.on('clusterCollapsed', (detail) => this.updateVisualization(detail));

        // Selection Mode Listeners
        this.appState.on('selectionModeChanged', (enabled) => {
            this.toggleSelectionMode(enabled);
        });
        this.appState.on('selectionUpdated', () => {
            this.updateSelectionVisuals();
        });

        window.addEventListener('resize', () => this.scheduleResize());
    }

    /**
     * Compute the visible graph based on state (expansion)
     * and update D3.
     */
    async updateVisualization(detail = {}) {
        if (this.currentDragId) {
            this.currentDragId = null;
            this.cancelPendingDragRender();
        }

        const layoutRunId = ++this.layoutRunId;
        const topologyRevision = detail?.topologyRevision ?? this.appState.topologyRevision;

        // Use GraphView to compute the aggregated view
        const { nodes, edges } = GraphView.compute(
            this.appState.graph,
            this.appState.expandedClusters,
            this.appState.hiddenNodes,
            this.appState.hiddenEdges
        );

        // Update viewGraph in AppState
        this.appState.setViewGraph(nodes, edges);

        await this.render(nodes, edges, {
            layoutReset: Boolean(detail && detail.layoutReset),
            fitView: Boolean(detail && detail.fitView),
            // New network loads set preservePins:false so pins cannot leak across datasets.
            preservePins: detail?.preservePins !== false,
            resetViewport: Boolean(detail && detail.resetViewport),
            topologyRevision,
            layoutRunId
        });
    }

    async render(nodes, edges, options = {}) {
        if (options.layoutRunId == null) {
            options.layoutRunId = ++this.layoutRunId;
        }

        const visibleNodes = nodes;
        const visibleEdges = edges;
        this.showLayoutStatus(options.layoutRunId);

        try {
            if (options.layoutReset) {
                if (options.resetViewport) {
                    this.applyZoomTransform(d3.zoomIdentity, 0);
                }
                const shouldPreservePins = options.preservePins !== false;
                const pinnedPositions = shouldPreservePins ? this.getPinnedNodePositions() : null;
                if (!shouldPreservePins) this.clearPinnedNodePositions();
                this.initializeComponentLayout(visibleNodes, visibleEdges);
                if (pinnedPositions) this.restorePinnedNodePositions(visibleNodes, pinnedPositions);
            } else {
                // PRESERVE POSITIONS: Copy x, y, vx, vy from old nodes to new nodes
                this.preserveNodePositions(visibleNodes);
                this.assignComponentTargets(visibleNodes, visibleEdges, false, options.topologyRevision);
            }

            // Update nodes
            this.nodeSelection = this.nodeSelection.data(visibleNodes, d => d.id);
            this.nodeSelection.exit().remove();

            const nodeEnter = this.nodeSelection.enter().append("g")
                .attr("class", "node")
                .call(d3.drag()
                    .on("start", (event, d) => this.dragstarted(event, d))
                    .on("drag", (event, d) => this.dragged(event, d))
                    .on("end", (event, d) => this.dragended(event, d)));

        // Add title for hover - REMOVED for custom tooltip
        // nodeEnter.append("title")
        //     .text(d => d.id + (d._isCluster ? ` (Size: ${d.size})` : ""));

        // Add mouse events for custom tooltip
        nodeEnter.on("mouseover", (event, d) => {
            this.appState.emit('nodeHover', {
                nodeId: d.id,
                data: d,
                x: event.pageX,
                y: event.pageY,
                type: 'mouseover'
            });
        })
            .on("mousemove", (event, d) => {
                this.appState.emit('nodeHover', {
                    nodeId: d.id,
                    data: d,
                    x: event.pageX,
                    y: event.pageY,
                    type: 'mousemove'
                });
            })
            .on("mouseout", (event, d) => {
                this.appState.emit('nodeHover', {
                    nodeId: d.id,
                    data: d,
                    type: 'mouseout'
                });
            });



        // Add click interaction
            nodeEnter.on("click", (event, d) => {
                if (this.appState.selectionMode) {
                    // Export Mode: Toggle Selection
                    this.appState.toggleNodeSelection(d.id);
                } else {
                    // Layout Mode: Expand/Collapse
                    if (d._isCluster) {
                        this.appState.expandCluster(d.id);
                    } else {
                        const nhId = d.NH_ID;
                        if (nhId && this.appState.expandedClusters.has(nhId)) {
                            this.appState.collapseCluster(nhId);
                        }
                    }
                }
                event.stopPropagation();
            });

            this.nodeSelection = nodeEnter.merge(this.nodeSelection);

            // Update links
            this.linkSelection = this.linkSelection.data(visibleEdges, d => d.id);
            this.linkSelection.exit().remove();

            const linkEnter = this.linkSelection.enter().append("g")
                .attr("class", "link");

            // Visible line
            linkEnter.append("line")
                .attr("class", "visible-line")
                .attr("stroke", "#ccc")
                .attr("stroke-opacity", 0.6)
                .attr("stroke-width", d => Math.max(1, Math.sqrt(d.weight || 1)));

            // Hit area (transparent, thicker)
            linkEnter.append("line")
                .attr("class", "hit-area")
                .attr("stroke", "transparent")
                .attr("stroke-width", 10)
                .style("cursor", "pointer")
                .append("title")
                .text(d => `Weight: ${d.weight ? Number(d.weight).toExponential(1).toUpperCase() : 'N/A'}`);

            this.linkSelection = linkEnter.merge(this.linkSelection);

            const layoutCompleted = await this.runStaticLayout(visibleNodes, visibleEdges, options);
            if (!layoutCompleted) return;

            // Update visuals after the fixed-tick layout has assigned final positions.
            try {
                this.updateNodeVisuals();
            } catch (visualErr) {
                console.error("Error updating node visuals:", visualErr);
            }

            if (options.fitView) {
                this.fitToView(0);
            }

        } catch (err) {
            this.clearLayoutStatus(options.layoutRunId);
            console.error("Error in D3Adapter.render:", err);
        }

        // Re-apply selection visuals if needed
        this.updateSelectionVisuals();
    }

    async runStaticLayout(nodes, edges, options = {}) {
        const ticks = this.getStaticTickCount(nodes.length, options.layoutReset);
        const layoutRunId = options.layoutRunId;

        this.simulation.stop();
        this.simulation.nodes(nodes);
        this.simulation.force("link").links(edges);
        this.simulation.alpha(options.layoutReset ? 1 : 0.35);

        await this.nextFrame();

        for (let completedTicks = 0; completedTicks < ticks; completedTicks += LAYOUT_TICK_CHUNK) {
            if (!this.isActiveLayout(layoutRunId)) return false;
            this.simulation.tick(Math.min(LAYOUT_TICK_CHUNK, ticks - completedTicks));
            await this.nextFrame();
        }

        if (!this.isActiveLayout(layoutRunId)) return false;
        // The simulation is intentionally stopped and alpha-zeroed because layouts
        // are solved in fixed chunks and painted once, not animated continuously.
        this.simulation.stop();
        this.simulation.alpha(0);
        this.ticked();
        this.clearLayoutStatus(layoutRunId);
        return true;
    }

    getStaticTickCount(nodeCount, layoutReset) {
        const base = layoutReset ? LAYOUT_RESET_BASE_TICKS : LAYOUT_UPDATE_BASE_TICKS;
        const perSqrtNode = layoutReset ? LAYOUT_RESET_TICKS_PER_SQRT_NODE : LAYOUT_UPDATE_TICKS_PER_SQRT_NODE;
        const maxTicks = layoutReset ? LAYOUT_RESET_MAX_TICKS : LAYOUT_UPDATE_MAX_TICKS;
        return Math.min(maxTicks, base + Math.ceil(perSqrtNode * Math.sqrt(Math.max(1, nodeCount))));
    }

    nextFrame() {
        return new Promise(resolve => requestAnimationFrame(resolve));
    }

    isActiveLayout(layoutRunId) {
        return layoutRunId === this.layoutRunId;
    }

    showLayoutStatus(layoutRunId) {
        if (!this.layoutStatusEl || !this.isActiveLayout(layoutRunId)) return;
        this.layoutStatusEl.hidden = false;
    }

    clearLayoutStatus(layoutRunId) {
        if (!this.layoutStatusEl || !this.isActiveLayout(layoutRunId)) return;
        this.layoutStatusEl.hidden = true;
    }

    forceClearLayoutStatus() {
        if (!this.layoutStatusEl) return;
        // Dragging cancels the single active layout run, so no run-id guard is needed here.
        this.layoutStatusEl.hidden = true;
    }

    scheduleResize() {
        if (this.resizeTimer) clearTimeout(this.resizeTimer);
        this.resizeTimer = setTimeout(() => this.handleResize(), 150);
    }

    handleResize() {
        const nextWidth = this.container.clientWidth;
        const nextHeight = this.container.clientHeight;
        if (nextWidth === this.width && nextHeight === this.height) return;

        this.width = nextWidth;
        this.height = nextHeight;
        this.componentLayoutCache = { revision: null, width: null, height: null, centers: new Map() };
        this.svg.attr("width", this.width).attr("height", this.height);
        this.simulation
            .force("center", d3.forceCenter(this.width / 2, this.height / 2))
            .force("x", d3.forceX(d => d._componentCenterX || this.width / 2).strength(FORCE_COMPONENT_STRENGTH))
            .force("y", d3.forceY(d => d._componentCenterY || this.height / 2).strength(FORCE_COMPONENT_STRENGTH));
        this.updateVisualization({ layoutReset: true, fitView: true, topologyRevision: this.appState.topologyRevision });
    }

    toggleSelectionMode(enabled) {
        if (enabled) {
            // Enable Brush
            this.brushGroup.style("display", "block");

            const brush = d3.brush()
                .extent([[0, 0], [this.width, this.height]])
                .on("start brush", (event) => {
                    // Optional: Live update during brush?
                    // Might be too heavy. Let's stick to "end" for selection logic,
                    // or "brush" if we want immediate feedback.
                })
                .on("end", (event) => {
                    if (!event.selection) return;
                    const [[x0, y0], [x1, y1]] = event.selection;

                    // Find nodes within selection
                    const selectedIds = [];
                    this.nodeSelection.each(d => {
                        // Need to use current transform position
                        // But d.x/d.y are simulation coordinates.
                        // The zoom transform applies to the group 'this.g'.
                        // The brush is on 'this.svg' (or 'this.brushGroup' attached to svg).
                        // So we need to transform node coordinates to screen coordinates OR transform selection to graph coordinates.

                        // Wait, brush is usually applied to the SVG.
                        // And nodes are inside 'g' which has a transform.
                        // So we need to apply the transform to d.x, d.y to compare with x0, y0.

                        const transform = d3.zoomTransform(this.svg.node());
                        const screenX = transform.applyX(d.x);
                        const screenY = transform.applyY(d.y);

                        if (screenX >= x0 && screenX <= x1 && screenY >= y0 && screenY <= y1) {
                            selectedIds.push(d.id);
                        }
                    });

                    if (selectedIds.length > 0) {
                        this.appState.selectNodes(selectedIds);
                    }

                    // Clear brush box after selection
                    this.brushGroup.call(brush.move, null);
                });

            this.brushGroup.call(brush);

            // Disable Zoom (Brush conflicts with Zoom)
            this.svg.on(".zoom", null);

        } else {
            // Disable Brush
            this.brushGroup.style("display", "none").on(".brush", null);

            // Re-enable Zoom
            this.svg.call(this.zoomBehavior).on("dblclick.zoom", null);
        }

        this.updateSelectionVisuals();
    }

    updateSelectionVisuals() {
        if (!this.nodeSelection) return;

        this.nodeSelection.classed("selected-node", d => {
            return this.appState.selectedNodes.has(d.id);
        });
    }

    preserveNodePositions(visibleNodes) {
        if (!this.simulation) return;

        const oldNodes = new Map(this.simulation.nodes().map(n => [n.id, n]));
        const expandedClusterMembers = new Map();

        visibleNodes.forEach(node => {
            if (!node || !node.NH_ID || oldNodes.has(node.id) || !oldNodes.has(node.NH_ID)) return;
            const clusterId = String(node.NH_ID);
            if (!expandedClusterMembers.has(clusterId)) {
                expandedClusterMembers.set(clusterId, []);
            }
            expandedClusterMembers.get(clusterId).push(node);
        });

        expandedClusterMembers.forEach((members, clusterId) => {
            this.seedExpandedClusterMembers(members, oldNodes.get(clusterId));
        });

        visibleNodes.forEach(node => {
            const oldNode = oldNodes.get(node.id);
            if (oldNode) {
                // Case 1: Node exists - keep position
                node.x = oldNode.x;
                node.y = oldNode.y;
                node.vx = oldNode.vx;
                node.vy = oldNode.vy;
                node.fx = oldNode.fx;
                node.fy = oldNode.fy;
            } else {
                // Case 2: New Node (Expansion or Collapse)

                // A. Expansion members are already seeded around their parent
                // cluster center above; avoid collapsing them back into a tight
                // jitter cloud here.
                if (node.NH_ID && oldNodes.has(node.NH_ID)) {
                    return;
                }
                // B. Collapse: Node is a Cluster, check if any of its children were visible
                else if (node._isCluster) {
                    // Find any child in oldNodes that belongs to this cluster
                    // We iterate oldNodes values. This is O(N), but N is visible nodes (usually < 1000)
                    for (const old of oldNodes.values()) {
                        if (old.NH_ID === node.id) {
                            node.x = old.x;
                            node.y = old.y;
                            node.vx = old.vx;
                            node.vy = old.vy;
                            break; // Found one, good enough
                        }
                    }
                }
            }
        });
    }

    seedExpandedClusterMembers(members, parent) {
        if (!parent || !Array.isArray(members) || members.length === 0) return;

        const orderedMembers = [...members].sort((left, right) => String(left.id).localeCompare(String(right.id)));
        const baseVelocityX = Number.isFinite(parent.vx) ? parent.vx : 0;
        const baseVelocityY = Number.isFinite(parent.vy) ? parent.vy : 0;
        const baseRingRadius = Math.max(this.getCollisionRadius(parent) + 12, 24);
        const ringSpacing = 14;

        orderedMembers.forEach((node, index) => {
            const angle = index * 2.399963229728653;
            const radius = baseRingRadius + Math.sqrt(index) * ringSpacing;
            node.x = parent.x + Math.cos(angle) * radius;
            node.y = parent.y + Math.sin(angle) * radius;
            node.vx = baseVelocityX;
            node.vy = baseVelocityY;
            node.fx = null;
            node.fy = null;
        });
    }

    getPinnedNodePositions() {
        const pinned = new Map(this.pinnedNodePositions || []);
        if (!this.simulation) return pinned.size > 0 ? pinned : null;

        this.simulation.nodes().forEach(node => {
            const hasPinnedX = node.fx !== null && node.fx !== undefined;
            const hasPinnedY = node.fy !== null && node.fy !== undefined;
            if (!hasPinnedX && !hasPinnedY) return;

            this.storePinnedNodePosition(node);
            pinned.set(String(node.id), this.pinnedNodePositions.get(String(node.id)));
        });

        return pinned.size > 0 ? pinned : null;
    }

    restorePinnedNodePositions(visibleNodes, pinnedPositions) {
        visibleNodes.forEach(node => {
            const pinned = pinnedPositions.get(String(node.id));
            if (!pinned) return;

            node.x = pinned.x;
            node.y = pinned.y;
            node.fx = pinned.fx;
            node.fy = pinned.fy;
            node.vx = 0;
            node.vy = 0;
            this.storePinnedNodePosition(node);
        });
    }

    storePinnedNodePosition(node) {
        if (!node || !this.pinnedNodePositions) return;
        const hasPinnedX = node.fx !== null && node.fx !== undefined;
        const hasPinnedY = node.fy !== null && node.fy !== undefined;
        if (!hasPinnedX && !hasPinnedY) return;

        this.pinnedNodePositions.set(String(node.id), {
            x: hasPinnedX ? node.fx : node.x,
            y: hasPinnedY ? node.fy : node.y,
            fx: node.fx,
            fy: node.fy
        });
    }

    clearPinnedNodePositions() {
        if (this.pinnedNodePositions) this.pinnedNodePositions.clear();
    }

    getEdgeNodeId(value) {
        return typeof value === 'object' && value !== null ? value.id : String(value);
    }

    computeComponents(nodes, edges) {
        const nodeIds = new Set(nodes.map(node => String(node.id)));
        const adjacency = new Map(nodes.map(node => [String(node.id), []]));

        edges.forEach(edge => {
            const sourceId = this.getEdgeNodeId(edge.source);
            const targetId = this.getEdgeNodeId(edge.target);
            if (!nodeIds.has(sourceId) || !nodeIds.has(targetId)) return;
            adjacency.get(sourceId).push(targetId);
            adjacency.get(targetId).push(sourceId);
        });

        const nodeById = new Map(nodes.map(node => [String(node.id), node]));
        const visited = new Set();
        const components = [];

        nodes.forEach(node => {
            const startId = String(node.id);
            if (visited.has(startId)) return;

            const queue = [startId];
            let queueIndex = 0;
            const component = [];
            visited.add(startId);

            while (queueIndex < queue.length) {
                const currentId = queue[queueIndex];
                queueIndex += 1;
                component.push(nodeById.get(currentId));

                adjacency.get(currentId).forEach(nextId => {
                    if (!visited.has(nextId)) {
                        visited.add(nextId);
                        queue.push(nextId);
                    }
                });
            }

            components.push(component);
        });

        components.sort((a, b) => b.length - a.length);
        return components;
    }

    applyCachedComponentTargets(nodes, topologyRevision) {
        if (
            this.componentLayoutCache.revision !== topologyRevision ||
            this.componentLayoutCache.width !== this.width ||
            this.componentLayoutCache.height !== this.height
        ) {
            return false;
        }

        nodes.forEach(node => {
            const center = this.componentLayoutCache.centers.get(String(node.id));
            if (!center) return;
            node._componentCenterX = center.x;
            node._componentCenterY = center.y;
        });
        return true;
    }

    updateComponentTargetCache(nodes, topologyRevision) {
        const centers = new Map();
        nodes.forEach(node => {
            centers.set(String(node.id), {
                x: node._componentCenterX,
                y: node._componentCenterY
            });
        });
        this.componentLayoutCache = {
            revision: topologyRevision,
            width: this.width,
            height: this.height,
            centers
        };
    }

    buildComponentEdgeMap(components, edges) {
        const componentIndexByNodeId = new Map();
        components.forEach((component, componentIndex) => {
            component.forEach(node => {
                componentIndexByNodeId.set(String(node.id), componentIndex);
            });
        });

        const componentEdges = components.map(() => []);
        (edges || []).forEach(edge => {
            const sourceId = this.getEdgeNodeId(edge.source);
            const targetId = this.getEdgeNodeId(edge.target);
            const sourceComponentIndex = componentIndexByNodeId.get(sourceId);
            const targetComponentIndex = componentIndexByNodeId.get(targetId);
            if (sourceComponentIndex == null || sourceComponentIndex !== targetComponentIndex) return;

            componentEdges[sourceComponentIndex].push({
                ...edge,
                source: sourceId,
                target: targetId,
            });
        });

        return componentEdges;
    }

    getComponentSignature(component) {
        return (component || [])
            .map(node => String(node.id))
            .sort()
            .join('|');
    }

    getCachedComponentPlacementRadius(signature, topologyRevision = this.appState.topologyRevision) {
        if (!signature) return null;
        if (this.componentMeasurementCache.revision !== topologyRevision) {
            this.componentMeasurementCache = { revision: topologyRevision, radiusBySignature: new Map() };
            return null;
        }
        return this.componentMeasurementCache.radiusBySignature.get(signature) ?? null;
    }

    storeCachedComponentPlacementRadius(signature, radius, topologyRevision = this.appState.topologyRevision) {
        if (!signature || !Number.isFinite(radius)) return;
        if (this.componentMeasurementCache.revision !== topologyRevision) {
            this.componentMeasurementCache = { revision: topologyRevision, radiusBySignature: new Map() };
        }
        this.componentMeasurementCache.radiusBySignature.set(signature, radius);
    }

    estimateComponentPlacementRadius(component) {
        const size = Array.isArray(component) ? component.length : Number(component) || 0;
        if (size <= 1) return COMPONENT_RADIUS_MIN;
        // The log input is floored at 10 so tiny components do not get
        // artificially boosted, and the per-sqrt term is floored at 8 so very
        // large components keep a conservative non-zero envelope.
        const perSqrtNode = Math.max(
            8,
            COMPONENT_RADIUS_PER_SQRT_NODE - COMPONENT_RADIUS_LOG_REDUCTION * Math.log10(Math.max(10, size))
        );
        return COMPONENT_RADIUS_BASE + Math.sqrt(size) * perSqrtNode;
    }

    getComponentExtentTickCount(nodeCount) {
        return Math.min(
            COMPONENT_EXTENT_MAX_TICKS,
            COMPONENT_EXTENT_BASE_TICKS + Math.ceil(COMPONENT_EXTENT_TICKS_PER_SQRT_NODE * Math.sqrt(Math.max(1, nodeCount)))
        );
    }

    seedComponentPositions(component, originX = 0, originY = 0, { resetPins = false } = {}) {
        (component || []).forEach((node, index) => {
            const radius = component.length === 1 ? 0 : COMPONENT_SEED_BASE_RADIUS + Math.sqrt(index) * COMPONENT_SEED_SPACING;
            node.x = originX + Math.cos(index * GOLDEN_ANGLE) * radius;
            node.y = originY + Math.sin(index * GOLDEN_ANGLE) * radius;
            node.vx = 0;
            node.vy = 0;
            if (resetPins) {
                node.fx = null;
                node.fy = null;
            }
        });
    }

    measureComponentPlacementRadius(component, componentEdges = [], topologyRevision = this.appState.topologyRevision) {
        const signature = this.getComponentSignature(component);
        const cachedRadius = this.getCachedComponentPlacementRadius(signature, topologyRevision);
        if (cachedRadius != null) return cachedRadius;
        if (!Array.isArray(component) || component.length <= 1) {
            const estimatedRadius = this.estimateComponentPlacementRadius(component);
            this.storeCachedComponentPlacementRadius(signature, estimatedRadius, topologyRevision);
            return estimatedRadius;
        }
        if (typeof d3 === 'undefined' || typeof d3.forceSimulation !== 'function') {
            const estimatedRadius = this.estimateComponentPlacementRadius(component);
            this.storeCachedComponentPlacementRadius(signature, estimatedRadius, topologyRevision);
            return estimatedRadius;
        }

        const simNodes = component.map(node => ({ ...node }));
        this.seedComponentPositions(simNodes);
        const simEdges = (componentEdges || []).map(edge => ({
            ...edge,
            source: this.getEdgeNodeId(edge.source),
            target: this.getEdgeNodeId(edge.target),
        }));

        const simulation = d3.forceSimulation(simNodes)
            .force('link', d3.forceLink(simEdges).id(node => node.id).distance(50))
            .force('charge', d3.forceManyBody().strength(-150))
            .force('collide', d3.forceCollide().radius(node => this.getCollisionRadius(node)))
            .force('x', d3.forceX(0).strength(FORCE_COMPONENT_STRENGTH))
            .force('y', d3.forceY(0).strength(FORCE_COMPONENT_STRENGTH))
            .force('center', d3.forceCenter(0, 0))
            .stop();

        const tickCount = this.getComponentExtentTickCount(simNodes.length);
        for (let tick = 0; tick < tickCount; tick += 1) {
            simulation.tick();
        }
        simulation.stop();

        let centerX = 0;
        let centerY = 0;
        simNodes.forEach(node => {
            centerX += Number.isFinite(node.x) ? node.x : 0;
            centerY += Number.isFinite(node.y) ? node.y : 0;
        });
        centerX /= simNodes.length;
        centerY /= simNodes.length;

        let extentRadius = COMPONENT_RADIUS_MIN;
        simNodes.forEach(node => {
            const dx = (Number.isFinite(node.x) ? node.x : centerX) - centerX;
            const dy = (Number.isFinite(node.y) ? node.y : centerY) - centerY;
            extentRadius = Math.max(extentRadius, Math.hypot(dx, dy) + this.getCollisionRadius(node));
        });

        this.storeCachedComponentPlacementRadius(signature, extentRadius, topologyRevision);
        return extentRadius;
    }

    getPackedCircleBounds(circles) {
        if (!circles || circles.length === 0) {
            return { minX: 0, maxX: 0, minY: 0, maxY: 0, width: 1, height: 1 };
        }

        let minX = Infinity;
        let maxX = -Infinity;
        let minY = Infinity;
        let maxY = -Infinity;

        circles.forEach(circle => {
            minX = Math.min(minX, circle.x - circle.r);
            maxX = Math.max(maxX, circle.x + circle.r);
            minY = Math.min(minY, circle.y - circle.r);
            maxY = Math.max(maxY, circle.y + circle.r);
        });

        return {
            minX,
            maxX,
            minY,
            maxY,
            width: Math.max(1, maxX - minX),
            height: Math.max(1, maxY - minY),
        };
    }

    packComponentsByEstimatedSize(components, bounds, componentEdges = [], topologyRevision = this.appState.topologyRevision) {
        const circles = components.map((component, index) => ({
            component,
            index,
            x: 0,
            y: 0,
            r: this.measureComponentPlacementRadius(component, componentEdges[index], topologyRevision) * COMPONENT_PACK_RADIUS_SCALE + COMPONENT_PACK_PADDING,
        }));

        if (circles.length === 0) return circles;
        circles.sort((left, right) => right.r - left.r || left.index - right.index);

        circles[0].x = 0;
        circles[0].y = 0;
        for (let index = 1; index < circles.length; index += 1) {
            const angle = index * GOLDEN_ANGLE;
            const distance = circles[index - 1].r + circles[index].r;
            circles[index].x = Math.cos(angle) * distance;
            circles[index].y = Math.sin(angle) * distance;
        }

        for (let iteration = 0; iteration < COMPONENT_PACK_ITERATIONS; iteration += 1) {
            let moved = false;

            for (let leftIndex = 0; leftIndex < circles.length; leftIndex += 1) {
                const left = circles[leftIndex];
                for (let rightIndex = leftIndex + 1; rightIndex < circles.length; rightIndex += 1) {
                    const right = circles[rightIndex];
                    let dx = right.x - left.x;
                    let dy = right.y - left.y;
                    let distance = Math.hypot(dx, dy);
                    const minDistance = left.r + right.r;

                    if (distance >= minDistance) continue;

                    if (distance < 1e-6) {
                        const angle = (leftIndex + rightIndex + 1) * GOLDEN_ANGLE;
                        dx = Math.cos(angle);
                        dy = Math.sin(angle);
                        distance = 1;
                    }

                    const overlap = minDistance - distance;
                    const pushX = (dx / distance) * overlap * 0.5;
                    const pushY = (dy / distance) * overlap * 0.5;
                    left.x -= pushX;
                    left.y -= pushY;
                    right.x += pushX;
                    right.y += pushY;
                    moved = true;
                }
            }

            let centerX = 0;
            let centerY = 0;
            circles.forEach(circle => {
                centerX += circle.x;
                centerY += circle.y;
            });
            centerX /= circles.length;
            centerY /= circles.length;

            circles.forEach(circle => {
                circle.x -= centerX * 0.08;
                circle.y -= centerY * 0.08;
            });

            if (!moved) break;
        }

        const packedBounds = this.getPackedCircleBounds(circles);
        const availableWidth = Math.max(1, bounds.width * COMPONENT_LAYOUT_FILL);
        const availableHeight = Math.max(1, bounds.height * COMPONENT_LAYOUT_FILL);
        const scale = Math.min(1, availableWidth / packedBounds.width, availableHeight / packedBounds.height);
        const packedCenterX = (packedBounds.minX + packedBounds.maxX) / 2;
        const packedCenterY = (packedBounds.minY + packedBounds.maxY) / 2;
        const targetCenterX = bounds.left + bounds.width / 2;
        const targetCenterY = bounds.top + bounds.height / 2;

        circles.forEach(circle => {
            circle.x = targetCenterX + (circle.x - packedCenterX) * scale;
            circle.y = targetCenterY + (circle.y - packedCenterY) * scale;
            circle.r *= scale;
        });

        circles.sort((left, right) => left.index - right.index);
        return circles;
    }

    assignComponentTargets(nodes, edges, resetPositions, topologyRevision = this.appState.topologyRevision) {
        if (!resetPositions && this.applyCachedComponentTargets(nodes, topologyRevision)) {
            return;
        }

        const components = this.computeComponents(nodes, edges);
        if (components.length === 0) return;

        const bounds = this.getLayoutViewportBounds();
        const componentEdges = this.buildComponentEdgeMap(components, edges);
        const packedComponents = this.packComponentsByEstimatedSize(components, bounds, componentEdges, topologyRevision);

        packedComponents.forEach(({ component, x: centerX, y: centerY }) => {
            if (resetPositions) {
                this.seedComponentPositions(component, centerX, centerY, { resetPins: true });
            }

            component.forEach((node, nodeIndex) => {
                node._componentCenterX = centerX;
                node._componentCenterY = centerY;

                if (!resetPositions) return;
            });
        });

        this.updateComponentTargetCache(nodes, topologyRevision);
    }

    initializeComponentLayout(nodes, edges) {
        this.assignComponentTargets(nodes, edges, true, this.appState.topologyRevision);
    }

    createExpandedGroupSeparationForce() {
        // Membership is cached on `force.initialize` (called by d3 whenever
        // simulation.nodes(...) is set) so the per-tick work is just centroid +
        // radius math, not a fresh Map/array allocation per tick.
        let groups = [];
        let clusterNodes = [];

        const force = (alpha) => {
            if (groups.length === 0) return;
            this.updateExpandedGroupBounds(groups);
            if (clusterNodes.length > 0) {
                this.applyExpandedGroupClusterPush(clusterNodes, groups, alpha);
            }
            if (groups.length >= 2) {
                this.applyExpandedGroupPairPush(groups, alpha);
            }
        };

        force.initialize = (nextNodes) => {
            const membership = this.buildExpandedProteinMembership(nextNodes || []);
            groups = membership.groups;
            clusterNodes = membership.clusterNodes;
        };

        return force;
    }

    buildExpandedProteinMembership(nodes) {
        const expandedClusters = this.appState && this.appState.expandedClusters;
        const groupMap = new Map();
        const clusterNodes = [];

        for (const node of nodes) {
            if (!node) continue;
            if (node._isCluster) {
                clusterNodes.push(node);
                continue;
            }
            const nhId = node.NH_ID;
            if (!nhId) continue;
            // Explicit invariant: only proteins whose parent cluster is expanded
            // contribute to a group bubble. Without the check the filter is
            // implicitly correct only because GraphView strips proteins from
            // collapsed clusters.
            if (expandedClusters && !expandedClusters.has(nhId)) continue;

            const groupId = String(nhId);
            let group = groupMap.get(groupId);
            if (!group) {
                group = { id: groupId, members: [], centerX: 0, centerY: 0, radius: 0, active: true };
                groupMap.set(groupId, group);
            }
            group.members.push(node);
        }

        const groups = [];
        for (const group of groupMap.values()) {
            if (group.members.length >= EXPANDED_GROUP_MIN_PROTEINS) {
                groups.push(group);
            }
        }

        return { groups, clusterNodes };
    }

    updateExpandedGroupBounds(groups) {
        const spreadCap = Math.min(this.width, this.height) * EXPANDED_GROUP_MAX_SPREAD_FRACTION;

        for (const group of groups) {
            const members = group.members;
            const count = members.length;
            if (count === 0) {
                group.centerX = 0;
                group.centerY = 0;
                group.radius = 0;
                group.active = false;
                continue;
            }

            let sumX = 0;
            let sumY = 0;
            for (const node of members) {
                sumX += Number.isFinite(node.x) ? node.x : 0;
                sumY += Number.isFinite(node.y) ? node.y : 0;
            }
            const centerX = sumX / count;
            const centerY = sumY / count;

            let maxRadius = 0;
            let effectiveMemberCount = 0;
            for (const node of members) {
                const x = Number.isFinite(node.x) ? node.x : centerX;
                const y = Number.isFinite(node.y) ? node.y : centerY;
                const dx = x - centerX;
                const dy = y - centerY;
                const r = Math.hypot(dx, dy) + this.getNodeRadius(node);
                if (r > maxRadius) maxRadius = r;
                effectiveMemberCount += 1;
            }

            group.centerX = centerX;
            group.centerY = centerY;
            group.radius = maxRadius + EXPANDED_GROUP_PADDING;
            // Disable separation only for sparse, fragmented groups. Large but
            // dense expanded clusters still need a protective bubble so nearby
            // collapsed clusters do not remain buried inside the protein cloud.
            const density = effectiveMemberCount / Math.max(1, maxRadius);
            group.active = maxRadius <= spreadCap || density >= EXPANDED_GROUP_MIN_DENSITY;
        }
    }

    applyExpandedGroupClusterPush(clusterNodes, groups, alpha = 1) {
        for (const node of clusterNodes) {
            if (!Number.isFinite(node.x) || !Number.isFinite(node.y)) continue;

            const nodeRadius = this.getNodeRadius(node);
            for (const group of groups) {
                if (group.active === false) continue;
                const minDistance = group.radius + nodeRadius + EXPANDED_GROUP_CLUSTER_MARGIN;
                let dx = node.x - group.centerX;
                let dy = node.y - group.centerY;
                let distance = Math.hypot(dx, dy);

                if (distance >= minDistance) continue;

                if (distance < EXPANDED_GROUP_MIN_DISTANCE) {
                    const angle = this.getDeterministicSeparationAngle(node.id, group.id);
                    dx = Math.cos(angle);
                    dy = Math.sin(angle);
                    distance = 1;
                }

                const push = (minDistance - distance) * EXPANDED_GROUP_SEPARATION_STRENGTH * alpha;
                node.vx = (node.vx || 0) + (dx / distance) * push;
                node.vy = (node.vy || 0) + (dy / distance) * push;
            }
        }
    }

    applyExpandedGroupPairPush(groups, alpha = 1) {
        for (let i = 0; i < groups.length; i += 1) {
            const a = groups[i];
            if (a.active === false) continue;
            for (let j = i + 1; j < groups.length; j += 1) {
                const b = groups[j];
                if (b.active === false) continue;
                const minDistance = a.radius + b.radius + EXPANDED_GROUP_PAIR_MARGIN;
                let dx = a.centerX - b.centerX;
                let dy = a.centerY - b.centerY;
                let distance = Math.hypot(dx, dy);

                if (distance >= minDistance) continue;

                if (distance < EXPANDED_GROUP_MIN_DISTANCE) {
                    const angle = this.getDeterministicSeparationAngle(a.id, b.id);
                    dx = Math.cos(angle);
                    dy = Math.sin(angle);
                    distance = 1;
                }

                // Split the push so each group is responsible for half. Apply
                // to each member protein rather than the centroid (the centroid
                // is derived from member positions on the next tick).
                const push = (minDistance - distance) * EXPANDED_GROUP_SEPARATION_STRENGTH * alpha * 0.5;
                const pushX = (dx / distance) * push;
                const pushY = (dy / distance) * push;

                for (const node of a.members) {
                    node.vx = (node.vx || 0) + pushX;
                    node.vy = (node.vy || 0) + pushY;
                }
                for (const node of b.members) {
                    node.vx = (node.vx || 0) - pushX;
                    node.vy = (node.vy || 0) - pushY;
                }
            }
        }
    }

    getDeterministicSeparationAngle(nodeId, groupId) {
        const key = `${nodeId}|${groupId}`;
        let hash = 0;
        for (let index = 0; index < key.length; index += 1) {
            hash = (hash * 31 + key.charCodeAt(index)) >>> 0;
        }
        return (hash / 0xFFFFFFFF) * Math.PI * 2;
    }

    getNodeRadius(d) {
        if (d._isCluster) {
            // Use d.size (aggregated size) for clusters
            // Ensure it's at least slightly larger than a protein (5)
            // Log scale is good for large variance
            return 7 + Math.log(d.size || 1) * 3;
        }
        return 5;
    }

    getCollisionRadius(d) {
        // Honour the legacy compact spacing for small/typical nodes (proteins
        // and small clusters) so that "collapse all" stays inside the
        // viewport. Only nodes whose geometric radius pushes past the floor
        // get extra room — the rare oversized cluster, not all clusters.
        return Math.max(NODE_COLLISION_BASE_RADIUS, this.getNodeRadius(d) + NODE_COLLISION_PADDING);
    }

    updateNodeVisuals() {
        if (!this.nodeSelection) return;
        this.nodeSelection.each((d, i, nodes) => {
            const element = d3.select(nodes[i]);
            const colors = this.appState.nodeColors.get(d.id);
            const r = this.getNodeRadius(d);

            // Remove old content
            element.select("circle").remove();
            element.selectAll("path.pie-slice").remove();

            // Check if highlighted
            if (colors && colors.length > 0) {
                element.classed("highlighted", true);

                if (colors.length === 1) {
                    // Single Color -> Circle
                    element.append("circle")
                        .attr("r", r)
                        .attr("fill", colors[0])
                        .attr("stroke", "#fff")
                        .attr("stroke-width", 1.5);
                } else {
                    // Multiple Colors -> Pie Chart
                    const pie = d3.pie().value(1).sort(null); // Equal slices
                    const arc = d3.arc().innerRadius(0).outerRadius(r);

                    element.selectAll("path.pie-slice")
                        .data(pie(colors))
                        .enter().append("path")
                        .attr("class", "pie-slice")
                        .attr("d", arc)
                        .attr("fill", slice => slice.data)
                        .attr("stroke", "#fff")
                        .attr("stroke-width", 0.5);

                    // Add border circle to match single-color nodes
                    element.append("circle")
                        .attr("r", r)
                        .attr("fill", "none")
                        .attr("stroke", "#fff")
                        .attr("stroke-width", 1.5);
                }
            } else {
                // Not highlighted -> Default Circle
                element.classed("highlighted", false);
                element.append("circle")
                    .attr("r", r)
                    .attr("fill", d._isCluster ? "#95a5a6" : "#3498db");
            }
        });
    }

    ticked() {
        this.updateLinkPositions(this.linkSelection);
        this.updateNodePositions(this.nodeSelection);
    }

    updateLinkPositions(selection) {
        selection.select(".visible-line")
            .attr("x1", d => d.source.x)
            .attr("y1", d => d.source.y)
            .attr("x2", d => d.target.x)
            .attr("y2", d => d.target.y);

        selection.select(".hit-area")
            .attr("x1", d => d.source.x)
            .attr("y1", d => d.source.y)
            .attr("x2", d => d.target.x)
            .attr("y2", d => d.target.y);
    }

    updateNodePositions(selection) {
        selection
            .attr("transform", d => `translate(${d.x},${d.y})`);
    }

    renderDraggedNode(node) {
        if (!node || !this.nodeSelection || !this.linkSelection) return;
        const nodeId = String(node.id);
        this.updateNodePositions(this.nodeSelection.filter(d => String(d.id) === nodeId));
        this.updateLinkPositions(this.linkSelection.filter(edge => {
            return this.getEdgeNodeId(edge.source) === nodeId || this.getEdgeNodeId(edge.target) === nodeId;
        }));
    }

    scheduleDragRender(node) {
        this.pendingDragNode = node;
        if (this.dragFrame) return;

        this.dragFrame = requestAnimationFrame(() => {
            const pendingNode = this.pendingDragNode;
            this.pendingDragNode = null;
            this.dragFrame = null;
            this.renderDraggedNode(pendingNode);
        });
    }

    cancelPendingDragRender() {
        if (this.dragFrame) {
            cancelAnimationFrame(this.dragFrame);
            this.dragFrame = null;
        }
        this.pendingDragNode = null;
    }

    // Drag functions
    dragstarted(event, d) {
        this.layoutRunId += 1;
        this.forceClearLayoutStatus();
        this.currentDragId = String(d.id);
        d.fx = d.x;
        d.fy = d.y;
        d.vx = 0;
        d.vy = 0;
        this.storePinnedNodePosition(d);
    }

    dragged(event, d) {
        if (this.currentDragId !== String(d.id)) return;
        d.fx = event.x;
        d.fy = event.y;
        d.x = event.x;
        d.y = event.y;
        d.vx = 0;
        d.vy = 0;
        this.storePinnedNodePosition(d);
        this.scheduleDragRender(d);
    }

    dragended(event, d) {
        this.currentDragId = null;
        this.storePinnedNodePosition(d);
        // Keep fx/fy set so a manually positioned node remains pinned across
        // future static layouts. Reloading the network clears all pins.
    }
}

// Export
if (typeof module !== 'undefined' && module.exports) {
    module.exports = D3Adapter;
} else {
    window.D3Adapter = D3Adapter;
}
