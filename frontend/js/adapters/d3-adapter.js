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
            this.svg.transition().duration(750).call(this.zoomBehavior.transform, d3.zoomIdentity);
            return;
        }

        const padding = 40;
        const width = this.width;
        const height = this.height;

        const dx = maxX - minX || 1; // Avoid divide by zero
        const dy = maxY - minY || 1;
        const x = (minX + maxX) / 2;
        const y = (minY + maxY) / 2;

        // Calculate scale to fit, capped at 2.0 to avoid excessive zoom on small graphs
        const scale = Math.min(2, 0.9 / Math.max(dx / width, dy / height));

        // Calculate translation to center the bounding box
        // transform = translate(tx, ty) scale(k)
        // We want the center of the bounding box (x, y) to be at the center of the SVG (width/2, height/2)
        // width/2 = x * scale + tx  =>  tx = width/2 - x * scale
        const tx = width / 2 - scale * x;
        const ty = height / 2 - scale * y;

        this.svg.transition().duration(750).call(
            this.zoomBehavior.transform,
            d3.zoomIdentity.translate(tx, ty).scale(scale)
        );
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
            .force("collide", d3.forceCollide().radius(15)) // Prevent overlap
            .force("center", d3.forceCenter(this.width / 2, this.height / 2));

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
        this.appState.on('graphUpdated', () => this.updateVisualization());
        this.appState.on('graphVisualsUpdated', () => this.updateNodeVisuals());
        this.appState.on('clusterExpanded', () => this.updateVisualization());
        this.appState.on('clusterCollapsed', () => this.updateVisualization());

        // Selection Mode Listeners
        this.appState.on('selectionModeChanged', (enabled) => {
            this.toggleSelectionMode(enabled);
        });
        this.appState.on('selectionUpdated', () => {
            this.updateSelectionVisuals();
        });
    }

    /**
     * Compute the visible graph based on state (expansion)
     * and update D3.
     */
    updateVisualization() {
        // Use GraphView to compute the aggregated view
        const { nodes, edges } = GraphView.compute(
            this.appState.graph,
            this.appState.expandedClusters
        );

        // Update viewGraph in AppState
        this.appState.setViewGraph(nodes, edges);

        this.render(nodes, edges);
    }

    render(nodes, edges) {
        // Filter out hidden nodes
        const visibleNodes = nodes.filter(n => !this.appState.hiddenNodes.has(n.id));

        // Filter edges connected to hidden nodes
        const visibleNodeIds = new Set(visibleNodes.map(n => n.id));
        const visibleEdges = edges.filter(e => visibleNodeIds.has(e.source) && visibleNodeIds.has(e.target));

        // PRESERVE POSITIONS: Copy x, y, vx, vy from old nodes to new nodes
        this.preserveNodePositions(visibleNodes);

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

        try {
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

            // Restart simulation
            this.simulation.nodes(visibleNodes).on("tick", () => this.ticked());
            this.simulation.force("link").links(visibleEdges);
            this.simulation.alpha(0.3).restart();

            // Update visuals (colors, pie charts) - AFTER simulation restart to ensure layout works
            try {
                this.updateNodeVisuals();
            } catch (visualErr) {
                console.error("Error updating node visuals:", visualErr);
            }

        } catch (err) {
            console.error("Error in D3Adapter.render:", err);
        }

        // Re-apply selection visuals if needed
        this.updateSelectionVisuals();
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

        visibleNodes.forEach(node => {
            const oldNode = oldNodes.get(node.id);
            if (oldNode) {
                // Case 1: Node exists - keep position
                node.x = oldNode.x;
                node.y = oldNode.y;
                node.vx = oldNode.vx;
                node.vy = oldNode.vy;
            } else {
                // Case 2: New Node (Expansion or Collapse)

                // A. Expansion: Node is a protein, check if its Cluster (NH_ID) was visible
                if (node.NH_ID && oldNodes.has(node.NH_ID)) {
                    const parent = oldNodes.get(node.NH_ID);
                    node.x = parent.x + (Math.random() - 0.5) * 10; // Tiny jitter
                    node.y = parent.y + (Math.random() - 0.5) * 10;
                    node.vx = parent.vx;
                    node.vy = parent.vy;
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

    getNodeRadius(d) {
        if (d._isCluster) {
            // Use d.size (aggregated size) for clusters
            // Ensure it's at least slightly larger than a protein (5)
            // Log scale is good for large variance
            return 7 + Math.log(d.size || 1) * 3;
        }
        return 5;
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
        // Use select() to propagate the new data from the group to the lines
        this.linkSelection.select(".visible-line")
            .attr("x1", d => d.source.x)
            .attr("y1", d => d.source.y)
            .attr("x2", d => d.target.x)
            .attr("y2", d => d.target.y);

        this.linkSelection.select(".hit-area")
            .attr("x1", d => d.source.x)
            .attr("y1", d => d.source.y)
            .attr("x2", d => d.target.x)
            .attr("y2", d => d.target.y);

        this.nodeSelection
            .attr("transform", d => `translate(${d.x},${d.y})`);
    }

    // Drag functions
    dragstarted(event, d) {
        if (!event.active) this.simulation.alphaTarget(0.3).restart();
        d.fx = d.x;
        d.fy = d.y;
    }

    dragged(event, d) {
        d.fx = event.x;
        d.fy = event.y;
    }

    dragended(event, d) {
        if (!event.active) this.simulation.alphaTarget(0);
        // d.fx = null; // Keep fixed position
        // d.fy = null;
    }
}

// Export
if (typeof module !== 'undefined' && module.exports) {
    module.exports = D3Adapter;
} else {
    window.D3Adapter = D3Adapter;
}
