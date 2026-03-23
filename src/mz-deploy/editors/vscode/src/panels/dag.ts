/**
 * DAG webview script — renders the project dependency graph as interactive SVG.
 *
 * ## Staged Pipeline
 *
 *     dagData + interactionState
 *         │
 *         ▼
 *     resolveScene()     ──► DagScene (positioned nodes/edges with visual properties)
 *         │
 *         ▼
 *     renderScene()      ──► RenderedDag (header HTML, SVG string, legend HTML)
 *         │
 *         ▼
 *     mountDag()         ──► DOM (sets innerHTML)
 *     attachInteraction() ──► event handlers
 *
 * Each phase transforms one intermediate type into the next. The entry point
 * `render()` wires the phases together and reads as a summary of the algorithm.
 *
 * ## Layout Algorithm
 *
 * 1. **Longest-path layering** — Topological BFS assigns each node to the
 *    deepest layer reachable from any root. Nodes in cycles (if any) are
 *    placed on an extra layer beyond the maximum.
 * 2. **Barycenter crossing minimization** — 4 alternating forward/backward
 *    passes reorder nodes within each layer by the average position of their
 *    neighbors, reducing edge crossings.
 * 3. **Coordinate assignment** — Layers are spaced vertically; nodes within a
 *    layer are centered horizontally with configurable padding.
 *
 * ## Interaction Model
 *
 * - **Hover** — Highlights the hovered node's full lineage (ancestors in
 *   cyan, descendants in pink). Suppressed while a focus is active.
 * - **Click** — Posts an `inspect-object` message to the extension host,
 *   which opens the object in the catalog sidebar.
 * - **Double-click** — Sets focus on the clicked node, highlighting its
 *   full lineage until dismissed or replaced.
 * - **Pan/Zoom** — Mouse drag on the SVG background pans; mouse wheel zooms
 *   (clamped to 0.3x–3x).
 * - **Focus** — The extension host can send a `focus` message to permanently
 *   highlight a node's lineage until new data arrives.
 *
 * ## Visual Encoding
 *
 * - **Schema colors** — `public`, `mz_internal`, and `mz_catalog` have
 *   stable, hardcoded color triples. All other schemas are assigned colors
 *   dynamically by hashing the schema name into a cycling palette, so the
 *   same schema always gets the same color regardless of render order.
 * - **Edges** — Cubic Bezier curves between node center-bottom and
 *   center-top. Lineage edges are dashed and colored by direction.
 * - **Badges** — Upstream (↑) / downstream (↓) arrows on lineage nodes;
 *   "ext" label for external objects.
 * - **Dimming** — Nodes and edges outside the active lineage fade to near-
 *   invisible, keeping focus on the relevant subgraph.
 */

declare function acquireVsCodeApi(): { postMessage(msg: unknown): void };

(function () {

  // ---------------------------------------------------------------------------
  // Types
  // ---------------------------------------------------------------------------

  interface DagNode {
    id: string;
    name: string;
    schema: string;
    is_external?: boolean;
  }

  interface DagEdge {
    source: string;
    target: string;
  }

  interface DagData {
    objects: DagNode[];
    edges: DagEdge[];
  }

  type DagInboundMessage =
    | { type: "dag-data"; data: DagData }
    | { type: "focus"; id: string };

  type DagOutboundMessage =
    | { type: "inspect-object"; id: string }
    | { type: "ready" };

  interface SchemaColorTriple {
    fill: string;
    bg: string;
    border: string;
  }

  // --- Phase 1 intermediate: DagScene ---

  /** The active node (focused or hovered) whose lineage is highlighted. */
  interface LineageContext {
    activeId: string;
    ancestors: Set<string>;
    descendants: Set<string>;
    /** Union of activeId + ancestors + descendants. */
    all: Set<string>;
  }

  /** A node with its layout position and resolved visual properties. */
  interface PositionedNode {
    node: DagNode;
    x: number;
    y: number;
    colors: SchemaColorTriple;
    isFocused: boolean;
    isAncestor: boolean;
    isDescendant: boolean;
    inLineage: boolean;
    dimmed: boolean;
  }

  /** An edge with its endpoint coordinates and resolved visual properties. */
  interface PositionedEdge {
    edge: DagEdge;
    sourceX: number;
    sourceY: number;
    targetX: number;
    targetY: number;
    inLineage: boolean;
    isUpstream: boolean;
    isDownstream: boolean;
    dimmed: boolean;
  }

  /** Complete resolved scene ready for rendering. */
  interface DagScene {
    nodes: PositionedNode[];
    edges: PositionedEdge[];
    lineage: LineageContext | null;
    canvasWidth: number;
    canvasHeight: number;
    schemas: string[];
  }

  /** HTML/SVG strings produced by the render phase. */
  interface RenderedDag {
    headerHtml: string;
    svgContent: string;
    legendHtml: string;
  }

  // ---------------------------------------------------------------------------
  // Mutable State
  // ---------------------------------------------------------------------------

  const vscode = acquireVsCodeApi();

  let dagData: DagData | null = null;
  let focusId: string | null = null;
  let hoveredId: string | null = null;
  let pan = { x: 0, y: 0 };
  let zoom = 1;
  let dragging = false;
  let dragStart = { x: 0, y: 0 };
  let panStart = { x: 0, y: 0 };

  // ---------------------------------------------------------------------------
  // Constants
  // ---------------------------------------------------------------------------

  const NODE_WIDTH = 150;
  const NODE_HEIGHT = 44;
  const HORIZONTAL_PAD = 70;
  const VERTICAL_PAD = 24;

  /**
   * Static color assignments for well-known schemas. `public` is the most
   * common user schema; `mz_internal` and `mz_catalog` are Materialize system
   * schemas. All other schemas are colored dynamically via {@link schemaColor}.
   */
  const STATIC_SCHEMA_COLORS: Record<string, SchemaColorTriple> = {
    public:      { fill: "#C9962A", bg: "#2A2210", border: "#4A3A18" },
    mz_internal: { fill: "#8B8FA3", bg: "#1A1B22", border: "#2E303A" },
    mz_catalog:  { fill: "#6A8A3A", bg: "#1A2210", border: "#2E3A18" },
  };

  /**
   * Cycling palette for dynamically assigned schemas. Each entry is a
   * fill/background/border triple chosen for legibility on a dark background
   * and visual distinctness from the static colors above.
   */
  const PALETTE: SchemaColorTriple[] = [
    { fill: "#22D3EE", bg: "#0E2A32", border: "#1A5568" },
    { fill: "#F472B6", bg: "#3A1230", border: "#6A2250" },
    { fill: "#A78BFA", bg: "#1E1638", border: "#3A2A60" },
    { fill: "#FB923C", bg: "#2A1A0E", border: "#4A3018" },
    { fill: "#34D399", bg: "#0E2A1E", border: "#1A5540" },
    { fill: "#F87171", bg: "#2A1010", border: "#5A2020" },
    { fill: "#60A5FA", bg: "#0E1A2A", border: "#1A3558" },
    { fill: "#FBBF24", bg: "#2A2210", border: "#4A3A18" },
  ];

  // ---------------------------------------------------------------------------
  // Utilities
  // ---------------------------------------------------------------------------

  /**
   * Returns the color triple for a schema name. Static schemas get a fixed
   * assignment; all others are hashed (sum of char codes mod palette length)
   * so the same name always maps to the same color.
   */
  function schemaColor(schema: string): SchemaColorTriple {
    if (STATIC_SCHEMA_COLORS[schema]) return STATIC_SCHEMA_COLORS[schema];
    let hash = 0;
    for (let i = 0; i < schema.length; i++) hash += schema.charCodeAt(i);
    return PALETTE[hash % PALETTE.length];
  }

  /** Escapes `&`, `<`, `>` for safe insertion into SVG text elements. */
  function escapeHtml(str: string): string {
    return str.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  }

  /**
   * Returns the set of all transitive ancestors of `id` (upstream nodes).
   * BFS traversal following edges in reverse (target → source).
   */
  function getAncestors(id: string, edges: DagEdge[]): Set<string> {
    const visited = new Set<string>();
    const queue: string[] = [id];
    while (queue.length) {
      const current = queue.shift()!;
      for (const edge of edges) {
        if (edge.target === current && !visited.has(edge.source)) {
          visited.add(edge.source);
          queue.push(edge.source);
        }
      }
    }
    return visited;
  }

  /**
   * Returns the set of all transitive descendants of `id` (downstream nodes).
   * BFS traversal following edges forward (source → target).
   */
  function getDescendants(id: string, edges: DagEdge[]): Set<string> {
    const visited = new Set<string>();
    const queue: string[] = [id];
    while (queue.length) {
      const current = queue.shift()!;
      for (const edge of edges) {
        if (edge.source === current && !visited.has(edge.target)) {
          visited.add(edge.target);
          queue.push(edge.target);
        }
      }
    }
    return visited;
  }

  // ---------------------------------------------------------------------------
  // Layout
  // ---------------------------------------------------------------------------

  /** Internal layout result: per-node positions and graph structure. */
  interface LayoutResult {
    x: number[];
    y: number[];
    canvasWidth: number;
    canvasHeight: number;
    nodeIndex: Record<string, number>;
  }

  /**
   * Computes a layered graph layout for the given nodes and edges.
   *
   * Three phases:
   * 1. Longest-path layering via topological BFS
   * 2. Barycenter crossing minimization (4 alternating passes)
   * 3. Coordinate assignment with centered layers
   */
  function computeLayout(nodes: DagNode[], edges: DagEdge[]): LayoutResult {
    // Build node index and adjacency lists
    const nodeIndex: Record<string, number> = {};
    nodes.forEach((node, idx) => { nodeIndex[node.id] = idx; });

    const successors: number[][] = nodes.map(() => []);
    const predecessors: number[][] = nodes.map(() => []);
    const inDegree: number[] = nodes.map(() => 0);

    for (const edge of edges) {
      const sourceIdx = nodeIndex[edge.source];
      const targetIdx = nodeIndex[edge.target];
      if (sourceIdx !== undefined && targetIdx !== undefined) {
        successors[sourceIdx].push(targetIdx);
        predecessors[targetIdx].push(sourceIdx);
        inDegree[targetIdx]++;
      }
    }

    // Phase 1: Longest-path layering
    const layerAssignment: number[] = nodes.map(() => 0);
    const visited = new Set<number>();
    const queue: number[] = [];
    for (let i = 0; i < nodes.length; i++) {
      if (inDegree[i] === 0) queue.push(i);
    }
    const remainingInDegree = [...inDegree];
    while (queue.length > 0) {
      const current = queue.shift()!;
      visited.add(current);
      for (const neighbor of successors[current]) {
        layerAssignment[neighbor] = Math.max(
          layerAssignment[neighbor],
          layerAssignment[current] + 1
        );
        if (--remainingInDegree[neighbor] === 0) queue.push(neighbor);
      }
    }
    // Place cycle nodes (if any) on an extra layer
    const maxLayer = Math.max(0, ...layerAssignment);
    for (let i = 0; i < nodes.length; i++) {
      if (!visited.has(i)) layerAssignment[i] = maxLayer + 1;
    }

    // Group nodes by layer
    const layerCount = Math.max(...layerAssignment) + 1;
    const layers: number[][] = Array.from({ length: layerCount }, () => []);
    nodes.forEach((_, idx) => { layers[layerAssignment[idx]].push(idx); });

    // Phase 2: Barycenter crossing minimization (4 alternating passes)
    const barycenter: number[] = nodes.map(() => 0);
    layers.forEach((layerNodes) => {
      layerNodes.forEach((nodeIdx, position) => { barycenter[nodeIdx] = position; });
    });

    for (let pass = 0; pass < 4; pass++) {
      // Forward sweep: order each layer by predecessor barycenters
      for (let layerNum = 1; layerNum < layerCount; layerNum++) {
        for (const nodeIdx of layers[layerNum]) {
          const preds = predecessors[nodeIdx];
          if (preds.length > 0) {
            barycenter[nodeIdx] =
              preds.reduce((sum, pred) => sum + barycenter[pred], 0) / preds.length;
          }
        }
        layers[layerNum].sort((a, b) => barycenter[a] - barycenter[b]);
        layers[layerNum].forEach((nodeIdx, position) => { barycenter[nodeIdx] = position; });
      }
      // Backward sweep: order each layer by successor barycenters
      for (let layerNum = layerCount - 2; layerNum >= 0; layerNum--) {
        for (const nodeIdx of layers[layerNum]) {
          const succs = successors[nodeIdx];
          if (succs.length > 0) {
            barycenter[nodeIdx] =
              succs.reduce((sum, succ) => sum + barycenter[succ], 0) / succs.length;
          }
        }
        layers[layerNum].sort((a, b) => barycenter[a] - barycenter[b]);
        layers[layerNum].forEach((nodeIdx, position) => { barycenter[nodeIdx] = position; });
      }
    }

    // Phase 3: Coordinate assignment
    const x: number[] = nodes.map(() => 0);
    const y: number[] = nodes.map(() => 0);
    const maxLayerWidth = Math.max(...layers.map((layerNodes) => layerNodes.length));

    layers.forEach((layerNodes, layerIdx) => {
      const centeringOffset =
        ((maxLayerWidth - layerNodes.length) * (NODE_WIDTH + HORIZONTAL_PAD)) / 2;
      layerNodes.forEach((nodeIdx, position) => {
        x[nodeIdx] = centeringOffset + position * (NODE_WIDTH + HORIZONTAL_PAD) + 50;
        y[nodeIdx] = layerIdx * (NODE_HEIGHT + VERTICAL_PAD) + 60;
      });
    });

    const canvasWidth = maxLayerWidth * (NODE_WIDTH + HORIZONTAL_PAD) + 100;
    const canvasHeight = layerCount * (NODE_HEIGHT + VERTICAL_PAD) + 100;

    return { x, y, canvasWidth, canvasHeight, nodeIndex };
  }

  /**
   * Resolves raw DAG data and interaction state into a fully positioned and
   * visually classified scene. Pure computation — no DOM access.
   */
  function resolveScene(
    data: DagData,
    currentFocusId: string | null,
    currentHoveredId: string | null,
  ): DagScene {
    const { objects: nodes, edges } = data;
    const layout = computeLayout(nodes, edges);

    // Compute lineage for the active node (focused takes priority over hovered)
    const activeId = currentFocusId || currentHoveredId;
    let lineage: LineageContext | null = null;
    if (activeId) {
      const ancestors = getAncestors(activeId, edges);
      const descendants = getDescendants(activeId, edges);
      lineage = {
        activeId,
        ancestors,
        descendants,
        all: new Set([activeId, ...ancestors, ...descendants]),
      };
    }

    // Resolve positioned nodes
    const positionedNodes: PositionedNode[] = nodes.map((node, idx) => {
      const isAncestor = lineage ? lineage.ancestors.has(node.id) : false;
      const isDescendant = lineage ? lineage.descendants.has(node.id) : false;
      const inLineage = lineage ? lineage.all.has(node.id) : true;
      return {
        node,
        x: layout.x[idx],
        y: layout.y[idx],
        colors: schemaColor(node.schema),
        isFocused: activeId === node.id,
        isAncestor,
        isDescendant,
        inLineage,
        dimmed: lineage !== null && !inLineage,
      };
    });

    // Resolve positioned edges
    const positionedEdges: PositionedEdge[] = [];
    for (const edge of edges) {
      const sourceIdx = layout.nodeIndex[edge.source];
      const targetIdx = layout.nodeIndex[edge.target];
      if (sourceIdx === undefined || targetIdx === undefined) continue;

      const bothInLineage = lineage !== null
        && lineage.all.has(edge.source)
        && lineage.all.has(edge.target);

      positionedEdges.push({
        edge,
        sourceX: layout.x[sourceIdx] + NODE_WIDTH / 2,
        sourceY: layout.y[sourceIdx] + NODE_HEIGHT,
        targetX: layout.x[targetIdx] + NODE_WIDTH / 2,
        targetY: layout.y[targetIdx],
        inLineage: bothInLineage,
        isUpstream: lineage !== null
          && lineage.ancestors.has(edge.source)
          && (lineage.ancestors.has(edge.target) || edge.target === activeId),
        isDownstream: lineage !== null
          && lineage.descendants.has(edge.target)
          && (lineage.descendants.has(edge.source) || edge.source === activeId),
        dimmed: lineage !== null && !bothInLineage,
      });
    }

    const schemas = [...new Set(nodes.map((n) => n.schema))];

    return {
      nodes: positionedNodes,
      edges: positionedEdges,
      lineage,
      canvasWidth: layout.canvasWidth,
      canvasHeight: layout.canvasHeight,
      schemas,
    };
  }

  // ---------------------------------------------------------------------------
  // Phase 2: Render
  // ---------------------------------------------------------------------------

  /** Renders the header bar with title, focus badge, stats, and reset button. */
  function renderHeader(
    nodeCount: number,
    edgeCount: number,
    currentFocusId: string | null,
  ): string {
    const focusBadge = currentFocusId
      ? `<span class="dag-focus-badge">Focused: ${currentFocusId} <span class="dag-focus-dismiss" title="Clear focus">\u00d7</span></span>`
      : "";

    return `
      <div class="dag-header-inner">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="#5B9BD5" stroke-width="2" stroke-linecap="round">
          <circle cx="5" cy="6" r="3"/><circle cx="19" cy="6" r="3"/><circle cx="12" cy="18" r="3"/>
          <line x1="7.5" y1="7.5" x2="10.5" y2="15.5"/><line x1="16.5" y1="7.5" x2="13.5" y2="15.5"/>
        </svg>
        <span class="dag-title">Schema DAG</span>
        ${focusBadge}
        <span class="dag-stats">${nodeCount} nodes &middot; ${edgeCount} edges</span>
        <span class="dag-reset-btn" title="Reset view">
          <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round"><path d="M15 3h6v6"/><path d="M9 21H3v-6"/><path d="M21 3l-7 7"/><path d="M3 21l7-7"/></svg>
        </span>
      </div>
    `;
  }

  /** Renders a single edge as a cubic Bezier path with lineage-aware coloring. */
  function renderEdgePath(edge: PositionedEdge): string {
    const controlY1 = edge.sourceY + (edge.targetY - edge.sourceY) * 0.4;
    const controlY2 = edge.sourceY + (edge.targetY - edge.sourceY) * 0.6;

    let color = "#252730";
    if (edge.isUpstream) color = "#22D3EE77";
    else if (edge.isDownstream) color = "#F472B677";
    else if (edge.inLineage) color = "#5B9BD555";
    if (edge.dimmed) color = "#111218";

    const strokeWidth = edge.inLineage ? 2.5 : 1.2;
    const dashAttr = edge.inLineage ? 'stroke-dasharray="5 3"' : "";

    return `<path d="M ${edge.sourceX} ${edge.sourceY} C ${edge.sourceX} ${controlY1}, ${edge.targetX} ${controlY2}, ${edge.targetX} ${edge.targetY}" fill="none" stroke="${color}" stroke-width="${strokeWidth}" ${dashAttr}/>`;
  }

  /** Renders a single node as a group of SVG elements (rect, labels, badges). */
  function renderNodeGroup(pNode: PositionedNode, lineage: LineageContext | null): string {
    const { node, x, y, colors, isFocused, isAncestor, isDescendant, inLineage, dimmed } = pNode;
    const opacity = dimmed ? 0.3 : 1;
    let svg = "";

    // Highlight ring
    if (isFocused) {
      svg += `<rect x="${x - 3}" y="${y - 3}" width="${NODE_WIDTH + 6}" height="${NODE_HEIGHT + 6}" rx="9" fill="none" stroke="${colors.fill}" stroke-width="2" opacity="0.7"/>`;
    } else if (inLineage && lineage) {
      const ringColor = isAncestor ? "#22D3EE" : "#F472B6";
      svg += `<rect x="${x - 2}" y="${y - 2}" width="${NODE_WIDTH + 4}" height="${NODE_HEIGHT + 4}" rx="8" fill="none" stroke="${ringColor}" stroke-width="1" opacity="0.35"/>`;
    }

    // Node body
    const bodyFill = dimmed ? "#0E0F14" : colors.bg;
    const bodyStroke = dimmed ? "#151720" : isFocused ? colors.fill : colors.border;
    const strokeWidth = isFocused ? 1.5 : 1;
    svg += `<rect class="dag-node" data-id="${node.id}" x="${x}" y="${y}" width="${NODE_WIDTH}" height="${NODE_HEIGHT}" rx="7" fill="${bodyFill}" stroke="${bodyStroke}" stroke-width="${strokeWidth}" opacity="${opacity}"/>`;

    // Name label
    const nameColor = dimmed ? "#252730" : isFocused ? "#E8E0D0" : "#8A8E9A";
    const nameWeight = isFocused ? 600 : 400;
    svg += `<text class="dag-node-label" data-id="${node.id}" x="${x + 12}" y="${y + NODE_HEIGHT / 2 - 3}" font-size="11" font-weight="${nameWeight}" fill="${nameColor}" dominant-baseline="middle">${escapeHtml(node.name)}</text>`;

    // Schema label
    const schemaLabelColor = dimmed ? "#1A1C22" : colors.fill + "88";
    svg += `<text data-id="${node.id}" x="${x + 12}" y="${y + NODE_HEIGHT / 2 + 11}" font-size="9" font-weight="600" fill="${schemaLabelColor}" dominant-baseline="middle">${escapeHtml(node.schema)}</text>`;

    // External badge
    if (node.is_external) {
      svg += `<text data-id="${node.id}" x="${x + NODE_WIDTH - 10}" y="${y + 12}" font-size="8" font-weight="700" fill="#4A4D58" text-anchor="end" dominant-baseline="middle">ext</text>`;
    }

    // Upstream/downstream badge
    if (lineage && inLineage && !isFocused) {
      const badgeColor = isAncestor ? "#22D3EE" : "#F472B6";
      const badgeLabel = isAncestor ? "\u2191" : "\u2193";
      svg += `<circle cx="${x + NODE_WIDTH - 8}" cy="${y + 4}" r="6" fill="${badgeColor}"/>`;
      svg += `<text x="${x + NODE_WIDTH - 8}" y="${y + 5}" font-size="8" font-weight="700" fill="#000" text-anchor="middle" dominant-baseline="middle">${badgeLabel}</text>`;
    }

    return svg;
  }

  /** Renders the legend showing schema colors and lineage direction indicators. */
  function renderLegend(schemas: string[]): string {
    const swatches = schemas
      .map((schema) => {
        const colors = schemaColor(schema);
        return `<span class="legend-item"><span class="legend-swatch" style="background:${colors.bg};border-color:${colors.border}"></span>${escapeHtml(schema)}</span>`;
      })
      .join("");

    return `
      <div class="dag-legend-inner">
        ${swatches}
        <span class="legend-sep">|</span>
        <span class="legend-item"><span class="legend-dot" style="background:#22D3EE"></span>upstream</span>
        <span class="legend-item"><span class="legend-dot" style="background:#F472B6"></span>downstream</span>
        <span class="legend-hint">Click a node to inspect in catalog</span>
      </div>
    `;
  }

  /**
   * Produces HTML/SVG strings from a resolved scene. Pure string building —
   * no DOM access, no side effects.
   */
  function renderScene(scene: DagScene, currentFocusId: string | null): RenderedDag {
    const headerHtml = renderHeader(scene.nodes.length, scene.edges.length, currentFocusId);

    let svgContent = `<svg width="${scene.canvasWidth}" height="${scene.canvasHeight}" class="dag-svg">`;
    for (const edge of scene.edges) {
      svgContent += renderEdgePath(edge);
    }
    for (const node of scene.nodes) {
      svgContent += renderNodeGroup(node, scene.lineage);
    }
    svgContent += "</svg>";

    const legendHtml = renderLegend(scene.schemas);

    return { headerHtml, svgContent, legendHtml };
  }

  /** Writes rendered HTML/SVG into the DOM. */
  function mountDag(rendered: RenderedDag): void {
    document.getElementById("dag-header")!.innerHTML = rendered.headerHtml;
    document.getElementById("dag-canvas")!.innerHTML = rendered.svgContent;
    document.getElementById("dag-legend")!.innerHTML = rendered.legendHtml;
  }

  /** Shows the empty state when no data is available. */
  function showEmptyState(): void {
    document.getElementById("dag-header")!.innerHTML = "";
    document.getElementById("dag-canvas")!.innerHTML =
      '<div class="empty-state">Waiting for DAG data...</div>';
    document.getElementById("dag-legend")!.innerHTML = "";
  }

  /**
   * Attaches DOM event handlers to the rendered SVG for click-to-inspect,
   * double-click-to-focus, hover lineage highlighting, mouse-drag panning,
   * and scroll-wheel zooming.
   */
  function attachInteraction(): void {
    const canvas = document.getElementById("dag-canvas")!;
    const svgEl = canvas.querySelector("svg") as SVGSVGElement | null;
    if (!svgEl) return;

    // Click to inspect in catalog
    svgEl.querySelectorAll("[data-id]").forEach((el) => {
      el.addEventListener("click", () => {
        const id = el.getAttribute("data-id");
        if (id) {
          vscode.postMessage({ type: "inspect-object", id } satisfies DagOutboundMessage);
        }
      });

      el.addEventListener("mouseenter", () => {
        if (!focusId) {
          const id = el.getAttribute("data-id");
          if (hoveredId !== id) {
            hoveredId = id;
            render();
          }
        }
      });

      el.addEventListener("mouseleave", () => {
        if (!focusId && hoveredId === el.getAttribute("data-id")) {
          hoveredId = null;
          render();
        }
      });
    });

    // Double-click on node rects only to set focus
    svgEl.querySelectorAll(".dag-node").forEach((el) => {
      el.addEventListener("dblclick", () => {
        const id = el.getAttribute("data-id");
        if (id) {
          focusId = id;
          hoveredId = null;
          render();
        }
      });
    });

    // Pan via mouse drag
    svgEl.addEventListener("mousedown", (e: MouseEvent) => {
      if (e.target === svgEl || (e.target as Element).tagName === "path") {
        dragging = true;
        dragStart = { x: e.clientX, y: e.clientY };
        panStart = { ...pan };
        svgEl.style.cursor = "grabbing";
      }
    });

    window.addEventListener("mousemove", (e: MouseEvent) => {
      if (dragging) {
        pan.x = panStart.x + (e.clientX - dragStart.x);
        pan.y = panStart.y + (e.clientY - dragStart.y);
        applyTransform(svgEl);
      }
    });

    window.addEventListener("mouseup", () => {
      if (dragging) {
        dragging = false;
        if (svgEl) svgEl.style.cursor = "grab";
      }
    });

    // Zoom via scroll wheel
    canvas.addEventListener("wheel", (e: WheelEvent) => {
      e.preventDefault();
      const factor = e.deltaY > 0 ? 0.97 : 1.03;
      zoom = Math.max(0.3, Math.min(3, zoom * factor));
      applyTransform(svgEl);
    });

    applyTransform(svgEl);
    svgEl.style.cursor = "grab";

    // Focus dismiss button
    const dismissBtn = document.querySelector(".dag-focus-dismiss");
    if (dismissBtn) {
      dismissBtn.addEventListener("click", () => {
        focusId = null;
        render();
      });
    }

    // View reset button
    const resetBtn = document.querySelector(".dag-reset-btn");
    if (resetBtn) {
      resetBtn.addEventListener("click", () => {
        pan = { x: 0, y: 0 };
        zoom = 1;
        const currentSvg = document.querySelector(".dag-svg") as SVGSVGElement | null;
        if (currentSvg) applyTransform(currentSvg);
      });
    }
  }

  /** Applies the current pan offset and zoom level as a CSS transform. */
  function applyTransform(svgEl: SVGSVGElement | null): void {
    if (!svgEl) return;
    svgEl.style.transform = `translate(${pan.x}px, ${pan.y}px) scale(${zoom})`;
    svgEl.style.transformOrigin = "0 0";
  }

  /**
   * Top-level render pipeline. Wires together the three phases:
   * resolve → render → mount + attach.
   */
  function render(): void {
    if (!dagData || !dagData.objects || dagData.objects.length === 0) {
      showEmptyState();
      return;
    }
    const scene = resolveScene(dagData, focusId, hoveredId);
    const rendered = renderScene(scene, focusId);
    mountDag(rendered);
    attachInteraction();
  }

  // ---------------------------------------------------------------------------
  // Message Handling
  // ---------------------------------------------------------------------------

  window.addEventListener("message", (event: MessageEvent) => {
    const msg = event.data as DagInboundMessage;
    switch (msg.type) {
      case "dag-data":
        dagData = msg.data;
        render();
        break;
      case "focus":
        focusId = msg.id;
        hoveredId = null;
        render();
        break;
    }
  });

  // Signal ready to receive data from the extension host
  vscode.postMessage({ type: "ready" } satisfies DagOutboundMessage);
})();
