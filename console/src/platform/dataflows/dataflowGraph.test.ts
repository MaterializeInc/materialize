// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { describe, expect, it } from "vitest";

import {
  allChannelTypes,
  buildDataflowStructure,
  type ChannelRow,
  decorateGraph,
  DEFAULT_FILTERS,
  defaultCollapseState,
  deriveVisibleGraph,
  expandAncestorsOf,
  expandForSearch,
  lirIndex,
  type LirSpanRow,
  lirTree,
  MAX_VISIBLE_NODES,
  nodeIdOf,
  type OperatorRow,
  rerouteHiddenNodes,
  type VisibleEdge,
  type VisibleNode,
  visibleNodeCount,
} from "./dataflowGraph";

// Dataflow 5: root [5], region [5,1] with children [5,1,1], [5,1,2], leaf [5,2].
export const OPS: OperatorRow[] = [
  {
    id: "10",
    address: ["5"],
    name: "Dataflow",
    arrangementRecords: null,
    arrangementSize: null,
    elapsedNs: "0",
  },
  {
    id: "11",
    address: ["5", "1"],
    name: "Region",
    arrangementRecords: "0",
    arrangementSize: "0",
    elapsedNs: "5",
  },
  {
    id: "12",
    address: ["5", "1", "1"],
    name: "Join",
    arrangementRecords: "100",
    arrangementSize: "4096",
    elapsedNs: "7",
  },
  {
    id: "13",
    address: ["5", "1", "2"],
    name: "Map",
    arrangementRecords: "0",
    arrangementSize: "0",
    elapsedNs: "1",
  },
  {
    id: "14",
    address: ["5", "2"],
    name: "Sink",
    arrangementRecords: "0",
    arrangementSize: "0",
    elapsedNs: "2",
  },
];
export const CHANNELS: ChannelRow[] = [
  // into the region: leaf [5,2] <- region output; region input port -> child
  {
    id: "1",
    fromOperatorAddress: ["5", "1", "0"],
    fromPort: "0",
    toOperatorAddress: ["5", "1", "1"],
    toPort: "0",
    messagesSent: "3",
    batchesSent: "1",
    channelType: "rows",
  },
  {
    id: "2",
    fromOperatorAddress: ["5", "1", "1"],
    fromPort: "0",
    toOperatorAddress: ["5", "1", "2"],
    toPort: "0",
    messagesSent: "5",
    batchesSent: "2",
    channelType: "rows",
  },
  {
    id: "3",
    fromOperatorAddress: ["5", "1"],
    fromPort: "0",
    toOperatorAddress: ["5", "2"],
    toPort: "0",
    messagesSent: "5",
    batchesSent: "2",
    channelType: "batches",
  },
];
export const LIR_SPANS: LirSpanRow[] = [
  {
    exportId: "u42",
    lirId: "1",
    parentLirId: null,
    nesting: 0,
    operator: "Join::Differential",
    operatorIdStart: "11",
    operatorIdEnd: "13",
  },
];

describe("buildDataflowStructure", () => {
  it("builds the region tree from address prefixes", () => {
    const s = buildDataflowStructure(OPS, CHANNELS, LIR_SPANS);
    expect(s.root).toEqual(nodeIdOf([5]));
    const root = s.nodes.get(s.root)!;
    expect(root.children).toEqual([nodeIdOf([5, 1]), nodeIdOf([5, 2])]);
    const region = s.nodes.get(nodeIdOf([5, 1]))!;
    expect(region.parent).toEqual(s.root);
    expect(region.children).toEqual([nodeIdOf([5, 1, 1]), nodeIdOf([5, 1, 2])]);
    expect(region.operatorId).toEqual(11n);
  });

  it("precomputes transitive stats", () => {
    const s = buildDataflowStructure(OPS, CHANNELS, LIR_SPANS);
    const region = s.nodes.get(nodeIdOf([5, 1]))!;
    expect(region.own.elapsedNs).toEqual(5n);
    expect(region.transitive.elapsedNs).toEqual(13n);
    expect(region.transitive.arrangementRecords).toEqual(100n);
    expect(region.transitive.arrangementSize).toEqual(4096n);
  });

  it("maps LIR spans to operators by id range, as an array", () => {
    const s = buildDataflowStructure(OPS, CHANNELS, LIR_SPANS);
    expect(s.nodes.get(nodeIdOf([5, 1, 1]))!.lir).toEqual([
      {
        exportId: "u42",
        lirId: "1",
        parentLirId: null,
        nesting: 0,
        operator: "Join::Differential",
      },
    ]);
    // end is exclusive
    expect(s.nodes.get(nodeIdOf([5, 2]))!.lir).toEqual([]);
  });

  it("throws without exactly one root", () => {
    expect(() => buildDataflowStructure([], [], [])).toThrow();
  });

  it("normalizes channels", () => {
    const s = buildDataflowStructure(OPS, CHANNELS, []);
    expect(s.channels[0]).toEqual({
      id: 1,
      fromAddress: [5, 1, 0],
      fromPort: 0,
      toAddress: [5, 1, 1],
      toPort: 0,
      messagesSent: 3n,
      batchesSent: 1n,
      channelType: "rows",
    });
  });
});

describe("deriveVisibleGraph", () => {
  const s = buildDataflowStructure(OPS, CHANNELS, LIR_SPANS);
  const regionId = nodeIdOf([5, 1]);

  it("collapses regions to a single node with remapped edges", () => {
    const g = deriveVisibleGraph(s, new Set([regionId]));
    expect(g.nodes.map((n) => [n.id, n.kind])).toEqual([
      [regionId, "collapsedRegion"],
      [nodeIdOf([5, 2]), "operator"],
    ]);
    // channel 3 region -> sink survives, channels 1 and 2 are internal
    expect(g.edges).toEqual([
      {
        id: `${regionId}=>${nodeIdOf([5, 2])}`,
        source: regionId,
        target: nodeIdOf([5, 2]),
        messagesSent: 5n,
        batchesSent: 2n,
        channelTypes: ["batches"],
      },
    ]);
    const collapsed = g.nodes[0];
    expect(collapsed.stats).toEqual(s.nodes.get(regionId)!.transitive);
    expect(collapsed.childCount).toEqual(2);
  });

  it("expands regions with port pseudo-nodes, parents before children", () => {
    const g = deriveVisibleGraph(s, new Set());
    const ids = g.nodes.map((n) => n.id);
    expect(ids.indexOf(regionId)).toBeLessThan(
      ids.indexOf(nodeIdOf([5, 1, 1])),
    );
    const port = g.nodes.find((n) => n.kind === "port")!;
    expect(port.id).toEqual(`${regionId}:in:0`);
    expect(port.parent).toEqual(regionId);
    expect(port.label).toEqual("input 0");
    expect(port.operatorId).toBeNull();
    // port -> Join edge preserved
    expect(g.edges.map((e) => e.id)).toContain(
      `${regionId}:in:0=>${nodeIdOf([5, 1, 1])}`,
    );
    const regionNode = g.nodes.find((n) => n.id === regionId)!;
    expect(regionNode.operatorId).toEqual(11n);
  });

  it("aggregates parallel channels between the same visible pair", () => {
    const extra = buildDataflowStructure(
      OPS,
      [
        ...CHANNELS,
        {
          id: "4",
          fromOperatorAddress: ["5", "1"],
          fromPort: "1",
          toOperatorAddress: ["5", "2"],
          toPort: "1",
          messagesSent: "7",
          batchesSent: "1",
          channelType: "rows",
        },
      ],
      [],
    );
    const g = deriveVisibleGraph(extra, new Set([regionId]));
    const e = g.edges.find((edge) => edge.target === nodeIdOf([5, 2]))!;
    expect(e.messagesSent).toEqual(12n);
    expect(e.channelTypes).toEqual(["batches", "rows"]);
  });

  it("defaultCollapseState collapses all non-root regions", () => {
    expect(defaultCollapseState(s)).toEqual(new Set([regionId]));
    expect(visibleNodeCount(s, defaultCollapseState(s))).toEqual(2);
    expect(MAX_VISIBLE_NODES).toEqual(1500);
  });

  it("routes an external-to-region channel through a port node, keyed by port number", () => {
    // Materialize logs a scope boundary crossing as two channels sharing a
    // port number but addressed differently: the outer half (external
    // producer/consumer <-> scope) targets the scope's OWN operator address
    // directly (no [..., 0] suffix), the same address the region's own
    // operator row uses. Verified against live introspection data: an
    // external channel `{5,4}->{5,2}` (to_port=0) pairs with the internal
    // fan-out channel `{5,2,0}->{5,2,1}` (from_port=0).
    const withExternal = buildDataflowStructure(
      [
        ...OPS,
        {
          id: "20",
          address: ["5", "3"],
          name: "External",
          arrangementRecords: "0",
          arrangementSize: "0",
          elapsedNs: "1",
        },
      ],
      [
        ...CHANNELS,
        {
          id: "98",
          fromOperatorAddress: ["5", "3"],
          fromPort: "0",
          toOperatorAddress: ["5", "1"],
          toPort: "0",
          messagesSent: "9",
          batchesSent: "3",
          channelType: "rows",
        },
      ],
      LIR_SPANS,
    );
    const externalId = nodeIdOf([5, 3]);
    const portId = `${regionId}:in:0`;

    const expanded = deriveVisibleGraph(withExternal, new Set());
    expect(
      expanded.nodes.some((n) => n.id === portId && n.kind === "port"),
    ).toBe(true);
    const edge = expanded.edges.find((e) => e.source === externalId)!;
    expect(edge.target).toEqual(portId);

    // Collapsed, the crossing lands on the region's own (collapsed) node,
    // matching how every other boundary crossing into a collapsed region
    // behaves: the whole region is one box.
    const collapsed = deriveVisibleGraph(withExternal, new Set([regionId]));
    const collapsedEdge = collapsed.edges.find((e) => e.source === externalId)!;
    expect(collapsedEdge.target).toEqual(regionId);
  });

  it("drops channels that reference an address with no operator row", () => {
    // Real dataflows can log a channel touching a scope address that never
    // got its own operator row (e.g. an elided region). [5, 5] does not
    // appear in OPS.
    const withDangling = buildDataflowStructure(
      OPS,
      [
        ...CHANNELS,
        {
          id: "99",
          fromOperatorAddress: ["5", "5"],
          fromPort: "0",
          toOperatorAddress: ["5", "2"],
          toPort: "0",
          messagesSent: "1",
          batchesSent: "1",
          channelType: "rows",
        },
      ],
      LIR_SPANS,
    );
    expect(() => deriveVisibleGraph(withDangling, new Set())).not.toThrow();
    const g = deriveVisibleGraph(withDangling, new Set());
    const nodeIds = new Set(g.nodes.map((n) => n.id));
    for (const e of g.edges) {
      expect(nodeIds.has(e.source)).toBe(true);
      expect(nodeIds.has(e.target)).toBe(true);
    }
  });
});

describe("decorateGraph", () => {
  const s = buildDataflowStructure(OPS, CHANNELS, LIR_SPANS);
  const expanded = deriveVisibleGraph(s, new Set());
  const heat = (t: number) => `heat(${t.toFixed(2)})`;

  it("search dims non-matching leaf nodes and lists matches", () => {
    const d = decorateGraph(
      expanded,
      { ...DEFAULT_FILTERS, search: "join" },
      heat,
    );
    expect(d.searchMatches).toEqual([nodeIdOf([5, 1, 1])]);
    expect(d.dimmedNodeIds.has(nodeIdOf([5, 2]))).toBe(true);
    expect(d.dimmedNodeIds.has(nodeIdOf([5, 1, 1]))).toBe(false);
  });

  it("hideIdle hides zero-activity operators and zero-message edges, never regions or ports", () => {
    const d = decorateGraph(
      expanded,
      { ...DEFAULT_FILTERS, hideIdle: true },
      heat,
    );
    // every fixture operator has elapsed > 0 except none; hide check via a synthetic idle op
    const idle = buildDataflowStructure(
      [
        ...OPS,
        {
          id: "15",
          address: ["5", "3"],
          name: "Idle",
          arrangementRecords: "0",
          arrangementSize: "0",
          elapsedNs: "0",
        },
      ],
      CHANNELS,
      [],
    );
    const d2 = decorateGraph(
      deriveVisibleGraph(idle, new Set()),
      { ...DEFAULT_FILTERS, hideIdle: true },
      heat,
    );
    expect(d2.hiddenNodeIds.has(nodeIdOf([5, 3]))).toBe(true);
    expect(d2.hiddenNodeIds.has(nodeIdOf([5, 1]))).toBe(false);
    expect(d.hiddenEdgeIds.size).toBe(0); // all fixture edges have messages
  });

  it("channel type filter dims non-matching edges", () => {
    const d = decorateGraph(
      expanded,
      { ...DEFAULT_FILTERS, channelTypes: ["batches"] },
      heat,
    );
    const rowsEdge = expanded.edges.find((e) =>
      e.channelTypes.includes("rows"),
    )!;
    const batchesEdge = expanded.edges.find((e) =>
      e.channelTypes.includes("batches"),
    )!;
    expect(d.dimmedEdgeIds.has(rowsEdge.id)).toBe(true);
    expect(d.dimmedEdgeIds.has(batchesEdge.id)).toBe(false);
  });

  it("heatmap colors by transitive metric and dims below threshold", () => {
    const d = decorateGraph(
      expanded,
      { ...DEFAULT_FILTERS, heatmap: "elapsed", heatmapThreshold: 0.5 },
      heat,
    );
    // max transitive elapsed among visible non-ports is the region (13n)
    expect(d.nodeColors.get(nodeIdOf([5, 1]))).toEqual("heat(1.00)");
    expect(d.dimmedNodeIds.has(nodeIdOf([5, 2]))).toBe(true); // 2/13 < 0.5
  });

  it("heatmap with all-zero metric sets no colors", () => {
    const zero = buildDataflowStructure(
      OPS.map((o) => ({ ...o, elapsedNs: "0" })),
      CHANNELS,
      [],
    );
    const d = decorateGraph(
      deriveVisibleGraph(zero, new Set()),
      { ...DEFAULT_FILTERS, heatmap: "elapsed" },
      heat,
    );
    expect(d.nodeColors.size).toBe(0);
  });
});

describe("expandForSearch / allChannelTypes", () => {
  const s = buildDataflowStructure(OPS, CHANNELS, LIR_SPANS);

  it("expands ancestors of matches", () => {
    const next = expandForSearch(s, defaultCollapseState(s), "join");
    expect(next.has(nodeIdOf([5, 1]))).toBe(false);
  });

  it("expandAncestorsOf expands ancestors of given addresses directly", () => {
    const next = expandAncestorsOf(defaultCollapseState(s), [[5, 1, 1]]);
    expect(next.has(nodeIdOf([5, 1]))).toBe(false);
    // unrelated collapsed regions are untouched
    expect(next.size).toEqual(defaultCollapseState(s).size - 1);
  });

  it("collects channel types", () => {
    expect(allChannelTypes(s)).toEqual(["batches", "rows"]);
  });
});

describe("lirIndex", () => {
  it("buckets member nodes by export/lir id", () => {
    const s = buildDataflowStructure(OPS, CHANNELS, LIR_SPANS);
    const index = lirIndex(s);
    expect([...index.keys()]).toEqual(["u42/1"]);
    // span 11..13 covers operator ids 11 ([5,1]) and 12 ([5,1,1])
    expect(index.get("u42/1")!.memberIds.sort()).toEqual(
      [nodeIdOf([5, 1]), nodeIdOf([5, 1, 1])].sort(),
    );
  });
});

describe("lirTree", () => {
  it("nests children under their parent via parentLirId, siblings descending by lir id", () => {
    const ops: OperatorRow[] = [
      {
        id: "1",
        address: ["9"],
        name: "Dataflow",
        arrangementRecords: "0",
        arrangementSize: "0",
        elapsedNs: "0",
      },
      {
        id: "2",
        address: ["9", "1"],
        name: "OpA",
        arrangementRecords: "0",
        arrangementSize: "0",
        elapsedNs: "0",
      },
      {
        id: "3",
        address: ["9", "2"],
        name: "OpB",
        arrangementRecords: "0",
        arrangementSize: "0",
        elapsedNs: "0",
      },
    ];
    const spans: LirSpanRow[] = [
      {
        exportId: "u1",
        lirId: "10",
        parentLirId: null,
        nesting: 0,
        operator: "Root",
        operatorIdStart: "2",
        operatorIdEnd: "4",
      },
      {
        exportId: "u1",
        lirId: "11",
        parentLirId: "10",
        nesting: 1,
        operator: "OpA",
        operatorIdStart: "2",
        operatorIdEnd: "3",
      },
      {
        exportId: "u1",
        lirId: "12",
        parentLirId: "10",
        nesting: 1,
        operator: "OpB",
        operatorIdStart: "3",
        operatorIdEnd: "4",
      },
    ];
    const s = buildDataflowStructure(ops, [], spans);
    const tree = lirTree(lirIndex(s));
    const roots = tree.get("u1")!;
    expect(roots.map((n) => n.key)).toEqual(["u1/10"]);
    expect(roots[0].children.map((n) => n.key)).toEqual(["u1/12", "u1/11"]);
    expect(roots[0].memberIds.sort()).toEqual(
      [nodeIdOf([9, 1]), nodeIdOf([9, 2])].sort(),
    );
    expect(roots[0].children.find((n) => n.key === "u1/11")!.memberIds).toEqual(
      [nodeIdOf([9, 1])],
    );
  });
});

describe("rerouteHiddenNodes", () => {
  const node = (id: string): VisibleNode => ({
    id,
    kind: "operator",
    label: id,
    parent: null,
    stats: null,
    transitive: null,
    childCount: 0,
    lir: [],
    address: null,
    operatorId: null,
  });
  const edge = (
    id: string,
    source: string,
    target: string,
    messagesSent = 0n,
    batchesSent = 0n,
    channelTypes: string[] = [],
  ): VisibleEdge => ({
    id,
    source,
    target,
    messagesSent,
    batchesSent,
    channelTypes,
  });

  it("returns the graph unchanged when nothing is hidden", () => {
    const g = {
      nodes: [node("a"), node("b")],
      edges: [edge("a=>b", "a", "b")],
    };
    expect(rerouteHiddenNodes(g, new Set())).toBe(g);
  });

  it("splices a pass-through edge across a single hidden node", () => {
    const g = {
      nodes: [node("a"), node("b"), node("c")],
      edges: [
        edge("a=>b", "a", "b", 3n, 1n, ["rows"]),
        edge("b=>c", "b", "c", 3n, 1n, ["rows"]),
      ],
    };
    const r = rerouteHiddenNodes(g, new Set(["b"]));
    expect(r.nodes.map((n) => n.id)).toEqual(["a", "c"]);
    expect(r.edges).toEqual([
      {
        id: "a=>c",
        source: "a",
        target: "c",
        messagesSent: 6n,
        batchesSent: 2n,
        channelTypes: ["rows"],
      },
    ]);
  });

  it("fans out through a hidden node with multiple successors", () => {
    const g = {
      nodes: [node("a"), node("b"), node("c"), node("d")],
      edges: [
        edge("a=>b", "a", "b"),
        edge("b=>c", "b", "c"),
        edge("b=>d", "b", "d"),
      ],
    };
    const r = rerouteHiddenNodes(g, new Set(["b"]));
    expect(new Set(r.edges.map((e) => e.id))).toEqual(
      new Set(["a=>c", "a=>d"]),
    );
  });

  it("fans in from multiple sources through a hidden node", () => {
    const g = {
      nodes: [node("a1"), node("a2"), node("b"), node("c")],
      edges: [
        edge("a1=>b", "a1", "b"),
        edge("a2=>b", "a2", "b"),
        edge("b=>c", "b", "c"),
      ],
    };
    const r = rerouteHiddenNodes(g, new Set(["b"]));
    expect(new Set(r.edges.map((e) => e.id))).toEqual(
      new Set(["a1=>c", "a2=>c"]),
    );
  });

  it("does not hang on a hidden feedback cycle and still bridges past it", () => {
    // Timely feedback loops mean two hidden nodes can point at each other.
    const g = {
      nodes: [node("a"), node("l"), node("m"), node("c")],
      edges: [
        edge("a=>l", "a", "l"),
        edge("l=>m", "l", "m"),
        edge("m=>l", "m", "l"), // cycle
        edge("m=>c", "m", "c"),
      ],
    };
    const r = rerouteHiddenNodes(g, new Set(["l", "m"]));
    expect(r.nodes.map((n) => n.id)).toEqual(["a", "c"]);
    expect(r.edges.map((e) => e.id)).toEqual(["a=>c"]);
  });

  it("drops an entirely hidden component with no visible endpoint", () => {
    const g = {
      nodes: [node("a"), node("b")],
      edges: [edge("a=>b", "a", "b")],
    };
    const r = rerouteHiddenNodes(g, new Set(["a", "b"]));
    expect(r.nodes).toEqual([]);
    expect(r.edges).toEqual([]);
  });

  it("keeps ordinary edges between two surviving visible nodes untouched", () => {
    const g = {
      nodes: [node("a"), node("b"), node("hidden")],
      edges: [edge("a=>b", "a", "b", 5n, 1n, ["rows"])],
    };
    const r = rerouteHiddenNodes(g, new Set(["hidden"]));
    expect(r.edges).toEqual(g.edges);
  });
});
