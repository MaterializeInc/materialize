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
  allSearchMatches,
  buildDataflowStructure,
  type ChannelRow,
  commonAncestorScope,
  decorateGraph,
  DEFAULT_FILTERS,
  deriveVisibleGraph,
  groupByLir,
  lirIndex,
  type LirInfo,
  type LirSpanRow,
  lirTree,
  nodeIdOf,
  type OperatorRow,
  type PerWorkerStatRow,
  representativeInView,
  rerouteHiddenNodes,
  subtreeSearchMatches,
  type VisibleEdge,
  type VisibleNode,
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

  it("throws with more than one root", () => {
    const twoRoots: OperatorRow[] = [
      { ...OPS[0], id: "10", address: ["5"] },
      { ...OPS[0], id: "20", address: ["6"] },
    ];
    expect(() => buildDataflowStructure(twoRoots, [], [])).toThrow();
  });

  it("returns a single placeholder node for a dropped dataflow, instead of throwing on zero operators", () => {
    // A dropped or transient dataflow returns zero operator rows: a valid
    // empty result, not an error. nodes.size === 1 here is what routes it
    // through the same "no longer exists" branch as any other empty
    // dataflow, rather than crashing mid-fetch.
    const s = buildDataflowStructure([], [], []);
    expect(s.nodes.size).toBe(1);
    expect(s.nodes.get(s.root)).toBeDefined();
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

  describe("skew", () => {
    // Join (id 12, [5,1,1]): worker 0 does 10ns/1000B, worker 1 does
    // 30ns/3000B -> 2x every direction. Map (id 13, [5,1,2]): only worker 0
    // ever scheduled it (5ns) and it never arranges anything -> perfectly
    // even CPU (a single data point can't be skewed), no memory data at all.
    const PER_WORKER: PerWorkerStatRow[] = [
      { id: "12", workerId: "0", elapsedNs: "10", arrangementSize: "1000" },
      { id: "12", workerId: "1", elapsedNs: "30", arrangementSize: "3000" },
      { id: "13", workerId: "0", elapsedNs: "5", arrangementSize: null },
    ];

    it("computes own skew as worst-worker over average, over participating workers only", () => {
      const s = buildDataflowStructure(OPS, CHANNELS, LIR_SPANS, PER_WORKER);
      const join = s.nodes.get(nodeIdOf([5, 1, 1]))!;
      expect(join.ownSkew.cpuSkew).toBeCloseTo(1.5); // 30 / ((10+30)/2)
      expect(join.ownSkew.memorySkew).toBeCloseTo(1.5); // 3000 / 2000

      const map = s.nodes.get(nodeIdOf([5, 1, 2]))!;
      expect(map.ownSkew.cpuSkew).toBeCloseTo(1); // one worker, one data point
      expect(map.ownSkew.memorySkew).toEqual(0); // no memory data at all

      const sink = s.nodes.get(nodeIdOf([5, 2]))!;
      expect(sink.ownSkew).toEqual({ cpuSkew: 0, memorySkew: 0 });
    });

    it("computes transitive skew from the subtree's summed per-worker vectors, not from children's own skew", () => {
      const s = buildDataflowStructure(OPS, CHANNELS, LIR_SPANS, PER_WORKER);
      const region = s.nodes.get(nodeIdOf([5, 1]))!;
      // cpu: worker0 = 10 (Join) + 5 (Map) = 15, worker1 = 30 (Join only,
      // Map never touched worker 1) -> avg 22.5, skew 30/22.5.
      expect(region.transitiveSkew.cpuSkew).toBeCloseTo(30 / 22.5);
      // memory: Map contributes nothing, so this is just Join's own vector.
      expect(region.transitiveSkew.memorySkew).toBeCloseTo(1.5);
    });
  });
});

describe("deriveVisibleGraph", () => {
  const s = buildDataflowStructure(OPS, CHANNELS, LIR_SPANS);
  const regionId = nodeIdOf([5, 1]);

  it("shows the region collapsed and the sibling leaf from the root view", () => {
    const g = deriveVisibleGraph(s, s.root);
    expect(g.nodes.map((n) => [n.id, n.kind])).toEqual([
      [regionId, "region"],
      [nodeIdOf([5, 2]), "operator"],
    ]);
    // channel 3 region -> sink survives, channels 1 and 2 are internal to
    // the region and never surface at this level.
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
    const region = g.nodes[0];
    expect(region.stats).toEqual(s.nodes.get(regionId)!.transitive);
    expect(region.childCount).toEqual(2);
  });

  it("drilling into the region shows its children and an inbound port", () => {
    const g = deriveVisibleGraph(s, regionId);
    expect(g.nodes.map((n) => [n.id, n.kind]).sort()).toEqual(
      [
        [nodeIdOf([5, 1, 1]), "operator"],
        [nodeIdOf([5, 1, 2]), "operator"],
        [`${regionId}:in:0`, "port"],
        [`${regionId}:out:0`, "port"],
      ].sort(),
    );
    const inPort = g.nodes.find((n) => n.id === `${regionId}:in:0`)!;
    expect(inPort.label).toEqual("input 0");
    expect(inPort.operatorId).toBeNull();
    // channel 1 (port fan-in) and channel 2 (internal) both survive.
    expect(g.edges.map((e) => e.id).sort()).toEqual(
      [
        `${regionId}:in:0=>${nodeIdOf([5, 1, 1])}`,
        `${nodeIdOf([5, 1, 1])}=>${nodeIdOf([5, 1, 2])}`,
      ].sort(),
    );
    // channel 3's outer half (region's own address -> sibling [5,2]) has no
    // representable target from inside the region (the sibling isn't part
    // of this view), so its edge is dropped, but the "out" port it resolved
    // to registers anyway: the box's boundary traffic isn't silently erased.
    expect(
      g.edges.some(
        (e) =>
          e.source === `${regionId}:out:0` || e.target === `${regionId}:out:0`,
      ),
    ).toBe(false);
    // ...and the sibling it can't show directly is recorded as a jump target.
    const outPort = g.nodes.find((n) => n.id === `${regionId}:out:0`)!;
    expect(outPort.peers).toEqual([
      {
        address: [5, 2],
        label: "Sink",
        messagesSent: 5n,
        batchesSent: 2n,
        channelTypes: ["batches"],
        // Sink is a leaf: nothing to drill into, so the peer box itself is
        // the target.
        peerPortId: null,
      },
    ]);
    // channel 1's other end (Join) is already visible in this same view, so
    // the "in" port needs no jump link for it.
    expect(inPort.peers).toEqual([]);
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
    const g = deriveVisibleGraph(extra, extra.root);
    const e = g.edges.find((edge) => edge.target === nodeIdOf([5, 2]))!;
    expect(e.messagesSent).toEqual(12n);
    expect(e.channelTypes).toEqual(["batches", "rows"]);
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

    // Viewed from inside the region, the crossing's inner half (channel 1)
    // already produced this port; the outer half's external source isn't
    // part of this view, so it contributes no new edge or node.
    const drilledIn = deriveVisibleGraph(withExternal, regionId);
    const inPort = drilledIn.nodes.find((n) => n.id === portId)!;
    expect(inPort.kind).toEqual("port");
    expect(
      drilledIn.edges.some(
        (e) => e.source === externalId || e.target === externalId,
      ),
    ).toBe(false);
    // ...but the external source is still reachable as a jump target off
    // the port itself.
    expect(inPort.peers).toEqual([
      {
        address: [5, 3],
        label: "External",
        messagesSent: 9n,
        batchesSent: 3n,
        channelTypes: ["rows"],
        // External is a leaf too.
        peerPortId: null,
      },
    ]);

    // From the root, the crossing lands on the region's own (collapsed)
    // node, matching how every other boundary crossing into a collapsed
    // region behaves: the whole region is one box.
    const root = deriveVisibleGraph(withExternal, withExternal.root);
    const rootEdge = root.edges.find((e) => e.source === externalId)!;
    expect(rootEdge.target).toEqual(regionId);
  });

  it("fans a port out to multiple peers (one output can feed several inputs)", () => {
    const fannedOut = buildDataflowStructure(
      [
        ...OPS,
        {
          id: "21",
          address: ["5", "4"],
          name: "Sink2",
          arrangementRecords: "0",
          arrangementSize: "0",
          elapsedNs: "1",
        },
      ],
      [
        ...CHANNELS,
        // Same output (region's port 0) also feeds a second sibling.
        {
          id: "5",
          fromOperatorAddress: ["5", "1"],
          fromPort: "0",
          toOperatorAddress: ["5", "4"],
          toPort: "0",
          messagesSent: "2",
          batchesSent: "1",
          channelType: "rows",
        },
      ],
      [],
    );
    const g = deriveVisibleGraph(fannedOut, regionId);
    const outPort = g.nodes.find((n) => n.id === `${regionId}:out:0`)!;
    expect(outPort.peers.map((p) => p.label).sort()).toEqual(["Sink", "Sink2"]);
    expect(outPort.peers.find((p) => p.label === "Sink2")).toEqual({
      address: [5, 4],
      label: "Sink2",
      messagesSent: 2n,
      batchesSent: 1n,
      channelTypes: ["rows"],
      peerPortId: null,
    });
  });

  it("resolves a region peer's own port, so a jump can drill straight to it", () => {
    // Two sibling regions, A's output feeding B's input directly (not
    // nested in each other): from inside A, B is a region peer with a
    // real port to land on inside B; from inside B, A is the same.
    const s2 = buildDataflowStructure(
      [
        { ...OPS[0], id: "1", address: ["9"], name: "Dataflow" },
        { ...OPS[0], id: "2", address: ["9", "1"], name: "RegionA" },
        { ...OPS[0], id: "3", address: ["9", "1", "1"], name: "LeafA" },
        { ...OPS[0], id: "4", address: ["9", "2"], name: "RegionB" },
        { ...OPS[0], id: "5", address: ["9", "2", "1"], name: "LeafB" },
      ],
      [
        {
          id: "1",
          fromOperatorAddress: ["9", "1"],
          fromPort: "3",
          toOperatorAddress: ["9", "2"],
          toPort: "7",
          messagesSent: "1",
          batchesSent: "1",
          channelType: "rows",
        },
      ],
      [],
    );
    const aId = nodeIdOf([9, 1]);
    const bId = nodeIdOf([9, 2]);

    const fromA = deriveVisibleGraph(s2, aId);
    const outPort = fromA.nodes.find((n) => n.id === `${aId}:out:3`)!;
    expect(outPort.peers).toEqual([
      expect.objectContaining({ address: [9, 2], peerPortId: `${bId}:in:7` }),
    ]);

    const fromB = deriveVisibleGraph(s2, bId);
    const inPort = fromB.nodes.find((n) => n.id === `${bId}:in:7`)!;
    expect(inPort.peers).toEqual([
      expect.objectContaining({ address: [9, 1], peerPortId: `${aId}:out:3` }),
    ]);
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
    expect(() =>
      deriveVisibleGraph(withDangling, withDangling.root),
    ).not.toThrow();
    const g = deriveVisibleGraph(withDangling, withDangling.root);
    const nodeIds = new Set(g.nodes.map((n) => n.id));
    for (const e of g.edges) {
      expect(nodeIds.has(e.source)).toBe(true);
      expect(nodeIds.has(e.target)).toBe(true);
    }
  });
});

describe("commonAncestorScope / representativeInView", () => {
  const s = buildDataflowStructure(OPS, CHANNELS, LIR_SPANS);
  const regionId = nodeIdOf([5, 1]);

  it("a single leaf address navigates to its immediate parent scope", () => {
    expect(commonAncestorScope(s, [[5, 1, 1]])).toEqual(regionId);
    expect(representativeInView([5, 1, 1], [5, 1])).toEqual(
      nodeIdOf([5, 1, 1]),
    );
  });

  it("backs off past an address that IS the raw common prefix", () => {
    // [5,1] is itself a prefix of [5,1,1], so it can't be the view root
    // (nothing could highlight it as its own child); back off to its parent.
    expect(
      commonAncestorScope(s, [
        [5, 1],
        [5, 1, 1],
      ]),
    ).toEqual(s.root);
    // From the root, [5,1] represents itself and [5,1,1] rolls up into it.
    expect(representativeInView([5, 1], [5])).toEqual(regionId);
    expect(representativeInView([5, 1, 1], [5])).toEqual(regionId);
  });

  it("returns null for an address outside the view root's subtree", () => {
    expect(representativeInView([5, 2], [5, 1])).toBeNull();
  });
});

describe("allSearchMatches / subtreeSearchMatches", () => {
  const s = buildDataflowStructure(OPS, CHANNELS, LIR_SPANS);
  const regionId = nodeIdOf([5, 1]);

  it("allSearchMatches finds matches anywhere, in address order, excluding the root", () => {
    expect(allSearchMatches(s, "i").map((n) => n.id)).toEqual([
      regionId, // "Region"
      nodeIdOf([5, 1, 1]), // "Join"
      nodeIdOf([5, 2]), // "Sink"
    ]);
    expect(allSearchMatches(s, "nonexistent")).toEqual([]);
    expect(allSearchMatches(s, "")).toEqual([]);
  });

  it("subtreeSearchMatches flags a region as containing a match without matching itself", () => {
    const { matches, containsMatch } = subtreeSearchMatches(s, "join");
    expect(matches.has(nodeIdOf([5, 1, 1]))).toBe(true);
    expect(matches.has(regionId)).toBe(false);
    // The region doesn't match "join" itself, but a descendant does.
    expect(containsMatch.has(regionId)).toBe(true);
    expect(containsMatch.has(nodeIdOf([5, 2]))).toBe(false);
  });
});

describe("decorateGraph", () => {
  const s = buildDataflowStructure(OPS, CHANNELS, LIR_SPANS);
  const regionId = nodeIdOf([5, 1]);
  const root = deriveVisibleGraph(s, s.root);
  const drilledIn = deriveVisibleGraph(s, regionId);
  const heat = (t: number) => `heat(${t.toFixed(2)})`;

  it("a region containing a match (but not matching itself) stays undimmed and uncounted", () => {
    const searchInfo = subtreeSearchMatches(s, "join");
    const d = decorateGraph(
      root,
      { ...DEFAULT_FILTERS, search: "join" },
      heat,
      searchInfo,
    );
    // "Join" is inside the region, not visible at the root; the region
    // doesn't match "join" by name either, so it's neither a listed match
    // nor dimmed, while the sibling sink (no match anywhere inside it) is.
    expect(d.searchMatches).toEqual([]);
    expect(d.dimmedNodeIds.has(regionId)).toBe(false);
    expect(d.dimmedNodeIds.has(nodeIdOf([5, 2]))).toBe(true);
  });

  it("a directly visible match is listed and left undimmed", () => {
    const searchInfo = subtreeSearchMatches(s, "join");
    const d = decorateGraph(
      drilledIn,
      { ...DEFAULT_FILTERS, search: "join" },
      heat,
      searchInfo,
    );
    expect(d.searchMatches).toEqual([nodeIdOf([5, 1, 1])]);
    expect(d.dimmedNodeIds.has(nodeIdOf([5, 1, 1]))).toBe(false);
    expect(d.dimmedNodeIds.has(nodeIdOf([5, 1, 2]))).toBe(true);
  });

  it("hideIdle hides zero-activity operators and zero-message edges, never regions or ports", () => {
    const d = decorateGraph(
      root,
      { ...DEFAULT_FILTERS, hideIdle: true },
      heat,
      null,
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
      deriveVisibleGraph(idle, idle.root),
      { ...DEFAULT_FILTERS, hideIdle: true },
      heat,
      null,
    );
    expect(d2.hiddenNodeIds.has(nodeIdOf([5, 3]))).toBe(true);
    expect(d2.hiddenNodeIds.has(nodeIdOf([5, 1]))).toBe(false);
    expect(d.hiddenEdgeIds.size).toBe(0); // all fixture edges have messages
  });

  it("heatmap colors by transitive metric and dims below threshold", () => {
    const d = decorateGraph(
      root,
      { ...DEFAULT_FILTERS, heatmap: "elapsed", heatmapThreshold: 0.5 },
      heat,
      null,
    );
    // max transitive elapsed among visible non-ports is the region (13n)
    expect(d.nodeColors.get(regionId)).toEqual("heat(1.00)");
    expect(d.dimmedNodeIds.has(nodeIdOf([5, 2]))).toBe(true); // 2/13 < 0.5
  });

  it("heatmap with all-zero metric sets no colors", () => {
    const zero = buildDataflowStructure(
      OPS.map((o) => ({ ...o, elapsedNs: "0" })),
      CHANNELS,
      [],
    );
    const d = decorateGraph(
      deriveVisibleGraph(zero, zero.root),
      { ...DEFAULT_FILTERS, heatmap: "elapsed" },
      heat,
      null,
    );
    expect(d.nodeColors.size).toBe(0);
  });

  it("cpuSkew/memorySkew heatmap colors by transitive skew", () => {
    const skewed = buildDataflowStructure(OPS, CHANNELS, LIR_SPANS, [
      { id: "12", workerId: "0", elapsedNs: "10", arrangementSize: "1000" },
      { id: "12", workerId: "1", elapsedNs: "30", arrangementSize: "3000" },
    ]);
    const skewedRoot = deriveVisibleGraph(skewed, skewed.root);
    const d = decorateGraph(
      skewedRoot,
      { ...DEFAULT_FILTERS, heatmap: "cpuSkew" },
      heat,
      null,
    );
    // The region's subtree (containing Join) is the only skewed thing; the
    // sibling sink has no per-worker data at all, so it's the coolest.
    expect(d.nodeColors.get(regionId)).toEqual("heat(1.00)");
    expect(d.nodeColors.get(nodeIdOf([5, 2]))).toEqual("heat(0.00)");
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
        arrangementRecords: "100",
        arrangementSize: "0",
        elapsedNs: "10",
      },
      {
        id: "3",
        address: ["9", "2"],
        name: "OpB",
        arrangementRecords: "200",
        arrangementSize: "0",
        elapsedNs: "20",
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
    const tree = lirTree(s, lirIndex(s));
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

  it("sums own stats over members, matching EXPLAIN ANALYZE's semantics: a parent's summary includes its children's", () => {
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
        arrangementRecords: "100",
        arrangementSize: "0",
        elapsedNs: "10",
      },
      {
        id: "3",
        address: ["9", "2"],
        name: "OpB",
        arrangementRecords: "200",
        arrangementSize: "0",
        elapsedNs: "20",
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
    ];
    const s = buildDataflowStructure(ops, [], spans);
    const tree = lirTree(s, lirIndex(s));
    const root = tree.get("u1")![0];
    expect(root.summary).toEqual({
      arrangementRecords: 300n,
      arrangementSize: 0n,
      elapsedNs: 30n,
    });
    expect(root.children[0].summary).toEqual({
      arrangementRecords: 100n,
      arrangementSize: 0n,
      elapsedNs: 10n,
    });
  });
});

describe("rerouteHiddenNodes", () => {
  const node = (id: string): VisibleNode => ({
    id,
    kind: "operator",
    label: id,
    stats: null,
    transitive: null,
    transitiveSkew: null,
    childCount: 0,
    lir: [],
    address: null,
    operatorId: null,
    peers: [],
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

describe("groupByLir", () => {
  const node = (
    id: string,
    operatorId: number,
    lir: LirInfo[] = [],
  ): VisibleNode => ({
    id,
    kind: "operator",
    label: id,
    stats: null,
    transitive: null,
    transitiveSkew: null,
    childCount: 0,
    lir,
    address: null,
    operatorId: BigInt(operatorId),
    peers: [],
  });
  const port = (id: string): VisibleNode => ({
    id,
    kind: "port",
    label: id,
    stats: null,
    transitive: null,
    transitiveSkew: null,
    childCount: 0,
    lir: [],
    address: null,
    operatorId: null,
    peers: [],
  });
  const span = (
    exportId: string,
    lirId: string,
    nesting: number,
    parentLirId: string | null,
  ): LirInfo => ({ exportId, lirId, parentLirId, nesting, operator: lirId });

  it("groups nothing when no node has lir data", () => {
    const g = groupByLir([node("a", 1), node("b", 2)]);
    expect(g.groups).toEqual([]);
    expect(g.parentOf.size).toEqual(0);
  });

  it("groups a flat run under one lir id", () => {
    const s = span("u1", "10", 0, null);
    const g = groupByLir([
      node("a", 1, [s]),
      node("b", 2, [s]),
      node("c", 3, [s]),
    ]);
    expect(g.groups.map((x) => x.id)).toEqual(["u1/10"]);
    expect(g.parentOf.get("a")).toEqual("u1/10");
    expect(g.parentOf.get("b")).toEqual("u1/10");
    expect(g.parentOf.get("c")).toEqual("u1/10");
  });

  it("nests an inner span inside an outer span", () => {
    const outer = span("u1", "10", 0, null);
    const inner = span("u1", "11", 1, "10");
    const g = groupByLir([
      node("a", 1, [outer]),
      node("b", 2, [outer, inner]),
      node("c", 3, [outer, inner]),
      node("d", 4, [outer]),
    ]);
    expect(g.groups.map((x) => x.id)).toEqual(["u1/10", "u1/11"]);
    expect(g.parentOf.get("u1/11")).toEqual("u1/10");
    expect(g.parentOf.get("a")).toEqual("u1/10");
    expect(g.parentOf.get("b")).toEqual("u1/11");
    expect(g.parentOf.get("c")).toEqual("u1/11");
    expect(g.parentOf.get("d")).toEqual("u1/10");
  });

  it("leaves ports and lir-less nodes ungrouped alongside a group", () => {
    const s = span("u1", "10", 0, null);
    const g = groupByLir([node("a", 1, [s]), port("p"), node("b", 2)]);
    expect(g.groups.map((x) => x.id)).toEqual(["u1/10"]);
    expect(g.parentOf.get("a")).toEqual("u1/10");
    expect(g.parentOf.has("p")).toEqual(false);
    expect(g.parentOf.has("b")).toEqual(false);
  });

  it("picks the lowest exportId, leaving other-export-only nodes ungrouped", () => {
    const sA = span("u1", "5", 0, null);
    const sB = span("u2", "3", 0, null);
    const g = groupByLir([
      node("a", 1, [sA]),
      node("b", 2, [sB]),
      node("c", 3, [sA, sB]),
    ]);
    expect(g.groups.map((x) => x.id)).toEqual(["u1/5"]);
    expect(g.parentOf.get("a")).toEqual("u1/5");
    expect(g.parentOf.has("b")).toEqual(false);
    expect(g.parentOf.get("c")).toEqual("u1/5");
  });

  it("sorts input by operatorId regardless of array order", () => {
    const outer = span("u1", "10", 0, null);
    const inner = span("u1", "11", 1, "10");
    // Deliberately out of operatorId order.
    const g = groupByLir([
      node("d", 4, [outer]),
      node("b", 2, [outer, inner]),
      node("a", 1, [outer]),
      node("c", 3, [outer, inner]),
    ]);
    expect(g.groups.map((x) => x.id)).toEqual(["u1/10", "u1/11"]);
    expect(g.parentOf.get("b")).toEqual("u1/11");
    expect(g.parentOf.get("c")).toEqual("u1/11");
  });
});
