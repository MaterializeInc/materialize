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

  it("computes own and transitive schedule counts alongside elapsed time", () => {
    const s = buildDataflowStructure(
      OPS.map((o) =>
        o.id === "12"
          ? { ...o, scheduleCount: "100" }
          : o.id === "13"
            ? { ...o, scheduleCount: "50" }
            : o,
      ),
      CHANNELS,
      LIR_SPANS,
    );
    const join = s.nodes.get(nodeIdOf([5, 1, 1]))!;
    expect(join.own.scheduleCount).toEqual(100n);
    const region = s.nodes.get(nodeIdOf([5, 1]))!;
    // Region's own row sets no scheduleCount (defaults 0); transitive sums
    // Join's and Map's.
    expect(region.transitive.scheduleCount).toEqual(150n);
  });

  it("computes a region's overhead as its own elapsed time minus its direct children's own elapsed time", () => {
    const overheadOps = OPS.map((o) =>
      o.id === "11"
        ? { ...o, elapsedNs: "100" }
        : o.id === "12"
          ? { ...o, elapsedNs: "30" }
          : o.id === "13"
            ? { ...o, elapsedNs: "20" }
            : o,
    );
    const s = buildDataflowStructure(overheadOps, CHANNELS, LIR_SPANS);
    const region = s.nodes.get(nodeIdOf([5, 1]))!;
    // 100 - (30 + 20) = 50: time in the region's own scheduling loop, not
    // attributable to either child.
    expect(region.overheadNs).toEqual(50n);
    // A leaf has no children to subtract, so this is just its own elapsed
    // time -- not a meaningful "overhead" reading on its own.
    const join = s.nodes.get(nodeIdOf([5, 1, 1]))!;
    expect(join.overheadNs).toEqual(30n);
  });

  it("compares a region's own time against its direct children's own time, not their transitive rollup", () => {
    // Timely's own-elapsed accounting is exclusive self time (verified
    // against live introspection data: a real region's own.elapsedNs can
    // sit well below its direct children's own.elapsedNs summed, which is
    // only possible if "own" never included child execution to begin
    // with). Join here has its own grandchild (a nested region inside it)
    // whose time dwarfs Join's own row; overhead must compare Region
    // against Join's own 5ns, not Join's transitive total.
    const nested = buildDataflowStructure(
      [
        { ...OPS[0], id: "20", address: ["6"], name: "Dataflow" },
        {
          ...OPS[0],
          id: "21",
          address: ["6", "1"],
          name: "Region",
          elapsedNs: "10",
        },
        {
          ...OPS[0],
          id: "22",
          address: ["6", "1", "1"],
          name: "Join",
          elapsedNs: "5",
        },
        {
          ...OPS[0],
          id: "23",
          address: ["6", "1", "1", "1"],
          name: "GrandchildLeaf",
          elapsedNs: "1000",
        },
      ],
      [],
      [],
    );
    const region = nested.nodes.get(nodeIdOf([6, 1]))!;
    // Join's transitive elapsed (5 + 1000 = 1005) would make this wildly
    // negative if used instead of Join's own (5): 10 - 5 = 5.
    expect(region.overheadNs).toEqual(5n);
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
      expect(sink.ownSkew).toEqual({
        cpuSkew: 0,
        memorySkew: 0,
        scheduleSkew: 0,
      });
    });

    it("computes schedule-count skew the same way as CPU/memory skew", () => {
      const s = buildDataflowStructure(OPS, CHANNELS, LIR_SPANS, [
        {
          id: "12",
          workerId: "0",
          elapsedNs: null,
          arrangementSize: null,
          scheduleCount: "100",
        },
        {
          id: "12",
          workerId: "1",
          elapsedNs: null,
          arrangementSize: null,
          scheduleCount: "300",
        },
      ]);
      const join = s.nodes.get(nodeIdOf([5, 1, 1]))!;
      expect(join.ownSkew.scheduleSkew).toBeCloseTo(1.5); // 300 / ((100+300)/2)
    });

    it("computes transitive skew as the max of its own and its subtree's skew, matching EXPLAIN ANALYZE's rollup, not a merged-and-then-ratioed vector", () => {
      // Join is skewed toward worker 0 (1000 vs 0); Map is skewed toward
      // worker 1 (0 vs 1000) by the same amount, in the opposite
      // direction. Summed per worker, the totals are 1000/1000 across the
      // whole region -- perfectly even -- even though both operators are
      // individually badly skewed (2x each): a merged-vector rollup would
      // report the region as perfectly balanced, hiding both bad actors.
      const s = buildDataflowStructure(OPS, CHANNELS, LIR_SPANS, [
        { id: "12", workerId: "0", elapsedNs: "1000", arrangementSize: "0" },
        { id: "12", workerId: "1", elapsedNs: "0", arrangementSize: "0" },
        { id: "13", workerId: "0", elapsedNs: "0", arrangementSize: "0" },
        { id: "13", workerId: "1", elapsedNs: "1000", arrangementSize: "0" },
      ]);
      const region = s.nodes.get(nodeIdOf([5, 1]))!;
      expect(region.transitiveSkew.cpuSkew).toBeCloseTo(2);
    });

    it("ignores an operator's own skew if its work is negligible next to the busiest node in the dataflow, however extreme its raw ratio, and keeps it from polluting an ancestor's rolled-up skew", () => {
      // Join is genuinely busy (1ms) and perfectly balanced. Map's own
      // elapsed is 1ns (0.0001% of Join's magnitude), but its per-worker
      // split (1/0/0/0) would post the theoretical 4-worker ceiling ratio
      // if taken at face value, despite contributing nothing real.
      const skewedOps = OPS.map((o) =>
        o.id === "12"
          ? { ...o, elapsedNs: "1000000" }
          : o.id === "13"
            ? { ...o, elapsedNs: "1" }
            : o,
      );
      const s = buildDataflowStructure(skewedOps, CHANNELS, LIR_SPANS, [
        { id: "12", workerId: "0", elapsedNs: "500000", arrangementSize: "0" },
        { id: "12", workerId: "1", elapsedNs: "500000", arrangementSize: "0" },
        { id: "13", workerId: "0", elapsedNs: "1", arrangementSize: "0" },
        { id: "13", workerId: "1", elapsedNs: "0", arrangementSize: "0" },
        { id: "13", workerId: "2", elapsedNs: "0", arrangementSize: "0" },
        { id: "13", workerId: "3", elapsedNs: "0", arrangementSize: "0" },
      ]);
      const map = s.nodes.get(nodeIdOf([5, 1, 2]))!;
      expect(map.ownSkew.cpuSkew).toEqual(0);
      // The region's worst real skew is Join's honest 1.0 (perfectly
      // even), not Map's suppressed near-ceiling ratio.
      const region = s.nodes.get(nodeIdOf([5, 1]))!;
      expect(region.transitiveSkew.cpuSkew).toEqual(1);
    });

    it("keeps the real own skew available unfiltered, for display, even when the magnitude floor suppresses it for rollup", () => {
      const skewedOps = OPS.map((o) =>
        o.id === "12"
          ? { ...o, elapsedNs: "1000000" }
          : o.id === "13"
            ? { ...o, elapsedNs: "1" }
            : o,
      );
      const s = buildDataflowStructure(skewedOps, CHANNELS, LIR_SPANS, [
        { id: "12", workerId: "0", elapsedNs: "500000", arrangementSize: "0" },
        { id: "12", workerId: "1", elapsedNs: "500000", arrangementSize: "0" },
        { id: "13", workerId: "0", elapsedNs: "1", arrangementSize: "0" },
        { id: "13", workerId: "1", elapsedNs: "0", arrangementSize: "0" },
        { id: "13", workerId: "2", elapsedNs: "0", arrangementSize: "0" },
        { id: "13", workerId: "3", elapsedNs: "0", arrangementSize: "0" },
      ]);
      const map = s.nodes.get(nodeIdOf([5, 1, 2]))!;
      expect(map.ownSkew.cpuSkew).toEqual(0); // suppressed, for rollup safety
      expect(map.ownSkewRaw.cpuSkew).toBeCloseTo(4); // 1 / ((1+0+0+0)/4)
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
        // channel 3 touches the region at exactly its own address (port 0),
        // so this edge's source can drill straight to that inner port.
        sourceLandings: [
          {
            address: [5, 1],
            label: "output 0",
            messagesSent: 5n,
            batchesSent: 2n,
            channelTypes: ["batches"],
            peerPortId: `${regionId}:out:0`,
          },
        ],
        targetLandings: [],
      },
    ]);
    const region = g.nodes[0];
    expect(region.stats).toEqual(s.nodes.get(regionId)!.transitive);
    expect(region.childCount).toEqual(2);
    // `stats` collapses to the subtree total for a quick canvas-level read,
    // but the detail panel needs the region's own activity (5ns) kept
    // distinct from that total (5+7+1=13ns), or a region's own dispatch
    // cost becomes invisible next to its children's.
    expect(region.own).toEqual(s.nodes.get(regionId)!.own);
    expect(region.own!.elapsedNs).toEqual(5n);
    expect(region.transitive!.elapsedNs).toEqual(13n);
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

  it("records which of a region's own ports a merged edge's real channels land on", () => {
    // Timely gates every scope crossing through the box's own address, one
    // hop at a time (verified against live introspection data: real
    // channels never skip straight past a region to something deeper
    // inside it), so a channel touching RegionA always does so at exactly
    // address [8,1], never deeper. Two of RegionA's own output ports (0 and
    // 1) both happen to feed the same downstream leaf, Sink: from the
    // root, both collapse onto the same visible edge (RegionA -> Sink),
    // discarding which of RegionA's ports is which unless something
    // records it separately.
    const withPorts = buildDataflowStructure(
      [
        { ...OPS[0], id: "60", address: ["8"], name: "Dataflow" },
        { ...OPS[0], id: "61", address: ["8", "1"], name: "RegionA" },
        { ...OPS[0], id: "62", address: ["8", "1", "1"], name: "LeafInner" },
        { ...OPS[0], id: "64", address: ["8", "2"], name: "Sink" },
      ],
      [
        {
          id: "50",
          fromOperatorAddress: ["8", "1"],
          fromPort: "0",
          toOperatorAddress: ["8", "2"],
          toPort: "0",
          messagesSent: "3",
          batchesSent: "1",
          channelType: "rows",
        },
        {
          id: "51",
          fromOperatorAddress: ["8", "1"],
          fromPort: "1",
          toOperatorAddress: ["8", "2"],
          toPort: "0",
          messagesSent: "2",
          batchesSent: "1",
          channelType: "rows",
        },
      ],
      [],
    );
    const g = deriveVisibleGraph(withPorts, withPorts.root);
    const regionAId = nodeIdOf([8, 1]);
    const edge = g.edges.find(
      (e) => e.id === `${regionAId}=>${nodeIdOf([8, 2])}`,
    )!;
    expect(
      edge.sourceLandings.sort((a, b) => a.label.localeCompare(b.label)),
    ).toEqual([
      {
        address: [8, 1],
        label: "output 0",
        messagesSent: 3n,
        batchesSent: 1n,
        channelTypes: ["rows"],
        peerPortId: `${regionAId}:out:0`,
      },
      {
        address: [8, 1],
        label: "output 1",
        messagesSent: 2n,
        batchesSent: 1n,
        channelTypes: ["rows"],
        peerPortId: `${regionAId}:out:1`,
      },
    ]);
    // Sink is a leaf: it has no inner ports of its own to land on.
    expect(edge.targetLandings).toEqual([]);
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

  it("resolves a peer past a nested scope's boundary pseudo-vertex", () => {
    // A scope's own boundary is logged as a pseudo-vertex [scope, 0], which
    // never gets an operator row (only real children do). When a region is
    // nested two scopes deep, its outer boundary channel targets its
    // *enclosing* scope's pseudo-vertex directly (mirroring the [5,1,0]
    // pattern in CHANNELS one level up), e.g. [6,1,0] -> [6,1,1] for a region
    // at [6,1,1] inside [6,1]. [6,1,0] has no operator row, so naively
    // resolving the peer address finds nothing; the fix is to recognize the
    // pseudo-vertex and peel back to its enclosing scope [6,1], which does.
    const nested = buildDataflowStructure(
      [
        { ...OPS[0], id: "30", address: ["6"], name: "Dataflow" },
        { ...OPS[0], id: "31", address: ["6", "1"], name: "RegionOuter" },
        { ...OPS[0], id: "32", address: ["6", "1", "1"], name: "RegionInner" },
        {
          ...OPS[0],
          id: "33",
          address: ["6", "1", "1", "1"],
          name: "LeafInner",
        },
      ],
      [
        {
          id: "40",
          fromOperatorAddress: ["6", "1", "0"],
          fromPort: "0",
          toOperatorAddress: ["6", "1", "1"],
          toPort: "0",
          messagesSent: "4",
          batchesSent: "1",
          channelType: "rows",
        },
        {
          id: "41",
          fromOperatorAddress: ["6", "1", "1", "0"],
          fromPort: "0",
          toOperatorAddress: ["6", "1", "1", "1"],
          toPort: "0",
          messagesSent: "4",
          batchesSent: "1",
          channelType: "rows",
        },
      ],
      [],
    );
    const outerId = nodeIdOf([6, 1]);
    const innerId = nodeIdOf([6, 1, 1]);

    const fromInner = deriveVisibleGraph(nested, innerId);
    const inPort = fromInner.nodes.find((n) => n.id === `${innerId}:in:0`)!;
    expect(inPort.peers).toEqual([
      expect.objectContaining({
        address: [6, 1],
        label: "RegionOuter",
        peerPortId: `${outerId}:in:0`,
      }),
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

describe("deriveVisibleGraph resolution refactor", () => {
  const structure = buildDataflowStructure(OPS, CHANNELS, [], []);

  it("preserves collapsed-region landings with no expansion", () => {
    // channel 3 crosses region [5,1]'s boundary to leaf [5,2]. At the root
    // view the region is collapsed, so the edge must still name the inner
    // port it lands on (the regression the naive global-set substitution
    // would erase).
    const g = deriveVisibleGraph(structure, structure.root);
    const edge = g.edges.find(
      (e) => e.source === nodeIdOf([5, 1]) && e.target === nodeIdOf([5, 2]),
    );
    expect(edge).toBeDefined();
    expect(edge!.sourceLandings.length).toBeGreaterThan(0);
  });

  it("accepts an empty expandedScopes and behaves identically", () => {
    const a = deriveVisibleGraph(structure, structure.root);
    const b = deriveVisibleGraph(structure, structure.root, new Set());
    expect(b).toEqual(a);
  });
});

describe("deriveVisibleGraph in-place expansion", () => {
  const structure = buildDataflowStructure(OPS, CHANNELS, [], []);

  it("materializes an expanded region's children nested under it", () => {
    const g = deriveVisibleGraph(
      structure,
      structure.root,
      new Set([nodeIdOf([5, 1])]),
    );
    const ids = new Set(g.nodes.map((n) => n.id));
    expect(ids.has(nodeIdOf([5, 1, 1]))).toBe(true); // Join, now visible
    expect(ids.has(nodeIdOf([5, 1, 2]))).toBe(true); // Map, now visible
    const join = g.nodes.find((n) => n.id === nodeIdOf([5, 1, 1]));
    expect(join!.parentId).toBe(nodeIdOf([5, 1]));
    const region = g.nodes.find((n) => n.id === nodeIdOf([5, 1]));
    expect(region!.expanded).toBe(true);
  });

  it("reroutes an inbound edge to the expanded region's inner port", () => {
    // Channel 1 feeds the region's input port -> child [5,1,1]. With the
    // region expanded, [5,1,1] is visible, so an edge must terminate on the
    // inner port (scope [5,1]) rather than collapsing onto the region box.
    const g = deriveVisibleGraph(
      structure,
      structure.root,
      new Set([nodeIdOf([5, 1])]),
    );
    const toJoin = g.edges.find((e) => e.target === nodeIdOf([5, 1, 1]));
    expect(toJoin).toBeDefined();
  });

  it("resolves edges to the deepest visible node under two-level expansion", () => {
    const g = deriveVisibleGraph(
      structure,
      structure.root,
      new Set([nodeIdOf([5, 1])]),
    );
    // The internal channel 2 ([5,1,1] -> [5,1,2]) is now a visible edge
    // between two inner operators.
    const inner = g.edges.find(
      (e) =>
        e.source === nodeIdOf([5, 1, 1]) && e.target === nodeIdOf([5, 1, 2]),
    );
    expect(inner).toBeDefined();
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
      1,
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
      1,
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
      1,
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
      1,
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
      1,
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
      1,
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
      4,
    );
    // The region's subtree (containing Join) is the only skewed thing (1.5
    // ratio); the sibling sink has no per-worker data at all, so it's the
    // coolest, and neither depends on the other being in view.
    expect(d.nodeColors.get(regionId)).toEqual(
      `heat(${(Math.log2(1.5) / Math.log2(4)).toFixed(2)})`,
    );
    expect(d.nodeColors.get(nodeIdOf([5, 2]))).toEqual("heat(0.00)");
  });

  it("scheduleSkew heatmap colors by transitive schedule-count skew, same as cpu/memory", () => {
    const skewed = buildDataflowStructure(OPS, CHANNELS, LIR_SPANS, [
      {
        id: "12",
        workerId: "0",
        elapsedNs: null,
        arrangementSize: null,
        scheduleCount: "100",
      },
      {
        id: "12",
        workerId: "1",
        elapsedNs: null,
        arrangementSize: null,
        scheduleCount: "300",
      },
    ]);
    const skewedRoot = deriveVisibleGraph(skewed, skewed.root);
    const d = decorateGraph(
      skewedRoot,
      { ...DEFAULT_FILTERS, heatmap: "scheduleSkew" },
      heat,
      null,
      4,
    );
    expect(d.nodeColors.get(regionId)).toEqual(
      `heat(${(Math.log2(1.5) / Math.log2(4)).toFixed(2)})`,
    );
    expect(d.nodeColors.get(nodeIdOf([5, 2]))).toEqual("heat(0.00)");
  });

  it("skew heatmap floor is exactly ratio 1, ceiling is exactly the worker count", () => {
    const perfectlyEven = buildDataflowStructure(OPS, CHANNELS, LIR_SPANS, [
      { id: "12", workerId: "0", elapsedNs: "100", arrangementSize: "0" },
      { id: "12", workerId: "1", elapsedNs: "100", arrangementSize: "0" },
    ]);
    const dAtFloor = decorateGraph(
      deriveVisibleGraph(perfectlyEven, regionId),
      { ...DEFAULT_FILTERS, heatmap: "cpuSkew" },
      heat,
      null,
      4,
    );
    expect(dAtFloor.nodeColors.get(nodeIdOf([5, 1, 1]))).toEqual("heat(0.00)");

    // One worker does everything, the other three do nothing (but are
    // present, not absent): ratio hits exactly the 4-worker ceiling
    // (400 / (400/4) = 4), so this must read as fully hot.
    const atCeiling = buildDataflowStructure(OPS, CHANNELS, LIR_SPANS, [
      { id: "12", workerId: "0", elapsedNs: "400", arrangementSize: "0" },
      { id: "12", workerId: "1", elapsedNs: "0", arrangementSize: "0" },
      { id: "12", workerId: "2", elapsedNs: "0", arrangementSize: "0" },
      { id: "12", workerId: "3", elapsedNs: "0", arrangementSize: "0" },
    ]);
    const dAtCeiling = decorateGraph(
      deriveVisibleGraph(atCeiling, regionId),
      { ...DEFAULT_FILTERS, heatmap: "cpuSkew" },
      heat,
      null,
      4,
    );
    expect(dAtCeiling.nodeColors.get(nodeIdOf([5, 1, 1]))).toEqual(
      "heat(1.00)",
    );
  });

  it("skew heatmap color for a fixed ratio does not depend on what else is in view", () => {
    const buildWithMapData = (mapRows: PerWorkerStatRow[]) =>
      decorateGraph(
        deriveVisibleGraph(
          buildDataflowStructure(OPS, CHANNELS, LIR_SPANS, [
            { id: "12", workerId: "0", elapsedNs: "100", arrangementSize: "0" },
            { id: "12", workerId: "1", elapsedNs: "200", arrangementSize: "0" },
            ...mapRows,
          ]),
          regionId,
        ),
        { ...DEFAULT_FILTERS, heatmap: "cpuSkew" },
        heat,
        null,
        4,
      );
    const mildCompanion = buildWithMapData([
      { id: "13", workerId: "0", elapsedNs: "100", arrangementSize: "0" },
      { id: "13", workerId: "1", elapsedNs: "105", arrangementSize: "0" },
    ]);
    const extremeCompanion = buildWithMapData([
      { id: "13", workerId: "0", elapsedNs: "1", arrangementSize: "0" },
      { id: "13", workerId: "1", elapsedNs: "1000", arrangementSize: "0" },
    ]);
    // Join's own ratio (200/150) never changes; its color must be identical
    // regardless of Map's ratio, since the domain is fixed at
    // [1, workerCount] rather than derived from whatever else is in view.
    expect(mildCompanion.nodeColors.get(nodeIdOf([5, 1, 1]))).toEqual(
      extremeCompanion.nodeColors.get(nodeIdOf([5, 1, 1])),
    );
  });

  // A negligible-magnitude operator's raw skew is suppressed to the "no
  // data" sentinel at the source (buildDataflowStructure's ownSkew, see
  // dataflowGraph.test.ts's "skew" describe block) rather than filtered
  // here, so decorateGraph itself no longer needs, or has, any
  // magnitude-aware logic of its own to test.
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
      scheduleCount: 0n,
    });
    expect(root.children[0].summary).toEqual({
      arrangementRecords: 100n,
      arrangementSize: 0n,
      elapsedNs: 10n,
      scheduleCount: 0n,
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
    own: null,
    ownSkew: null,
    transitiveSkew: null,
    overheadNs: null,
    childCount: 0,
    lir: [],
    address: null,
    operatorId: null,
    peers: [],
    direction: null,
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
    sourceLandings: [],
    targetLandings: [],
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
        sourceLandings: [],
        targetLandings: [],
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
    own: null,
    ownSkew: null,
    transitiveSkew: null,
    overheadNs: null,
    childCount: 0,
    lir,
    address: null,
    operatorId: BigInt(operatorId),
    peers: [],
    direction: null,
  });
  const port = (id: string): VisibleNode => ({
    id,
    kind: "port",
    label: id,
    stats: null,
    transitive: null,
    own: null,
    ownSkew: null,
    transitiveSkew: null,
    overheadNs: null,
    childCount: 0,
    lir: [],
    address: null,
    operatorId: null,
    peers: [],
    direction: null,
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
