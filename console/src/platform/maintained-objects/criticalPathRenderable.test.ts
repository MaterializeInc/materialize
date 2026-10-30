// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import parse from "postgres-interval";

import { CriticalPathRow } from "~/api/materialize/maintained-objects/criticalPath";

import {
  buildGraphView,
  buildSourceExportsView,
  DEFAULT_VISIBLE_EXPORTS,
} from "./criticalPathRenderable";
import { CriticalPathData, MaintainedObjectListItem } from "./queries";

const buildRow = (
  overrides: Partial<CriticalPathRow> = {},
): CriticalPathRow => ({
  id: "u1",
  childId: "probe",
  name: "input1",
  objectType: "materialized-view",
  schemaName: "public",
  databaseName: "materialize",
  clusterId: "u1",
  clusterName: "default",
  lag: parse("00:00:01"),
  targetLag: parse("00:00:10"),
  isBottleneck: true,
  isIngestion: false,
  peakLag: null,
  peakLagAt: null,
  ...overrides,
});

const buildProbe = (
  overrides: Partial<MaintainedObjectListItem> = {},
): MaintainedObjectListItem => ({
  id: "probe",
  name: "probe_object",
  schemaName: "public",
  databaseName: "materialize",
  objectType: "materialized-view",
  sourceType: null,
  parentSource: null,
  cluster: null,
  hydratedReplicas: 1,
  totalReplicas: 1,
  lag: { value: parse("00:00:10"), ms: 10_000 },
  sourceStatus: null,
  ...overrides,
});

const buildData = (rows: CriticalPathRow[]): CriticalPathData => ({
  rows,
  directInputs: rows.filter((r) => r.childId === "probe"),
});

describe("buildGraphView", () => {
  it("walks a linear chain", () => {
    // probe(10s) ← a(8s) ← b(5s)
    const data = buildData([
      buildRow({
        id: "a",
        childId: "probe",
        name: "a",
        lag: parse("00:00:08"),
      }),
      buildRow({ id: "b", childId: "a", name: "b", lag: parse("00:00:05") }),
    ]);
    const view = buildGraphView(data, buildProbe(), null, 5);

    expect(view.nodes.map((n) => n.id).sort()).toEqual(["a", "b", "probe"]);
    expect(view.edges).toHaveLength(2);
    expect(view.hiddenChainCount).toBe(0);
  });

  it("hides chain nodes past visibleDepth and reports hiddenChainCount", () => {
    // probe ← a ← b ← c (depths 1, 2, 3); visibleDepth=2 hides c.
    const data = buildData([
      buildRow({ id: "a", childId: "probe", name: "a" }),
      buildRow({ id: "b", childId: "a", name: "b" }),
      buildRow({ id: "c", childId: "b", name: "c" }),
    ]);
    const view = buildGraphView(data, buildProbe(), null, 2);

    expect(view.nodes.map((n) => n.id).sort()).toEqual(["a", "b", "probe"]);
    expect(view.hiddenChainCount).toBe(1);
  });

  it("flags the chain node with the largest self-delay as primary", () => {
    // probe(10s) ← a(8s) ← b(2s)
    // self-delays: probe=2, a=6, b=2 → a wins.
    const data = buildData([
      buildRow({
        id: "a",
        childId: "probe",
        name: "a",
        lag: parse("00:00:08"),
      }),
      buildRow({ id: "b", childId: "a", name: "b", lag: parse("00:00:02") }),
    ]);
    const view = buildGraphView(data, buildProbe(), null, 5);

    expect(view.nodes.find((n) => n.id === "a")?.kind).toBe("primary");
    expect(view.nodes.find((n) => n.id === "probe")?.kind).not.toBe("primary");
  });

  it("classifies a probe with lag within 2s as healthy, not primary", () => {
    // Even though the probe is the only candidate (largest self-delay), the
    // healthy threshold takes precedence — we don't flag bottlenecks on
    // objects that are within the freshness target.
    const view = buildGraphView(
      buildData([]),
      buildProbe({ lag: { value: parse("00:00:01"), ms: 1_000 } }),
      null,
      5,
    );
    expect(view.nodes.find((n) => n.id === "probe")?.kind).toBe("healthy");
  });

  it("returns just the probe when there is no chain", () => {
    // E.g. a source with no in-Materialize upstream — the probe ends up
    // labelled as its own primary because its lag is past the healthy bar.
    const view = buildGraphView(buildData([]), buildProbe(), null, 5);

    expect(view.nodes).toHaveLength(1);
    expect(view.nodes[0].id).toBe("probe");
    expect(view.nodes[0].kind).toBe("primary");
    expect(view.edges).toHaveLength(0);
    expect(view.hiddenChainCount).toBe(0);
  });

  it("counts off-path siblings on the chain node", () => {
    // `a` is the chain node; sib1 and sib2 also feed probe but are off-path.
    const data = buildData([
      buildRow({ id: "a", childId: "probe", name: "a", isBottleneck: true }),
      buildRow({
        id: "sib1",
        childId: "probe",
        name: "sib1",
        isBottleneck: false,
      }),
      buildRow({
        id: "sib2",
        childId: "probe",
        name: "sib2",
        isBottleneck: false,
      }),
    ]);
    const view = buildGraphView(data, buildProbe(), null, 5);

    expect(view.nodes.find((n) => n.id === "a")?.offPathCount).toBe(2);
    // Off-path siblings aren't rendered until the chain node is expanded.
    expect(view.nodes.find((n) => n.id === "sib1")).toBeUndefined();
  });

  it("splices off-path siblings in when a bottleneck is expanded", () => {
    const data = buildData([
      buildRow({ id: "a", childId: "probe", name: "a", isBottleneck: true }),
      buildRow({
        id: "sib1",
        childId: "probe",
        name: "sib1",
        isBottleneck: false,
      }),
      buildRow({
        id: "sib2",
        childId: "probe",
        name: "sib2",
        isBottleneck: false,
      }),
    ]);
    const view = buildGraphView(data, buildProbe(), "a", 5);

    expect(view.nodes.find((n) => n.id === "sib1")?.kind).toBe("offPath");
    expect(view.nodes.find((n) => n.id === "sib2")?.kind).toBe("offPath");
    // Chain edge a→probe + two off-path edges.
    expect(view.edges).toHaveLength(3);
  });
});

describe("buildSourceExportsView", () => {
  const BUCKET_MS = 60_000;
  const ANCHOR = new Date("2026-10-22T12:00:00Z");
  const source = buildProbe({
    id: "src",
    name: "pg",
    objectType: "source",
    lag: { value: parse("00:00:01"), ms: 1_000 },
  });
  const table = (id: string) =>
    buildProbe({ id, name: id, objectType: "table", parentSource: source });
  const reading = (
    objectId: string,
    lag: string | null,
    bucketStart = ANCHOR,
  ) => ({ objectId, bucketStart, lag: lag === null ? null : parse(lag) });
  const view = (
    exports: MaintainedObjectListItem[],
    readings: ReturnType<typeof reading>[],
    showAll = false,
  ) =>
    buildSourceExportsView({
      probe: source,
      exports,
      readings,
      anchor: ANCHOR,
      bucketSizeMs: BUCKET_MS,
      showAll,
    });

  it("marks the export that trails its source the most as primary", () => {
    const { nodes, edges } = view(
      [table("ok"), table("slow")],
      [reading("slow", "00:01:00"), reading("ok", "00:00:01")],
    );

    expect(nodes.map((n) => [n.id, n.kind])).toEqual([
      ["src", "healthy"],
      ["slow", "primary"],
      ["ok", "healthy"],
    ]);
    expect(edges).toEqual([
      { id: "src", childId: "slow", isBottleneck: true, isIngestion: true },
      { id: "src", childId: "ok", isBottleneck: true, isIngestion: true },
    ]);
  });

  it("marks the source primary when no export trails it", () => {
    const { nodes } = view(
      [table("t")],
      [reading("src", "00:00:30"), reading("t", "00:00:30")],
    );

    expect(nodes.find((n) => n.id === "src")?.kind).toBe("primary");
    expect(nodes.find((n) => n.id === "t")?.kind).toBe("upstream");
  });

  it("reads the source's lag from the anchor bucket, not the list", () => {
    // The list's pMAX can come from a later minute than the anchor bucket.
    const { nodes } = view([table("t")], [reading("src", "00:00:03")]);

    expect(nodes[0].lag?.toPostgres()).toBe("3 seconds");
  });

  it("reads each export's lag from the bucket containing the anchor", () => {
    const earlier = new Date(ANCHOR.getTime() - BUCKET_MS);
    const { nodes } = view(
      [table("t")],
      [reading("t", "00:05:00", earlier), reading("t", "00:00:02")],
    );

    const node = nodes.find((n) => n.id === "t");
    expect(node?.lag?.toPostgres()).toBe("2 seconds");
    expect(node?.peakLag?.toPostgres()).toBe("5 minutes");
    expect(node?.peakLagAt).toEqual(earlier);
  });

  it("keeps an unreadable export in view past the cap", () => {
    // A snapshotting table reads NULL, the worst lag, so the cap must not
    // hide it behind exports that merely lag.
    const lagging = Array.from({ length: DEFAULT_VISIBLE_EXPORTS }, (_, i) =>
      table(`t${i}`),
    );
    const { nodes, hiddenChainCount } = view(
      [...lagging, table("snapshotting")],
      [
        ...lagging.map((t) => reading(t.id, "00:00:10")),
        reading("snapshotting", null),
      ],
    );

    expect(nodes[1].id).toBe("snapshotting");
    expect(hiddenChainCount).toBe(1);
  });

  it("hides exports past the visible count until expanded", () => {
    const exports = Array.from(
      { length: DEFAULT_VISIBLE_EXPORTS + 2 },
      (_, i) => table(`t${i}`),
    );

    const collapsed = view(exports, []);
    expect(collapsed.nodes).toHaveLength(DEFAULT_VISIBLE_EXPORTS + 1);
    expect(collapsed.hiddenChainCount).toBe(2);

    const expanded = view(exports, [], true);
    expect(expanded.nodes).toHaveLength(exports.length + 1);
    expect(expanded.hiddenChainCount).toBe(0);
  });

  it("draws the source alone when it has no exports", () => {
    const { nodes, edges } = view([], []);

    expect(nodes.map((n) => n.id)).toEqual(["src"]);
    expect(edges).toEqual([]);
  });
});
