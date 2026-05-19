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

import { buildGraphView } from "./criticalPathRenderable";
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
  parentSourceId: null,
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
  cluster: null,
  hydratedReplicas: 1,
  totalReplicas: 1,
  lag: { value: parse("00:00:10"), ms: 10_000 },
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
