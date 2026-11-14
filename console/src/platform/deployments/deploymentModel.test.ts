// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  baselineDeploys,
  ClusterHealth,
  clusterVerdict,
  DeploymentRow,
  diffObjects,
  formatDuration,
  groupDeployments,
  liveDeployIds,
  promoteBlockers,
  SINK_STUCK_AFTER_MS,
  sinkCutover,
  SinkCutoverRow,
} from "./deploymentModel";

const row = (overrides: Partial<DeploymentRow>): DeploymentRow => ({
  deployId: "a1",
  deployedAt: new Date("2026-11-13T10:00:00Z"),
  promotedAt: null,
  database: "materialize",
  schema: "analytics",
  deployedBy: "deployer",
  commit: "a1b2",
  kind: "objects",
  mode: "stage",
  ...overrides,
});

const health = (overrides: Partial<ClusterHealth>): ClusterHealth => ({
  id: "u1",
  name: "compute_a1",
  size: "50cc",
  replicationFactor: 1,
  replicas: 1,
  oomLoopingReplicas: 0,
  hydrationReported: true,
  hydrated: 4,
  total: 4,
  memoryPercent: 40,
  maxLagMs: 1_000,
  ...overrides,
});

const sink = (overrides: Partial<SinkCutoverRow>): SinkCutoverRow => ({
  sinkId: "u9",
  sinkDatabase: "materialize",
  sinkSchema: "egress",
  sinkName: "orders_kafka",
  status: "running",
  error: null,
  dependencySchema: "analytics",
  dependencyName: "orders",
  hasReplacement: true,
  sinkWriteFrontierMs: 10_000,
  replacementReadFrontierMs: 9_000,
  sinkLagMs: 1_000,
  ...overrides,
});

describe("groupDeployments", () => {
  it("folds one row per schema into one deploy, newest first", () => {
    const deployments = groupDeployments([
      row({ deployId: "old", deployedAt: new Date("2026-11-01T00:00:00Z") }),
      row({ schema: "serving" }),
      row({ schema: "analytics" }),
    ]);
    expect(deployments.map((d) => d.deployId)).toEqual(["a1", "old"]);
    expect(deployments[0].schemas.map((s) => s.schema)).toEqual([
      "analytics",
      "serving",
    ]);
  });
});

describe("clusterVerdict", () => {
  it("follows mz-deploy's order: failing, hydrating, lagging, ready", () => {
    expect(clusterVerdict(undefined).status).toBe("failing");
    expect(clusterVerdict(health({ replicas: 0 })).status).toBe("failing");
    expect(
      clusterVerdict(health({ replicas: 2, oomLoopingReplicas: 2 })).status,
    ).toBe("failing");
    expect(
      clusterVerdict(health({ replicas: 2, oomLoopingReplicas: 1 })).status,
    ).toBe("ready");
    expect(
      clusterVerdict(
        health({ hydrationReported: false, hydrated: 0, total: 0 }),
      ).status,
    ).toBe("hydrating");
    expect(clusterVerdict(health({ hydrated: 3 })).detail).toBe(
      "3 of 4 objects hydrated",
    );
    expect(clusterVerdict(health({ maxLagMs: 301_000 })).status).toBe(
      "lagging",
    );
    expect(clusterVerdict(health({ maxLagMs: null })).status).toBe("ready");
  });
});

describe("sinkCutover", () => {
  it("is ready once the sink has written past the replacement's read frontier", () => {
    expect(sinkCutover(sink({}), 0)).toEqual({ state: "ready", gapMs: 0 });
  });

  it("is behind while the sink trails but still advances", () => {
    expect(
      sinkCutover(
        sink({ sinkWriteFrontierMs: 4_000, replacementReadFrontierMs: 9_000 }),
        5_000,
      ),
    ).toEqual({ state: "behind", gapMs: 5_000 });
  });

  it("is stuck when the sink trails and its frontier has stopped", () => {
    expect(
      sinkCutover(
        sink({ sinkWriteFrontierMs: 4_000, replacementReadFrontierMs: 9_000 }),
        SINK_STUCK_AFTER_MS,
      ).state,
    ).toBe("stuck");
  });

  it("can't tell without a sink write frontier", () => {
    expect(sinkCutover(sink({ sinkWriteFrontierMs: null }), 0)).toEqual({
      state: "unknown",
      gapMs: 0,
    });
  });

  it("flags a sink whose input is not staged by the deploy", () => {
    expect(sinkCutover(sink({ hasReplacement: false }), 0).state).toBe(
      "no-replacement",
    );
  });
});

describe("diffObjects", () => {
  it("classifies against each schema's baseline deploy", () => {
    const rows = [
      { deployId: "new", object: "a", hash: "1" },
      { deployId: "new", object: "b", hash: "2" },
      { deployId: "new", object: "c", hash: "3" },
      { deployId: "old", object: "a", hash: "1" },
      { deployId: "old", object: "b", hash: "x" },
      { deployId: "old", object: "gone", hash: "4" },
      { deployId: "older", object: "c", hash: "3" },
    ].map((r) => ({ ...r, database: "materialize", schema: "analytics" }));

    const diff = diffObjects(
      "new",
      rows,
      new Map([["materialize.analytics", "old"]]),
    );
    expect(diff.map((d) => [d.object, d.change])).toEqual([
      ["a", "unchanged"],
      ["b", "changed"],
      ["c", "added"],
      ["gone", "removed"],
    ]);
  });
});

describe("baselineDeploys", () => {
  const deployments = groupDeployments([
    row({
      deployId: "first",
      deployedAt: new Date("2026-11-01T00:00:00Z"),
      promotedAt: new Date("2026-11-01T01:00:00Z"),
    }),
    // Staged before "second" but promoted after it.
    row({
      deployId: "slow",
      deployedAt: new Date("2026-11-01T12:00:00Z"),
      promotedAt: new Date("2026-11-03T00:00:00Z"),
    }),
    row({
      deployId: "second",
      deployedAt: new Date("2026-11-02T00:00:00Z"),
      promotedAt: new Date("2026-11-02T01:00:00Z"),
    }),
    row({ deployId: "pending", deployedAt: new Date("2026-11-01T06:00:00Z") }),
  ]);
  const byId = (id: string) => {
    const found = deployments.find((d) => d.deployId === id);
    if (!found) throw new Error(`no deploy ${id}`);
    return found;
  };

  it("compares a promoted deploy with the one live when it was promoted", () => {
    expect(baselineDeploys(byId("slow"), deployments)).toEqual(
      new Map([["materialize.analytics", "second"]]),
    );
  });

  it("compares an in-progress deploy with the one live now", () => {
    expect(baselineDeploys(byId("pending"), deployments)).toEqual(
      new Map([["materialize.analytics", "slow"]]),
    );
  });
});

describe("liveDeployIds", () => {
  it("marks the latest promote per schema, not the latest staged deploy", () => {
    const deployments = groupDeployments([
      row({
        deployId: "x-old",
        deployedAt: new Date("2026-11-01T00:00:00Z"),
        promotedAt: new Date("2026-11-05T00:00:00Z"),
      }),
      row({
        deployId: "x-new",
        deployedAt: new Date("2026-11-02T00:00:00Z"),
        promotedAt: new Date("2026-11-03T00:00:00Z"),
      }),
      row({
        deployId: "y",
        schema: "serving",
        deployedAt: new Date("2026-11-06T00:00:00Z"),
        promotedAt: new Date("2026-11-06T01:00:00Z"),
      }),
      row({ deployId: "staged", deployedAt: new Date("2026-11-07T00:00:00Z") }),
    ]);
    expect([...liveDeployIds(deployments)].sort()).toEqual(["x-old", "y"]);
  });
});

describe("promoteBlockers", () => {
  it("lists unready clusters and sinks that would stall the cutover", () => {
    const blockers = promoteBlockers(
      [
        {
          liveName: "compute",
          stagingName: "compute_a1",
          verdict: { status: "hydrating", detail: "3 of 4 objects hydrated" },
        },
        {
          liveName: "serve",
          stagingName: "serve_a1",
          verdict: { status: "ready", detail: "Hydrated and caught up" },
        },
      ],
      [{ row: sink({}), cutover: { state: "stuck", gapMs: 270_000 } }],
    );
    expect(blockers.map((b) => b.name)).toEqual([
      "compute_a1",
      "egress.orders_kafka",
    ]);
  });
});

describe("formatDuration", () => {
  it("picks a compact unit", () => {
    expect(formatDuration(42_400)).toBe("42s");
    expect(formatDuration(270_000)).toBe("4m 30s");
    expect(formatDuration(8_040_000)).toBe("2h 14m");
    expect(formatDuration(4.8 * 86_400_000)).toBe("4.8 days");
  });
});
