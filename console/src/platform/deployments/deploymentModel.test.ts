// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  ClusterHealth,
  clusterVerdict,
  DefinitionRow,
  DeployEventRow,
  diffDefinitions,
  finishCommands,
  formatDuration,
  groupDeployments,
  liveDeployKeys,
  normalizeCreateSql,
  promoteBlockers,
  SINK_STUCK_AFTER_MS,
  sinkCutover,
  SinkCutoverRow,
  StagedObjectRow,
} from "./deploymentModel";

const at = (iso: string) => new Date(`2026-11-${iso}Z`);

const staged = (overrides: Partial<StagedObjectRow>): StagedObjectRow => ({
  kind: "schema",
  id: "u20",
  database: "materialize",
  liveName: "analytics",
  stagedName: "analytics_a1",
  suffix: "a1",
  stagedAt: at("13T10:00:00"),
  stagedBy: "deployer",
  ...overrides,
});

const event = (
  overrides: Partial<DeployEventRow> & Pick<DeployEventRow, "state">,
): DeployEventRow => ({
  ...staged({}),
  endedAt: at("13T11:00:00"),
  endedBy: "deployer",
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
  deployId: "a1",
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
  commitIntervalMs: null,
  ...overrides,
});

describe("groupDeployments", () => {
  it("groups staged objects by suffix and promotes by swap", () => {
    const deployments = groupDeployments(
      [
        staged({}),
        staged({
          kind: "cluster",
          id: "u5",
          database: null,
          liveName: "compute",
          stagedName: "compute_a1",
        }),
      ],
      [
        event({ state: "promoted", suffix: "old", id: "u10" }),
        event({
          state: "promoted",
          suffix: "old",
          id: "u11",
          liveName: "serving",
        }),
      ],
      true,
    );
    expect(
      deployments.map((d) => [d.state, d.suffix, d.schemas.length]),
    ).toEqual([
      ["in-progress", "a1", 1],
      ["promoted", "old", 2],
    ]);
    expect(deployments[0].clusters.map((c) => c.stagedName)).toEqual([
      "compute_a1",
    ]);
    expect(deployments[0].tool).toBe("unknown");
  });

  it("keeps each dbt deploy apart although they share a suffix", () => {
    const deployments = groupDeployments(
      [],
      [
        event({
          state: "promoted",
          suffix: "dbt_deploy",
          endedAt: at("10T00:00:00"),
        }),
        event({
          state: "promoted",
          suffix: "dbt_deploy",
          endedAt: at("12T00:00:00"),
        }),
      ],
      false,
    );
    expect(deployments).toHaveLength(2);
    expect(deployments.every((d) => d.tool === "dbt")).toBe(true);
  });

  it("splits aborted drops of one suffix that are far apart", () => {
    const deployments = groupDeployments(
      [],
      [
        event({ state: "aborted", id: "u1", endedAt: at("13T10:00:00") }),
        event({ state: "aborted", id: "u2", endedAt: at("13T10:00:05") }),
        event({ state: "aborted", id: "u3", endedAt: at("13T12:00:00") }),
      ],
      false,
    );
    expect(deployments.map((d) => d.schemas.length)).toEqual([1, 2]);
  });
});

describe("normalizeCreateSql", () => {
  it("reads a staged definition as it will once promoted", () => {
    expect(
      normalizeCreateSql(
        'CREATE INDEX "i" IN CLUSTER [u15] ON [u521 AS "materialize"."serving_a1"."v"] ("x")',
        "serve_a1",
        new Map([
          ["serving_a1", "serving"],
          ["serve_a1", "serve"],
        ]),
      ),
    ).toBe(
      'CREATE INDEX "i" IN CLUSTER "serve" ON "materialize"."serving"."v" ("x")',
    );
  });
});

describe("normalizeCreateSql drops the creation timestamp", () => {
  it("compares two copies of a materialized view as equal", () => {
    const renames = new Map([["analytics_a1", "analytics"]]);
    expect(
      normalizeCreateSql(
        'CREATE MATERIALIZED VIEW "m"."analytics_a1"."v" AS SELECT 1 AS OF 1794603806000',
        null,
        renames,
      ),
    ).toBe(
      normalizeCreateSql(
        'CREATE MATERIALIZED VIEW "m"."analytics"."v" AS SELECT 1 AS OF 1794602409000',
        null,
        renames,
      ),
    );
  });
});

describe("diffDefinitions", () => {
  it("classifies staged objects against the live ones", () => {
    const [deployment] = groupDeployments([staged({})], [], false);
    const def = (
      schema: string,
      name: string,
      createSql: string,
    ): DefinitionRow => ({
      id: `${schema}.${name}`,
      database: "materialize",
      schema,
      name,
      type: "view",
      clusterName: null,
      createSql,
    });
    const diff = diffDefinitions(deployment, [
      def(
        "analytics",
        "a",
        'CREATE VIEW "materialize"."analytics"."a" AS SELECT 1',
      ),
      def("analytics", "b", "old"),
      def("analytics", "gone", "x"),
      def(
        "analytics_a1",
        "a",
        'CREATE VIEW "materialize"."analytics_a1"."a" AS SELECT 1',
      ),
      def("analytics_a1", "b", "new"),
      def("analytics_a1", "c", "y"),
    ]);
    expect(diff.map((d) => [d.object, d.change, d.schema])).toEqual([
      ["a", "unchanged", "analytics"],
      ["b", "changed", "analytics"],
      ["c", "added", "analytics"],
      ["gone", "removed", "analytics"],
    ]);
  });
});

describe("liveDeployKeys", () => {
  it("marks the latest promote per schema, not the latest staged deploy", () => {
    const deployments = groupDeployments(
      [],
      [
        event({
          state: "promoted",
          suffix: "x-old",
          stagedAt: at("01T00:00:00"),
          endedAt: at("05T00:00:00"),
        }),
        event({
          state: "promoted",
          suffix: "x-new",
          stagedAt: at("02T00:00:00"),
          endedAt: at("03T00:00:00"),
        }),
        event({
          state: "promoted",
          suffix: "y",
          id: "u30",
          liveName: "serving",
          endedAt: at("06T00:00:00"),
        }),
        event({ state: "aborted", suffix: "z", endedAt: at("07T00:00:00") }),
      ],
      false,
    );
    const live = liveDeployKeys(deployments);
    expect(
      deployments.filter((d) => live.has(d.key)).map((d) => d.suffix),
    ).toEqual(["y", "x-old"]);
  });
});

describe("finishCommands", () => {
  it("names the tool's own commands, and none for an unknown tool", () => {
    const hex = staged({ suffix: "91bb3bc", stagedName: "analytics_91bb3bc" });
    const [mzDeploy] = groupDeployments([hex], [], true);
    const [withoutTables] = groupDeployments([hex], [], false);
    const [customId] = groupDeployments([staged({})], [], true);
    expect(finishCommands(mzDeploy)?.promote).toBe("mz-deploy promote 91bb3bc");
    expect(finishCommands(withoutTables)).toBeNull();
    expect(finishCommands(customId)).toBeNull();
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
  const behind = {
    sinkWriteFrontierMs: 4_000,
    replacementReadFrontierMs: 9_000,
  };

  it("is ready once the sink has written past the replacement's read frontier", () => {
    expect(sinkCutover(sink({}), 0)).toEqual({ state: "ready", gapMs: 0 });
  });

  it("is behind while the sink trails but still advances", () => {
    expect(sinkCutover(sink(behind), 5_000)).toEqual({
      state: "behind",
      gapMs: 5_000,
    });
  });

  it("is stuck when the sink trails and its frontier has stopped", () => {
    expect(sinkCutover(sink(behind), SINK_STUCK_AFTER_MS).state).toBe("stuck");
  });

  it("gives a sink that commits on an interval that interval first", () => {
    const iceberg = sink({ ...behind, commitIntervalMs: 300_000 });
    expect(sinkCutover(iceberg, SINK_STUCK_AFTER_MS).state).toBe("behind");
    expect(sinkCutover(iceberg, SINK_STUCK_AFTER_MS + 300_000).state).toBe(
      "stuck",
    );
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
