// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { buildClusterReplicaMetricsQuery } from "./clusterReplicaMetrics";

describe("fetchClusterReplicaMetrics", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime("1999-09-03");
  });

  it("should throw if start date is after end date in dateRange", () => {
    const { sql, parameters } = buildClusterReplicaMetricsQuery({
      clusterId: "u1",
    }).compile();

    expect({ sql, parameters }).toMatchSnapshot();
  });

  // The indexed view only exists on mz >= 26.32, and reading it on an older
  // environment errors the Replicas tab and the cluster metrics card rather
  // than degrading. useReplicaUtilizationHistory gates the charts on the same
  // version.
  describe("utilization source by environment version", () => {
    const sqlFor = (environmentVersion?: string) =>
      buildClusterReplicaMetricsQuery({ clusterId: "u1", environmentVersion })
        .compile()
        .sql.replaceAll('"', "");

    it("reads the indexed view on 26.32 and later", () => {
      for (const version of ["26.32.0", "26.44.0", undefined]) {
        expect(sqlFor(version)).toContain(
          "mz_console_cluster_utilization_overview_3h",
        );
      }
    });

    it("falls back to mz_cluster_replica_utilization before 26.32", () => {
      for (const version of ["0.161.0", "26.31.0"]) {
        const sql = sqlFor(version);
        expect(sql).toContain("mz_cluster_replica_utilization");
        expect(sql).not.toContain("mz_console_cluster_utilization_overview_3h");
      }
    });

    // Kysely's .select appends rather than replaces, so a tier layering its
    // own heap_percent over the shared aggregates emitted the column twice:
    // unknown below 0.161.0, and an ambiguous reference above it.
    it("selects heap_percent exactly once in the fallback", () => {
      const sql = sqlFor("26.31.0");
      expect(sql).toContain("MAX(cru.heap_percent) as heap_percent");
      expect(sql.match(/as heap_percent/g)).toHaveLength(1);
    });

    // Grouping by process_id too would put one row in every group, leaving
    // COUNT at 1, SUM(x)/COUNT(x) equal to x, and one row per process fanning
    // out the join.
    it("aggregates the fallback across processes, not per process", () => {
      const sql = sqlFor("26.31.0");
      expect(sql).toContain("group by replica_id");
      expect(sql).not.toContain("process_id,");
    });
  });
});
