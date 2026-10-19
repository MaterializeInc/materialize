// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { parseAutoScaling } from "~/platform/clusters/ClusterPage/Configuration/clusterConfigurationModel";
import {
  executeSqlHttp,
  getMaterializeClient,
} from "~/test/sql/materializeSqlClient";
import { testdrive } from "~/test/sql/mzcompose";

import { buildClusterAutoScalingQuery } from "./clusterConfiguration";
import { buildObjectCreationTimesQuery } from "./objectCreationTimes";

async function clusterIdByName(name: string) {
  const client = await getMaterializeClient();
  const {
    rows: [cluster],
  } = await client.query("SELECT id FROM mz_clusters WHERE name = $1", [name]);
  return cluster.id as string;
}

describe("cluster configuration queries", () => {
  it("tells a cluster's creation from a replica's with the same id", async () => {
    await testdrive(`> SET cluster to quickstart;`);
    const client = await getMaterializeClient();
    // Clusters and replicas take ids from separate counters, so the first of
    // each share an id.
    const {
      rows: [shared],
    } = await client.query(
      `SELECT c.id FROM mz_clusters c JOIN mz_cluster_replicas r ON r.id = c.id LIMIT 1`,
    );
    expect(shared?.id).toBeDefined();

    const { rows } = await executeSqlHttp(
      buildObjectCreationTimesQuery({
        objectIds: [shared.id],
        objectTypes: ["cluster", "cluster-replica"],
      }).compile(),
    );

    expect(rows.map(({ id, objectType }) => [id, objectType]).sort()).toEqual([
      [shared.id, "cluster"],
      [shared.id, "cluster-replica"],
    ]);
    expect(rows.every(({ occurredAt }) => occurredAt instanceof Date)).toBe(
      true,
    );
  });

  it("reads a cluster's autoscaling policy as the drawer parses it", async () => {
    await testdrive(`
      $ postgres-execute connection=postgres://mz_system@\${testdrive.materialize-internal-sql-addr}/materialize
      ALTER SYSTEM SET enable_auto_scaling_strategy = true

      > CREATE CLUSTER lingering (SIZE 'scale=1,workers=1', AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE = 'scale=1,workers=2', LINGER DURATION = '120s')));
      > CREATE CLUSTER defaulted (SIZE 'scale=1,workers=1', AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE = 'scale=1,workers=2')));
      > CREATE CLUSTER steady (SIZE 'scale=1,workers=1');
    `);

    const policy = async (name: string) => {
      const { rows } = await executeSqlHttp(
        buildClusterAutoScalingQuery(await clusterIdByName(name)).compile(),
      );
      return parseAutoScaling(rows.at(0) ?? null).policy;
    };

    expect(await policy("lingering")).toEqual({
      hydrationSize: "scale=1,workers=2",
      lingerMs: 120_000,
    });
    // An omitted linger arrives as a null duration, the server's default.
    expect(await policy("defaulted")).toEqual({
      hydrationSize: "scale=1,workers=2",
      lingerMs: undefined,
    });
    expect(await policy("steady")).toBeNull();
  });
});
