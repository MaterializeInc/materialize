// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  executeSqlHttp,
  getMaterializeClient,
} from "~/test/sql/materializeSqlClient";
import { testdrive } from "~/test/sql/mzcompose";

import { buildClustersQuery } from "./clusterList";

describe("buildClusterSubscribe", () => {
  it("with all options", async () => {
    // reset Materialize state
    await testdrive(`> SET cluster to quickstart;`);
    const query = buildClustersQuery({
      queryOwnership: true,
      includeSystemObjects: true,
    }).compile();
    const result = await executeSqlHttp(query);
    const clusterNames = result.rows.map((r) => r.name);
    expect(clusterNames).toContain("mz_system");
    expect(clusterNames).toContain("mz_catalog_server");
    expect(result.rows.find((r) => r.name == "quickstart")).toEqual({
      id: expect.any(String),
      isOwner: true,
      name: "quickstart",
      disk: true,
      managed: true,
      size: "bootstrap",
      replicas: [
        {
          disk: true,
          id: expect.any(String),
          name: "r1",
          size: "bootstrap",
          statuses: [
            {
              process_id: 0,
              reason: null,
              replica_id: "u1",
              status: "online",
              updated_at: expect.any(String),
            },
          ],
        },
      ],
      latestStatusUpdate: expect.any(Date),
    });
  });

  it("with options disabled", async () => {
    // reset Materialize state
    await testdrive(`> SET cluster to quickstart;`);
    const query = buildClustersQuery({
      queryOwnership: false,
      includeSystemObjects: false,
    }).compile();
    const result = await executeSqlHttp(query);
    expect(result.rows).toEqual([
      {
        id: expect.any(String),
        name: "quickstart",
        disk: true,
        managed: true,
        size: "bootstrap",
        replicas: [
          {
            disk: true,
            id: expect.any(String),
            name: "r1",
            size: "bootstrap",
            statuses: [
              {
                process_id: 0,
                reason: null,
                replica_id: "u1",
                status: "online",
                updated_at: expect.any(String),
              },
            ],
          },
        ],
        latestStatusUpdate: expect.any(Date),
      },
    ]);
  });

  it("correctly associates statuses with multiple replicas", async () => {
    const client = await getMaterializeClient();

    await testdrive(
      `> CREATE CLUSTER test_multi (SIZE 'scale=1,workers=1', REPLICATION FACTOR 2);`,
    );

    try {
      const {
        rows: [cluster],
      } = await client.query<{ id: string }>(
        "SELECT id FROM mz_clusters WHERE name = 'test_multi'",
      );
      const { rows: replicas } = await client.query<{
        id: string;
        name: string;
      }>(
        `SELECT id, name FROM mz_cluster_replicas WHERE cluster_id = '${cluster.id}' ORDER BY name`,
      );
      const [r1, r2] = replicas;

      // Wait for both replicas to have statuses
      const waitForStatuses = async () => {
        for (let i = 0; i < 30; i++) {
          const { rows } = await client.query<{ count: string }>(
            `SELECT COUNT(DISTINCT replica_id)::text as count FROM mz_cluster_replica_statuses WHERE replica_id IN ('${r1.id}', '${r2.id}')`,
          );
          if (rows[0].count === "2") return;
          await new Promise((resolve) => setTimeout(resolve, 1000));
        }
        throw new Error("Timed out waiting for replica statuses");
      };
      await waitForStatuses();

      const query = buildClustersQuery({
        queryOwnership: false,
        includeSystemObjects: false,
      }).compile();
      const result = await executeSqlHttp(query);

      const testCluster = result.rows.find((r) => r.name === "test_multi");
      expect(testCluster).toBeDefined();
      expect(testCluster!.replicas).toHaveLength(2);

      const replicaR1 = testCluster!.replicas.find((r) => r.name === "r1");
      const replicaR2 = testCluster!.replicas.find((r) => r.name === "r2");

      // Each replica's statuses should only contain its own replica_id
      expect(replicaR1!.statuses).toEqual([
        expect.objectContaining({ replica_id: r1.id }),
      ]);
      expect(replicaR2!.statuses).toEqual([
        expect.objectContaining({ replica_id: r2.id }),
      ]);
    } finally {
      await testdrive(`> DROP CLUSTER IF EXISTS test_multi;`);
    }
  });
});
