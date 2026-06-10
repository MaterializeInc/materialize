// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { SEARCH_PATH } from "~/api/materialize";
import {
  executeSqlHttp,
  getMaterializeClient,
  QUICKSTART_CLUSTER,
} from "~/test/sql/materializeSqlClient";
import { testdrive } from "~/test/sql/mzcompose";

import { buildLargestClusterReplicaQuery } from "./largestClusterReplica";

describe("buildLargestClusterReplicaQuery", () => {
  it("returns the largest replica", async () => {
    const mockedSearchPath = "internal_test, " + SEARCH_PATH;
    const client = await getMaterializeClient();

    await testdrive(`
      > CREATE CLUSTER c REPLICAS (r1 (SIZE 'scale=1,workers=1,mem=4GiB'), r2 (SIZE 'scale=1,workers=1,mem=8GiB'), r3 (size 'scale=1,workers=1,mem=4GiB'));
      > CREATE SCHEMA internal_test;
      > SET schema = internal_test;
      > CREATE TABLE mz_cluster_replica_metrics (
          replica_id TEXT NOT NULL,
          process_id uint8 NOT NULL,
          cpu_nano_cores double,
          memory_bytes double,
          disk_bytes double,
          heap_bytes double,
          heap_limit double
        );
      > CREATE TABLE mz_hydration_statuses (
          object_id TEXT NOT NULL,
          replica_id TEXT NOT NULL,
          hydrated boolean NOT NULL
        );
    `);

    const {
      rows: [cluster],
    } = await client.query("select id from mz_clusters where name = 'c'");
    const { rows: replicas } = await client.query(
      `SELECT id, name FROM mz_cluster_replicas WHERE cluster_id = '${cluster.id}' ORDER BY name`,
    );

    const [r1, r2, r3] = replicas as { id: string; name: string }[];

    // Mock metrics - r2 has the largest heap_limit (8GiB)
    await client.query(`
      INSERT INTO internal_test.mz_cluster_replica_metrics VALUES
        ('${r1.id}', 0, 0, 0, 0, 0, 4294967296),
        ('${r2.id}', 0, 0, 0, 0, 0, 8589934592),
        ('${r3.id}', 0, 0, 0, 0, 0, 4294967296)
    `);

    // Mock hydration statuses - all replicas are hydrated
    await client.query(`
      INSERT INTO internal_test.mz_hydration_statuses VALUES
        ('u1', '${r1.id}', true),
        ('u2', '${r2.id}', true),
        ('u3', '${r3.id}', true)
    `);

    await client.query(`SET search_path TO ${mockedSearchPath};`);

    const query = buildLargestClusterReplicaQuery(cluster.id).compile();
    const result = (
      await executeSqlHttp(query, {
        sessionVariables: {
          search_path: mockedSearchPath,
          cluster: QUICKSTART_CLUSTER,
        },
      })
    ).rows[0];

    expect(result.name).toBe("r2");
    expect(result.size).toBe("scale=1,workers=1,mem=8GiB");
    expect(result.heapLimit).toBe("8589934592");
    expect(result.isHydrated).toBe(true);
  });
});
