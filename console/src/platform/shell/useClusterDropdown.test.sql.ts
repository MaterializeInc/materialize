// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { queryBuilder } from "~/api/materialize";
import { buildSubscribeQuery } from "~/api/materialize/buildSubscribeQuery";
import { buildClustersQuery } from "~/api/materialize/cluster/clusterList";
import { getMaterializeClient } from "~/test/sql/materializeSqlClient";
import { testdrive } from "~/test/sql/mzcompose";

describe("useClustersDropdown subscribe", () => {
  it("subscribes to non-system clusters", async () => {
    // reset Materialize state
    await testdrive(`> SET cluster to quickstart;`);
    const client = await getMaterializeClient();
    const { sql, parameters } = buildSubscribeQuery(
      buildClustersQuery({
        includeSystemObjects: true,
        queryOwnership: false,
      }),
      { upsertKey: "id" },
    ).compile(queryBuilder);
    await client.query("BEGIN");
    await client.query(`DECLARE c CURSOR FOR ${sql}`, parameters as string[]);
    const result = await client.query(`FETCH ALL c WITH (timeout='1s')`);
    await client.query("COMMIT");
    const clusters = result.rows.filter((r) => r["mz_state"] === "upsert");
    expect(clusters).toContainEqual({
      mz_timestamp: expect.any(String),
      mz_progressed: false,
      mz_state: "upsert",
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
    });
  });
});
