// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { executeSqlHttp } from "~/test/sql/materializeSqlClient";
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
});
