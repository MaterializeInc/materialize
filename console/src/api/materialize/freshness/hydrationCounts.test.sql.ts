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
  QUICKSTART_CLUSTER,
} from "~/test/sql/materializeSqlClient";
import { testdrive } from "~/test/sql/mzcompose";

import { buildHydrationCountsQuery } from "./hydrationCounts";

describe("buildHydrationCountsQuery", () => {
  it("counts each replica once when an id repeats", async () => {
    await testdrive(`
      > CREATE SCHEMA hydration_test;
      > SET schema = hydration_test;
      > CREATE TABLE mz_hydration_statuses (
          object_id text NOT NULL,
          replica_id text NOT NULL,
          hydrated boolean NOT NULL
        );
      > INSERT INTO mz_hydration_statuses VALUES
          ('u1', 'u1', true),
          ('u1', 'u2', false),
          ('u2', 'u1', true);
    `);

    const { rows } = await executeSqlHttp(
      buildHydrationCountsQuery(["u1", "u1"]).compile(),
      {
        sessionVariables: {
          search_path: `hydration_test, ${SEARCH_PATH}`,
          cluster: QUICKSTART_CLUSTER,
        },
      },
    );

    expect(rows).toEqual([
      {
        objectId: "u1",
        totalReplicas: BigInt(2),
        hydratedReplicas: BigInt(1),
      },
    ]);
  });
});
