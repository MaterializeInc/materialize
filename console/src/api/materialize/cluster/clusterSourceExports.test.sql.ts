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
  QUICKSTART_CLUSTER,
} from "~/test/sql/materializeSqlClient";
import { testdrive } from "~/test/sql/mzcompose";

import { SEARCH_PATH } from "../executeSql";
import { buildClusterSourceExportsQuery } from "./clusterSourceExports";

const TEST_SCHEMA = "test_cluster_source_exports";

/**
 * Source u10 runs on cluster u1 and ingests into subsource u11 and table u13,
 * with u12 as its progress collection. Source u20 runs on cluster u2 and
 * ingests into table u21. Table u30 is written with INSERT.
 */
const SEED = `
  > DROP SCHEMA IF EXISTS ${TEST_SCHEMA} CASCADE;
  > CREATE SCHEMA ${TEST_SCHEMA};
  > CREATE TABLE ${TEST_SCHEMA}.mz_sources (
      id TEXT NOT NULL,
      type TEXT NOT NULL,
      cluster_id TEXT
    );
  > CREATE TABLE ${TEST_SCHEMA}.mz_object_dependencies (
      object_id TEXT NOT NULL,
      referenced_object_id TEXT NOT NULL
    );
  > CREATE TABLE ${TEST_SCHEMA}.mz_tables (
      id TEXT NOT NULL,
      source_id TEXT
    );
  > INSERT INTO ${TEST_SCHEMA}.mz_sources VALUES
      ('u10', 'postgres', 'u1'),
      ('u11', 'subsource', NULL),
      ('u12', 'progress', NULL),
      ('u20', 'postgres', 'u2');
  > INSERT INTO ${TEST_SCHEMA}.mz_object_dependencies VALUES
      ('u10', 'u12'),
      ('u11', 'u10'),
      ('u13', 'u10'),
      ('u21', 'u20');
  > INSERT INTO ${TEST_SCHEMA}.mz_tables VALUES
      ('u13', 'u10'),
      ('u21', 'u20'),
      ('u30', NULL);
`;

describe("buildClusterSourceExportsQuery", () => {
  it("returns the subsources and tables of the cluster's sources only", async () => {
    await testdrive(SEED);

    const result = await executeSqlHttp(
      buildClusterSourceExportsQuery("u1").compile(),
      {
        sessionVariables: {
          cluster: QUICKSTART_CLUSTER,
          search_path: `${TEST_SCHEMA}, ${SEARCH_PATH}`,
        },
      },
    );

    expect(result.rows.map((row) => row.exportId).sort()).toEqual([
      "u11",
      "u13",
    ]);
  });
});
