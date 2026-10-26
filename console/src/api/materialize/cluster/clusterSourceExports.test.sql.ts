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
 * Source u1 runs on cluster c1 and ingests into subsource s1 and table t1.
 * Source u2 runs on cluster c2 and ingests into table t2. Table t3 is written
 * with INSERT, and p1 is u1's progress collection.
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
      ('u1', 'postgres', 'c1'),
      ('u2', 'postgres', 'c2'),
      ('s1', 'subsource', NULL),
      ('p1', 'progress', NULL);
  > INSERT INTO ${TEST_SCHEMA}.mz_object_dependencies VALUES
      ('s1', 'u1'),
      ('u1', 'p1'),
      ('t1', 'u1'),
      ('t2', 'u2');
  > INSERT INTO ${TEST_SCHEMA}.mz_tables VALUES
      ('t1', 'u1'),
      ('t2', 'u2'),
      ('t3', NULL);
`;

describe("buildClusterSourceExportsQuery", () => {
  it("returns the subsources and tables of the cluster's sources only", async () => {
    await testdrive(SEED);

    const result = await executeSqlHttp(
      buildClusterSourceExportsQuery("c1").compile(),
      {
        sessionVariables: {
          cluster: QUICKSTART_CLUSTER,
          search_path: `${TEST_SCHEMA}, ${SEARCH_PATH}`,
        },
      },
    );

    expect(result.rows.map((row) => row.exportId).sort()).toEqual(["s1", "t1"]);
  });
});
