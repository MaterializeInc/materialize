// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { getMaterializeClient } from "~/test/sql/materializeSqlClient";
import { testdrive } from "~/test/sql/mzcompose";

import { RELATION_OIDS } from "./privilegeTable";

describe("RELATION_OIDS", () => {
  it("should match the OIDs in the system catalog", async () => {
    // Each SQL test must start with a testdrive call to reset database state between tests.
    await testdrive(`> SELECT 1 LIMIT 0`);
    const client = await getMaterializeClient();
    try {
      for (const [name, oid] of Object.entries(RELATION_OIDS)) {
        const result = await client.query(
          "SELECT oid::text FROM mz_objects WHERE name = $1",
          [name],
        );
        expect(result.rows[0]?.oid).toBe(String(oid));
      }
    } finally {
      await client.end();
    }
  });
});
