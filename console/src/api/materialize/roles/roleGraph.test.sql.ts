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

import { buildRoleGraphEdgesQuery } from "./roleGraph";

describe("buildRoleGraphEdgesQuery", () => {
  /**
   * Role hierarchy being tested:
   *
   *                  ┌───────────┐
   *                  │ org_admin │
   *                  └─────┬─────┘
   *                        │
   *                        ▼
   *               ┌────────────────┐
   *               │ platform_admin │
   *               └───────┬────────┘
   *                       │
   *             ┌─────────┴─────────┐
   *             ▼                   ▼
   *      ┌──────────────┐   ┌─────────────┐
   *      │app_team_alpha│   │app_team_beta│
   *      └──────┬───────┘   └──────┬──────┘
   *             │                  │
   *        ┌────┴────┐             │
   *        ▼         ▼             ▼
   *     ┌─────────────────┐ ┌────────────────┐ ┌───────────────┐
   *     │app_team_alpha_ro│ │svc_alpha_ingest│ │app_team_beta_ro│
   *     └─────────────────┘ └───────┬────────┘ └───────────────┘
   *                                 │
   *                                 ▼
   *                        ┌────────────────┐
   *                        │svc_alpha_reader│
   *                        └────────────────┘
   */
  it("returns edges for a multi-level role hierarchy", async () => {
    await testdrive(`
      $ postgres-execute connection=postgres://mz_system:materialize@materialized:6877
      DROP ROLE IF EXISTS svc_alpha_reader, svc_alpha_ingest, app_team_alpha_ro, app_team_beta_ro, app_team_alpha, app_team_beta, platform_admin, org_admin;

      > CREATE ROLE org_admin;
      > CREATE ROLE platform_admin;
      > CREATE ROLE app_team_alpha;
      > CREATE ROLE app_team_beta;
      > CREATE ROLE app_team_alpha_ro;
      > CREATE ROLE app_team_beta_ro;
      > CREATE ROLE svc_alpha_ingest;
      > CREATE ROLE svc_alpha_reader;

      $ postgres-execute connection=postgres://mz_system:materialize@materialized:6877
      GRANT org_admin TO platform_admin;
      GRANT platform_admin TO app_team_alpha;
      GRANT platform_admin TO app_team_beta;
      GRANT app_team_alpha TO app_team_alpha_ro;
      GRANT app_team_alpha TO svc_alpha_ingest;
      GRANT app_team_beta TO app_team_beta_ro;
      GRANT svc_alpha_ingest TO svc_alpha_reader;
    `);

    const query = buildRoleGraphEdgesQuery().compile();
    const result = await executeSqlHttp(query);

    const expectedEdges = [
      { parentName: "org_admin", childName: "platform_admin" },
      { parentName: "platform_admin", childName: "app_team_alpha" },
      { parentName: "platform_admin", childName: "app_team_beta" },
      { parentName: "app_team_alpha", childName: "app_team_alpha_ro" },
      { parentName: "app_team_alpha", childName: "svc_alpha_ingest" },
      { parentName: "app_team_beta", childName: "app_team_beta_ro" },
      { parentName: "svc_alpha_ingest", childName: "svc_alpha_reader" },
    ];

    for (const edge of expectedEdges) {
      expect(result.rows).toContainEqual(expect.objectContaining(edge));
    }

    const parentNames = result.rows.map((r) => r.parentName);
    const childNames = result.rows.map((r) => r.childName);
    expect(parentNames).not.toContain("mz_system");
    expect(childNames).not.toContain("mz_system");
  });
});
