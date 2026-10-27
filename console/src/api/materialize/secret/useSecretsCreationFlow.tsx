// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { sql } from "kysely";
import React from "react";

import { queryBuilder, useSqlTyped } from "~/api/materialize";
import { hasSuperUserPrivileges } from "~/api/materialize/expressionBuilders";

/**
 * Fetches all secrets for for selects in creation flow
 */
export function useSecretsCreationFlow() {
  const query = React.useMemo(() => {
    return queryBuilder
      .selectFrom("mz_secrets as s")
      .innerJoin("mz_schemas as sc", "sc.id", "s.schema_id")
      .innerJoin("mz_databases as d", "d.id", "sc.database_id")
      .select([
        "s.id",
        "s.name",
        "d.name as databaseName",
        "sc.name as schemaName",
      ])
      .where("s.id", "like", "u%")
      .where((eb) =>
        eb.or([
          sql<boolean>`(${hasSuperUserPrivileges()})`,
          eb.fn<boolean>("has_secret_privilege", [
            sql.raw("current_user"),
            eb.ref("s.oid"),
            sql.lit("USAGE"),
          ]),
        ]),
      )
      .compile();
  }, []);

  return useSqlTyped(query);
}

export const useSecretsCreationFlowColumns = [
  "id",
  "name",
  "databaseName",
  "schemaName",
];

export type SecretsCreationFlowResponse = ReturnType<
  typeof useSecretsCreationFlow
>;

export type Secret = NonNullable<SecretsCreationFlowResponse["results"]>[0];
