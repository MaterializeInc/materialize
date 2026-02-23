// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { InferResult, sql } from "kysely";

import { queryBuilder } from "~/api/materialize/db";

import { hasSuperUserPrivileges } from "./expressionBuilders";

/**
 * These are schemas that were intended to be used in a migration https://github.com/MaterializeInc/database-issues/issues/7110
 * but we never got around to merging the changes. Nevertheless, they shouldn't be exposed to the user.
 */
const HIDDEN_SYSTEM_SCHEMAS = ["mz_unsafe", "mz_catalog_unstable"];

export function buildAllSchemaListQuery() {
  return queryBuilder
    .selectFrom("mz_schemas as s")
    .leftJoin("mz_databases as d", "d.id", "s.database_id")
    .select(["s.id", "s.name", "d.id as databaseId", "d.name as databaseName"])
    .where("s.name", "not in", HIDDEN_SYSTEM_SCHEMAS)
    .orderBy("s.name");
}

/**
 * These are schemas that are queried for the Object Explorer especially databases that don't have a schema.
 * This decouples the query from fetching schemas for other parts of the UI.
 */
export function buildAllNamespacesQuery() {
  return queryBuilder
    .selectFrom("mz_schemas as s")
    .fullJoin("mz_databases as d", "d.id", "s.database_id")
    .select([
      "s.id as schemaId",
      "s.name as schemaName",
      "d.id as databaseId",
      "d.name as databaseName",
    ])
    .where((eb) =>
      eb.or([
        eb("s.name", "is", null),
        eb("s.name", "not in", HIDDEN_SYSTEM_SCHEMAS),
      ]),
    )
    .orderBy("d.name")
    .orderBy("s.name");
}

export type SchemaWithOptionalDatabase = InferResult<
  ReturnType<typeof buildAllSchemaListQuery>
>[0];

export function buildSchemaListQuery(
  options: {
    databaseId?: string;
    filterByCreatePrivilege?: boolean;
  } = {},
) {
  let qb = queryBuilder
    .selectFrom("mz_schemas as s")
    .innerJoin("mz_databases as d", "d.id", "s.database_id")
    .select(["s.id", "s.name", "d.id as databaseId", "d.name as databaseName"]);
  if (options.filterByCreatePrivilege) {
    qb = qb.where((eb) =>
      eb.or([
        sql<boolean>`(${hasSuperUserPrivileges()})`,
        eb.fn<boolean>("has_schema_privilege", [
          sql.raw("current_user"),
          eb.ref("s.oid"),
          sql.lit("CREATE"),
        ]),
      ]),
    );
  }
  qb = qb.orderBy("s.name");

  if (options.databaseId) {
    qb = qb.where("d.id", "=", options.databaseId);
  }

  return qb;
}

export type Schema = InferResult<ReturnType<typeof buildSchemaListQuery>>[0];

export type AllNamespaceItem = InferResult<
  ReturnType<typeof buildAllNamespacesQuery>
>[0];
