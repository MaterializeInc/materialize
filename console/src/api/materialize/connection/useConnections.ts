// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { QueryKey } from "@tanstack/react-query";
import { sql } from "kysely";
import React from "react";

import {
  executeSqlV2,
  queryBuilder,
  SchemaObject,
  useSqlTyped,
} from "~/api/materialize";
import { escapedLiteral } from "~/api/materialize";

import { getOwners, hasSuperUserPrivileges } from "../expressionBuilders";

export type ConnectionType =
  | "mysql"
  | "postgres"
  | "sql-server"
  | "kafka"
  | "confluent-schema-registry"
  | "private-link"
  | "ssh";

export const dataConnectionTypes: ConnectionType[] = [
  "mysql",
  "postgres",
  "sql-server",
  "kafka",
];
export const networkSecurityConnectionTypes: ConnectionType[] = [
  "private-link",
  "ssh",
];

export interface Connection extends SchemaObject {
  type: string;
}

export type ConnectionsQueryFilter = {
  databaseId?: string;
  schemaId?: string;
  nameFilter?: string;
};

function buildConnectionsQuery({
  databaseId,
  schemaId,
  nameFilter,
}: ConnectionsQueryFilter = {}) {
  let qb = queryBuilder
    .selectFrom("mz_connections as c")
    .innerJoin("mz_schemas as sc", "sc.id", "c.schema_id")
    .innerJoin("mz_databases as d", "d.id", "sc.database_id")
    .innerJoin(getOwners().as("owners"), "owners.id", "c.owner_id")
    .select((eb) => [
      "c.id",
      "c.name",
      "sc.name as schemaName",
      "d.name as databaseName",
      "c.type",
      eb
        .selectFrom("mz_sources")
        .where("connection_id", "=", eb.ref("c.id"))
        .select((builder) => [builder.fn.countAll().as("count")])
        .as("numSources"),
      eb
        .selectFrom("mz_sinks")
        .where("connection_id", "=", eb.ref("c.id"))
        .select((builder) => [builder.fn.countAll().as("count")])
        .as("numSinks"),
      "owners.isOwner",
    ])
    .where("c.id", "like", "u%")
    .orderBy(["d.name", "sc.name", "name"]);
  if (databaseId) {
    qb = qb.where("d.id", "=", databaseId);
  }
  if (schemaId) {
    qb = qb.where("sc.id", "=", schemaId);
  }
  if (nameFilter) {
    qb = qb.where("c.name", "like", `%${nameFilter}%`);
  }
  return qb;
}

export function fetchConnections({
  filters,
  queryKey,
  requestOptions,
}: {
  filters: ConnectionsQueryFilter;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const compiledQuery = buildConnectionsQuery(filters).compile();
  return executeSqlV2({
    queries: compiledQuery,
    queryKey,
    requestOptions,
  });
}

/**
 * Fetches all connections, filter by any of the following:
 * * type
 * * name substring
 */
export function useConnectionsFiltered({
  nameFilter,
  type,
}: { nameFilter?: string; type?: ConnectionType | ConnectionType[] } = {}) {
  const query = React.useMemo(() => {
    let qb = queryBuilder
      .selectFrom("mz_connections as c")
      .innerJoin("mz_schemas as sc", "sc.id", "c.schema_id")
      .innerJoin("mz_databases as d", "d.id", "sc.database_id")
      .select([
        "c.id",
        "c.name",
        "d.name as databaseName",
        "sc.name as schemaName",
        "c.type",
      ])
      .where("c.id", "like", "u%")
      .where((eb) =>
        eb.or([
          sql<boolean>`(${hasSuperUserPrivileges()})`,
          eb.fn<boolean>("has_connection_privilege", [
            sql.raw("current_user"),
            eb.ref("c.oid"),
            sql.lit("USAGE"),
          ]),
        ]),
      )
      .orderBy(["d.name", "sc.name", "name"]);
    if (nameFilter) {
      qb = qb.where("c.name", "ilike", escapedLiteral(`%${nameFilter}%`));
    }
    if (type) {
      if (type instanceof Array) {
        qb = qb.where("c.type", "in", type);
      } else {
        qb = qb.where("c.type", "=", escapedLiteral(type));
      }
    }
    return qb.compile();
  }, [nameFilter, type]);

  return useSqlTyped(query);
}

export const connectionsFilteredColumns = [
  "id",
  "name",
  "databaseName",
  "schemaName",
  "type",
];

export type ConnectionsFilteredResponse = ReturnType<
  typeof useConnectionsFiltered
>;

export type ConnectionFiltered = NonNullable<
  ConnectionsFilteredResponse["results"]
>[0];
