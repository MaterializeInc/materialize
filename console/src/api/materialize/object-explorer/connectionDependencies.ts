// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { QueryKey } from "@tanstack/react-query";
import { InferResult } from "kysely";

import { escapedLiteral as lit } from "~/api/materialize";

import { queryBuilder } from "../db";
import { executeSqlV2 } from "../executeSqlV2";

export type ConnectionDependenciesParameters = {
  connectionId: string;
};

/**
 * Builds a query that fetches the sources and sinks of a connection.
 */
export function buildConnectionDependenciesQuery({
  connectionId,
}: ConnectionDependenciesParameters) {
  return queryBuilder
    .selectFrom("mz_object_fully_qualified_names as ofqn")
    .innerJoin(
      (innerJoinEb) =>
        innerJoinEb
          .selectFrom("mz_sources as sources")
          .where("sources.connection_id", "=", connectionId)
          .select([
            "sources.id as id",
            "sources.name as name",
            lit("source").as("type"),
            "sources.type as subType",
          ])
          .union((unionEb) =>
            unionEb
              .selectFrom("mz_sinks as sinks")
              .where("sinks.connection_id", "=", connectionId)
              .select([
                "sinks.id as id",
                "sinks.name as name",
                lit("sink").as("type"),
                "sinks.type as subType",
              ]),
          )
          .as("connectionDependencies"),
      (join) => join.onRef("connectionDependencies.id", "=", "ofqn.id"),
    )
    .select([
      "connectionDependencies.id",
      "connectionDependencies.name",
      "connectionDependencies.type",
      "connectionDependencies.subType",
      "ofqn.database_name as databaseName",
      "ofqn.schema_name as schemaName",
    ]);
}

export function fetchConnectionDependencies({
  parameters,
  queryKey,
  requestOptions,
}: {
  parameters: ConnectionDependenciesParameters;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const compiledQuery = buildConnectionDependenciesQuery(parameters).compile();
  return executeSqlV2({
    queries: compiledQuery,
    queryKey: queryKey,
    requestOptions,
  });
}

export type ConnectionDependency = InferResult<
  ReturnType<typeof buildConnectionDependenciesQuery>
>[0];
