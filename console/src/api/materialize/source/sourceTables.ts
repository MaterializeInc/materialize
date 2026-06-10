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

import { buildSourceDependenciesQuery } from "~/api/materialize/expressionBuilders";

import { queryBuilder } from "../db";
import { executeSqlV2 } from "../executeSqlV2";

export type SourceTableParams = {
  sourceId: string;
};

/**
 * Fetches tables of a single source
 */
export function buildSourceTablesQuery(sourceId: string) {
  return (
    queryBuilder
      .selectFrom(
        buildSourceDependenciesQuery(sourceId).as("source_dependencies"),
      )
      .innerJoin(
        "mz_object_fully_qualified_names as fqn",
        "fqn.id",
        "source_dependencies.id",
      )
      .innerJoin("mz_source_statuses as st", "st.id", "source_dependencies.id")
      // Get the type of the source table from the parent source
      .innerJoin("mz_sources as s", "s.id", "source_dependencies.sourceId")
      .select([
        "source_dependencies.id as id",
        "fqn.name as name",
        "fqn.database_name as databaseName",
        "fqn.schema_name as schemaName",
        "st.status as status",
        "s.type as type",
      ])
      .orderBy(["fqn.database_name", "fqn.schema_name", "fqn.name"])
  );
}

/**
 * Fetches tables for a given source.
 */
export async function fetchSourceTables({
  params,
  queryKey,
  requestOptions,
}: {
  params: SourceTableParams;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const compiledQuery = buildSourceTablesQuery(params.sourceId).compile();

  return executeSqlV2({
    queries: compiledQuery,
    queryKey: queryKey,
    requestOptions,
  });
}

export type SourceTable = InferResult<
  ReturnType<typeof buildSourceTablesQuery>
>[0];
