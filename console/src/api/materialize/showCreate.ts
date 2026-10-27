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

import { kebabToScreamingSpaceCase } from "~/util";

import { buildFullyQualifiedObjectName, executeSqlV2, queryBuilder } from ".";

export const SHOW_CREATE_OBJECT_TYPES = [
  "index",
  "connection",
  "view",
  "materialized-view",
  "source",
  "sink",
  "table",
] as const;

export type ShowCreateObjectType = (typeof SHOW_CREATE_OBJECT_TYPES)[number];

export type ShowCreateStatementParameters = {
  objectType: ShowCreateObjectType;
  isSourceTable?: boolean;
  object: {
    databaseName: string | null;
    schemaName: string;
    name: string;
  };
};

export function buildShowCreateStatement(
  parameters: ShowCreateStatementParameters,
) {
  // TODO: (#3400) Remove once all subsources are tables
  const type = parameters.isSourceTable
    ? sql.raw("source")
    : sql.raw(kebabToScreamingSpaceCase(parameters.objectType));
  return sql<{
    sql: string;
  }>`SELECT create_sql AS sql FROM (SHOW CREATE ${type} ${buildFullyQualifiedObjectName(
    parameters.object,
  )})`;
}

export function fetchShowCreate(
  queryKey: QueryKey,
  parameters: ShowCreateStatementParameters,
  requestOptions?: RequestInit,
) {
  return executeSqlV2({
    queries: buildShowCreateStatement(parameters).compile(queryBuilder),
    queryKey,
    requestOptions,
  });
}
