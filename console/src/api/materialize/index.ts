// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { SelectQueryBuilder, sql } from "kysely";
import parsePostgresInterval from "postgres-interval";

import { assert, isTruthy, notNullOrUndefined } from "~/util";

import { Results } from "./executeSql";
import { Column, ColumnMetadata, DatabaseObject, MzDataType } from "./types";

export * from "./db";
export * from "./executeSql";
export { default as executeSql } from "./executeSql";
export { executeSqlV2 } from "./executeSqlV2";
export * from "./useSqlApiRequest";
export * from "./useSqlLazy";
export * from "./useSqlMany";
export * from "./useSqlManyTyped";
export * from "./useSqlTyped";
export type { IPostgresInterval } from "postgres-interval";

export type OnSettled = (data?: Results[] | null, error?: string) => void;
export type OnSuccess = (data?: Results[] | null) => void;
export type OnError = (error?: string) => void;

export interface TimestampedCounts {
  count: number;
  timestamp: number;
}

export interface SchemaObject {
  id: string;
  name: string;
  schemaName: string;
  databaseName: string | null;
}

export interface GroupedError {
  error: string;
  lastOccurred: Date;
  count: number;
}

/**
 * Name used to identify ourselves to the server, needs to be kept in sync with
 * the `ApplicationNameHint`.
 */
export const APPLICATION_NAME = "web_console";

/**
 * Clusters that are created for new environments.
 * Historically it was "default", soon it will be "quickstart".
 */
export const DEFAULT_CLUSTERS = ["default", "quickstart"];

/* mz_catalog_server is a cluster dedicated for catalog/Console queries. */
export const CATALOG_SERVER_CLUSTER = "mz_catalog_server";

/**
 * mz_probe is a cluster dedicated for internal uptime monitoring.
 */
export const MZ_PROBE_CLUSTER = "mz_probe";

/**
 * Name used to identify the Shell feature specifically to the server.
 */
export const SHELL_APPLICATION_NAME = "web_console_shell";
export const DEFAULT_QUERY_ERROR = "Error running query.";
export const TIMEOUT_ABORT_MESSAGE = "Timeout: query aborted";
export const NEW_QUERY_ABORT_MESSAGE = "Previous query aborted";

/**
 * Quotes a string to be used as a SQL identifier.
 * It is an error to call this function with a string that contains the zero code point.
 */
export function quoteIdentifier(identifier: string) {
  // According to https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS,
  // any string may be used as an identifier, except those that contain the zero code point.
  //
  // In order to support special characters, quoted identifiers must be used.
  // Within a quoted identifier, the literal double-quote character must be escaped
  // by writing it twice.
  // For example, the identifier foo" is represented as "foo""" (including the quotes).

  // Materialize never allows any identifiers to be used whose name contains the null byte,
  // so this assert should never fire unless this function is called with arbitrary user input.
  assert(identifier.search("\0") === -1);

  return `"${identifier.replace(/"/g, '""')}"`;
}

/**
 * Given an array of sql expressions, builds a string of WHERE + AND clauses.
 * Also accepts and filters out undefined values for convenience.
 */
export function buildWhereConditions(expressions: Array<undefined | string>) {
  expressions = expressions.filter(isTruthy);
  return expressions.length > 0 ? `\nWHERE ${expressions.join("\nAND ")}` : "";
}

/**
 * Adds a limit clause to the end of a query.
 * Materialize does not support parameters in the limit clause currently, so we have to
 * hard code the limit value as a work around.
 * https://github.com/MaterializeInc/materialize/issues/19867
 */
export function rawLimit<DB, TB extends keyof DB, O>(
  query: SelectQueryBuilder<DB, TB, O>,
  value: number,
) {
  return query.modifyEnd(sql`limit ${sql.raw(value.toString())}`);
}

export function createNamespace(
  databaseName?: string | null,
  schemaName?: string | null,
  options?: {
    quoteIdentifiers?: boolean;
  },
) {
  let namespace = [databaseName, schemaName].filter(notNullOrUndefined);

  if (options?.quoteIdentifiers) {
    namespace = namespace.map(quoteIdentifier);
  }

  return namespace.join(".");
}

function formatFullyQualifiedSchemaIdentifier(
  {
    name,
    schemaName,
    databaseName,
  }: {
    name: string;
    schemaName: string;
    databaseName?: string | null;
  },
  options?: {
    quoteIdentifiers?: boolean;
  },
) {
  let res = [databaseName, schemaName, name].filter(notNullOrUndefined);

  if (options?.quoteIdentifiers) {
    res = res.map(quoteIdentifier);
  }

  return res.join(".");
}

export function formatFullyQualifiedObjectName(
  dbObject:
    | { name: string }
    | { clusterName: string; name: string }
    | { databaseName?: string | null; schemaName: string; name: string },
  options?: {
    quoteIdentifiers?: boolean;
  },
) {
  if ("schemaName" in dbObject) {
    return formatFullyQualifiedSchemaIdentifier(dbObject, options);
  }
  if ("clusterName" in dbObject) {
    let res = [dbObject.clusterName, dbObject.name];
    if (options?.quoteIdentifiers) {
      res = res.map(quoteIdentifier);
    }
    return res.join(".");
  }
  return options?.quoteIdentifiers
    ? quoteIdentifier(dbObject.name)
    : dbObject.name;
}

/**
 * Escapes a string to be used as a SQL literal and returns a Kysely RawBuilder.
 */
export function escapedLiteral(literal: string) {
  return sql.lit(literal.replace(/'/g, "''"));
}

/**
 * Given a cluster id like 'u1' or 's1', returns true for system clusters and false for user clusters.
 *
 * Examples of system clusters include `mz_system` and `mz_catalog_server`
 */
export function isSystemCluster(clusterId: string) {
  return clusterId.startsWith("s");
}

/** System object IDs start with an "s". **/
export function isSystemId(objectId: string) {
  return objectId.startsWith("s");
}

/**
 * Builds a fully qualified object name for database objects that exist in a schema
 */
export function buildFullyQualifiedSchemaIdentifier({
  databaseName,
  schemaName,
  name,
}: {
  databaseName?: string | null;
  schemaName: string;
  name: string;
}) {
  if (databaseName) {
    return sql`${sql.id(databaseName)}.${sql.id(schemaName)}.${sql.id(name)}`;
  }
  return sql`${sql.id(schemaName)}.${sql.id(name)}`;
}

/** Parses schema names from the raw search_path value returned from `SHOW search_path`. */
export function parseSearchPath(searchPath: string | undefined) {
  if (!searchPath) return [];
  // If a search path contains special characters it will be quoted
  let normalized = searchPath.startsWith('"')
    ? searchPath.slice(1, searchPath.length - 1)
    : searchPath;
  normalized = normalized.replace('""', '"');
  return normalized.split(",").map((str) => str.trim());
}

/**
 * Builds a fully qualified object name for clusters, replicas or objects that exist in a schema
 */
export function buildFullyQualifiedObjectName(
  dbObject:
    | DatabaseObject
    | { databaseName?: string | null; schemaName: string; name: string },
) {
  if ("schemaName" in dbObject) {
    return buildFullyQualifiedSchemaIdentifier(dbObject);
  }
  if ("clusterName" in dbObject) {
    return sql`${sql.id(dbObject.clusterName)}.${sql.id(dbObject.name)}`;
  }
  return sql.id(dbObject.name);
}

/**
 * Builds a SqlRequest that can be passed to executeSql
 */
export function buildSqlRequest(
  sqlQuery?: string,
  cluster = CATALOG_SERVER_CLUSTER,
  replica?: string,
) {
  return sqlQuery
    ? // Run all queries on the `mz_catalog_server` cluster, as it's
      // guaranteed to exist. (The `default` cluster may have been dropped
      // by the user.)
      {
        queries: [{ query: sqlQuery, params: [] }],
        cluster,
        replica,
      }
    : undefined;
}

export function extractData<DataType>(
  data: Results,
  f: (extractor: (colName: string) => any) => DataType,
): DataType[] {
  const { rows, getColumnByName } = data;
  assert(getColumnByName);
  return rows.map((row) => f((colName) => getColumnByName(row, colName)));
}

export function convertResultValue(value: unknown, column: ColumnMetadata) {
  if (value === null) return null;

  switch (column.typeOid) {
    case MzDataType.uint2:
    case MzDataType.uint4:
    case MzDataType.int2:
    case MzDataType.int4:
    case MzDataType.oid:
      return parseInt(value as string);
    case MzDataType.uint8:
    case MzDataType.int8:
      return BigInt(value as string);
    case MzDataType.numeric:
    case MzDataType.float4:
    case MzDataType.float8:
      return parseFloat(value as string);
    case MzDataType.timestamptz:
    case MzDataType.timestamp:
      return new Date(parseInt(value as string));
    case MzDataType.interval:
      return parsePostgresInterval(value as string);
    default:
      return value;
  }
}

export function mapRowToObject<T = unknown>(
  row: unknown[],
  columns: ColumnMetadata[],
  ignoreColumns: string[] = [],
) {
  const result: Record<string, unknown> = {};
  for (let i = 0; i < columns.length; i++) {
    const col = columns[i];
    if (!ignoreColumns.includes(col.name)) {
      result[col.name] = convertResultValue(row[i], col);
    }
  }
  return result as T;
}

export function mapColumnToColumnMetadata(column: Column): ColumnMetadata {
  return {
    name: column.name,
    typeOid: column.type_oid,
    typeLength: column.type_len,
  };
}
