// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import * as Sentry from "@sentry/react";
import { hashKey, QueryKey } from "@tanstack/react-query";
import { CompiledQuery, InferResult } from "kysely";

import { getStore } from "~/jotai";
import { currentEnvironmentState } from "~/store/environments";
import { assert } from "~/util";
import { anySignal } from "~/utils/signal";

import { apiClient } from "../apiClient";
import {
  DEFAULT_QUERY_ERROR,
  mapColumnToColumnMetadata,
  mapRowToObject,
} from ".";
import {
  DatabaseError,
  isPermissonError,
  PermissionError,
} from "./DatabaseError";
import {
  buildExecuteSqlUrl,
  buildSessionVariables,
  UNAUTHORIZED_ERROR,
} from "./executeSql";
import {
  ColumnMetadata,
  ExtendedRequestItem,
  OkSqlResult,
  SessionVariables,
  SqlResult,
  TabularSqlResult,
} from "./types";

type CompiledQueryList = Readonly<CompiledQuery[]>;

export const DEFAULT_REQUEST_TIMEOUT = 30_000;

function isCompiledQuery(query: unknown): query is CompiledQuery {
  return (
    typeof query === "object" &&
    query !== null &&
    "query" in query &&
    "sql" in query &&
    "parameters" in query
  );
}

function isCompiledQueryList(queries: unknown): queries is CompiledQueryList {
  return Array.isArray(queries) && queries.every(isCompiledQuery);
}

function buildExtendedRequestQuery(query: CompiledQuery) {
  const params = query.parameters.map((param) => {
    return typeof param !== "string" ? JSON.stringify(param) : param;
  });

  return {
    query: query.sql,
    params,
  };
}

function buildExtendedSqlRequest<Q extends object>(queries: Q) {
  let extendedSqlRequestQueries: ExtendedRequestItem[] = [];

  if (isCompiledQueryList(queries)) {
    extendedSqlRequestQueries = queries.map(buildExtendedRequestQuery);
  } else if (isCompiledQuery(queries)) {
    extendedSqlRequestQueries = [buildExtendedRequestQuery(queries)];
  }

  return {
    queries: extendedSqlRequestQueries,
  };
}

/**
 * Maps 2D array data we get from Materialize's HTTP API to
 * a 1D array of objects keyed by the result's columns
 */
function mapTabularSqlResultToJson(tabularSqlResult: TabularSqlResult) {
  if (!tabularSqlResult.desc) {
    Sentry.captureException(new Error("SQL results missing column metadata"), {
      extra: {
        tabularSqlResult,
      },
    });
  }
  const columnMetaData = tabularSqlResult.desc.columns.map(
    mapColumnToColumnMetadata,
  );

  return {
    columns: columnMetaData,
    rows: tabularSqlResult.rows.map((row) =>
      mapRowToObject(row, columnMetaData),
    ),
  };
}

type InferCompiledQuery<Q> = Q extends CompiledQuery<infer O> ? O : never;

type TypedSqlResult<Q extends CompiledQuery> =
  InferCompiledQuery<Q> extends OkSqlResult
    ? OkSqlResult
    : {
        tag: TabularSqlResult["tag"];
        notices: TabularSqlResult["notices"];
        columns: ColumnMetadata[];
        rows: InferResult<Q>;
      };

type TypedSqlResultList<Q extends CompiledQueryList> = {
  [K in keyof Q]: TypedSqlResult<Q[K]>;
};

type ExecuteSqlReturnType<Q extends CompiledQueryList | CompiledQuery> =
  Q extends CompiledQueryList
    ? TypedSqlResultList<Q>
    : Q extends CompiledQuery
      ? TypedSqlResult<Q>
      : never;

function mapToTypedSqlResults<Q extends CompiledQueryList | CompiledQuery>(
  queries: Q,
  results: SqlResult[],
): ExecuteSqlReturnType<Q> {
  const mapToTypedSqlResult = (result: SqlResult) => {
    if ("error" in result) {
      /**
       * Note: We say the fetch call has failed if any queries in the request failed.
       * This isn't necessarily true since if the nth query was a CREATE and succeeded but the (n+1)th query failed,
       * the CREATE is still committed to the database.
       *
       * For the sake of keeping this API simple, we can assume all queries are part of a single transaction and throw on the first error.
       * If necessary in the future however, we can add an option to return each error object rather than throw.
       *
       */
      if (isPermissonError(result)) {
        throw new PermissionError(result);
      }
      throw new DatabaseError(result);
    }

    if ("ok" in result) {
      return result;
    }

    const { rows, columns } = mapTabularSqlResultToJson(result);

    return {
      tag: result.tag,
      notices: result.notices,
      columns,
      rows,
    };
  };

  const mappedResults: Readonly<Array<ReturnType<typeof mapToTypedSqlResult>>> =
    results.map(mapToTypedSqlResult);

  if (isCompiledQueryList(queries)) {
    return mappedResults as ExecuteSqlReturnType<typeof queries>;
  }

  if (isCompiledQuery(queries)) {
    return mappedResults[0] as ExecuteSqlReturnType<typeof queries>;
  }

  throw new TypeError(
    "'queries' must be of type CompiledQuery or a read-only array of CompiledQuery",
  );
}

export interface ExecuteSqlRequest<
  Q extends CompiledQuery | CompiledQueryList,
> {
  queries: Q;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
  sessionVariables?: SessionVariables;
  requestTimeoutMs?: number;
  httpAddress?: string;
}

async function getHttpAddress<Q extends CompiledQuery | CompiledQueryList>(
  request: ExecuteSqlRequest<Q>,
) {
  if (request.httpAddress) {
    return request.httpAddress;
  }
  const environment = await getStore().get(currentEnvironmentState);
  assert(environment && environment.state === "enabled");
  return environment.httpAddress;
}

/**
 *
 * A fetch call that executes SQL queries against Materialize's HTTP API. We throw if the database
 * returns an error for any queries.
 *
 * @param queries - A single instance or an array of 'CompiledQuery's used to issue an SQL request.
 * Note: For an array of `CompiledQuery's, make sure it's a const array otherwise the queries of the array won't be recognized.
 * @param queryKey - The key of a query. This is important for identifying queries in the network log and mock identifiers for unit tests.
 * @param requestOptions - Fetch API Options
 * @param sessionVariables - Materialize session variables that will be set before running the request.
 *
 * @return - A single instance or an array of each query's response data. The response data is typed, the rows are keyed by their column names, and
 * the row values are cast to their appropriate data types.
 *
 * This is used for the React Query migration since its fetches can't rely on React's lifecycle.
 * Deprecate the old executeSql methods when the migration is finished.
 * https://github.com/MaterializeInc/console/issues/1176
 */
export async function executeSqlV2<
  Q extends CompiledQuery | CompiledQueryList,
>(request: {
  queries: Q;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
  sessionVariables?: SessionVariables;
  requestTimeoutMs?: number;
  /** Overrides the currently configured environment, only used in test */
  httpAddress?: string;
}): Promise<ExecuteSqlReturnType<Q>> {
  const sqlRequest = buildExtendedSqlRequest(request.queries);
  const httpAddress = await getHttpAddress(request);
  const url = buildExecuteSqlUrl(httpAddress);

  url.searchParams.append(
    "options",
    JSON.stringify(buildSessionVariables(request.sessionVariables)),
  );

  // TODO: Put query_key in a custom header rather than a search param
  url.searchParams.append("query_key", hashKey(request.queryKey));

  const {
    headers: headersOverride,
    signal,
    ...restRequestOptions
  } = request.requestOptions ?? {};

  const signals = [
    AbortSignal.timeout(request.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT),
  ];
  if (signal) {
    signals.push(signal);
  }

  const response = await apiClient.mzApiFetch(url.toString(), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...headersOverride,
    },
    body: JSON.stringify(sqlRequest),
    signal: anySignal(signals),
    ...restRequestOptions,
  });

  const responseText = await response.text();

  const isFetchFailure = !response.ok;

  if (isFetchFailure) {
    if (response.status === 401) {
      // Should never happen
      throw new Error(UNAUTHORIZED_ERROR, {
        cause: { status: response.status },
      });
    }

    throw new Error(responseText || DEFAULT_QUERY_ERROR, {
      cause: { status: response.status },
    });
  }

  const parsedResponse = JSON.parse(responseText);

  const { results }: { results: SqlResult[] } = parsedResponse;

  const typedSqlResults = mapToTypedSqlResults(request.queries, results);

  return typedSqlResults;
}
