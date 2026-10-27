// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { apiClient } from "../apiClient";
import {
  APPLICATION_NAME,
  DEFAULT_QUERY_ERROR,
  mapColumnToColumnMetadata,
} from ".";
import MaterializeErrorCode from "./errorCodes";
import {
  Column,
  ColumnMetadata,
  ErrorCode,
  ExtendedRequestItem,
  SessionVariables,
} from "./types";

export interface SqlRequest {
  queries: ExtendedRequestItem[];
  cluster: string;
  replica?: string;
  /**
   * The level of consistency we want per query. You can read more here: https://materialize.com/docs/get-started/isolation-level/
   */
  transactionIsolation?: "strict serializable" | "serializable";
}

export interface Results {
  columns: Array<ColumnMetadata>;
  rows: Array<any>;
  getColumnByName?: <R, V>(row: R[], name: string) => V;
}

interface ExecuteSqlSuccess {
  results: Results[];
}

type GenericError = { errorMessage: string };

type NetworkError = GenericError & {
  status: number;
};

type MaterializeError = GenericError & {
  code: ErrorCode | MaterializeErrorCode;
  detail?: string;
  hint?: string;
};

export type ExecuteSqlError = MaterializeError | NetworkError | GenericError;

type ExecuteSqlOutput = ExecuteSqlSuccess | ExecuteSqlError;

export function isExecuteSqlError(error: unknown): error is ExecuteSqlError {
  return error != null && typeof error === "object" && "errorMessage" in error;
}

export function buildExecuteSqlUrl(environmentdHttpAddress: string) {
  return new URL(
    `${apiClient.mzHttpUrlScheme}://${environmentdHttpAddress}/api/sql`,
  );
}

export const SEARCH_PATH =
  "mz_catalog, mz_internal, mz_catalog_unstable, mz_introspection";

export function buildSessionVariables(variables?: SessionVariables) {
  return {
    application_name: APPLICATION_NAME,
    cluster: CATALOG_SERVER_CLUSTER,
    /**
     * The default transaction isolation level must be set to "strict serializable" to ensure that
     * when multiple queries are ran at once, the data is consistent. This is especially important
     * for our read your write operations.
     */
    transaction_isolation: "strict serializable",
    // The search path must correspond with the schemas set in bin/generate-types
    search_path: SEARCH_PATH,
    ...variables,
  };
}

/* mz_catalog_server is a cluster dedicated for catalog/Console queries. */
export const CATALOG_SERVER_CLUSTER = "mz_catalog_server";

function isJsonObject(val: string): boolean {
  try {
    const parsed = JSON.parse(val);
    return parsed instanceof Object;
  } catch (err) {
    return false;
  }
}

const doExecuteSql = async (
  environmentdHttpAddress: string,
  request: SqlRequest,
  requestOptions?: RequestInit,
): Promise<ExecuteSqlOutput> => {
  const url = buildExecuteSqlUrl(environmentdHttpAddress);
  // Optional session vars that will be set before running the request.
  //
  // Note: the JSON object is automatically URI encoded by the URL object.
  const options = buildSessionVariables({
    cluster: request.cluster,
    cluster_replica: request.replica,
    transaction_isolation:
      request.transactionIsolation ?? "strict serializable",
  });
  url.searchParams.append("options", JSON.stringify(options));

  const { headers, ...requestOpts } = requestOptions ?? {};
  const response = await apiClient.mzApiFetch(
    new Request(url.toString(), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...headers,
      },
      body: JSON.stringify({
        queries: request.queries,
      }),
      ...requestOpts,
    }),
  );

  const responseText = await response.text();

  if (!response.ok) {
    if (response.status === 401) {
      throw new Error(UNAUTHORIZED_ERROR);
    }
    if (response.status === 403 && isJsonObject(responseText)) {
      const responseError = JSON.parse(responseText);
      return {
        errorMessage: responseError.message,
        code: responseError.code,
        detail: responseError.detail,
        status: response.status,
      };
    }
    return {
      status: response.status,
      errorMessage: responseText || DEFAULT_QUERY_ERROR,
    };
  } else {
    const parsedResponse = JSON.parse(responseText);
    const { results } = parsedResponse;
    const outResults = [];
    for (const oneResult of results) {
      // Queries like `CREATE TABLE` or `CREATE CLUSTER` returns a null inside the results array
      const { error: resultsError, rows } = oneResult || {};

      const columns: Column[] | undefined =
        oneResult.desc?.columns ?? undefined;

      let getColumnByName = undefined;
      if (columns) {
        const columnMap: Map<string, number> = new Map(
          (columns as Column[]).map(({ name }, index) => [name, index]),
        );

        getColumnByName = (row: any[], name: string) => {
          const index = columnMap.get(name);
          if (index === undefined) {
            throw new Error(`Column named ${name} not found`);
          }

          return row[index];
        };
      }
      if (resultsError) {
        return {
          errorMessage: resultsError.message,
          code: resultsError.code,
          detail: resultsError.detail,
          hint: resultsError.hint,
        };
      } else {
        outResults.push({
          rows: rows,
          columns: columns?.map(mapColumnToColumnMetadata) ?? [],
          getColumnByName,
        });
      }
    }
    return { results: outResults };
  }
};

/**
 * Runs sql statements against the currently selected environment
 */
const executeSql = async (
  environmentdHttpAddress: string,
  request: SqlRequest,
  requestOpts?: RequestInit,
): Promise<ExecuteSqlOutput> => {
  return doExecuteSql(environmentdHttpAddress, request, requestOpts);
};

export default executeSql;

export const UNAUTHORIZED_ERROR = "Unauthorized";
