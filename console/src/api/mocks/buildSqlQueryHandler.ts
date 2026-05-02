// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { hashKey, QueryKey } from "@tanstack/react-query";
import debug from "debug";
import { delay, DelayMode, http, HttpResponse } from "msw";

import {
  Column,
  Error,
  ExtendedRequest,
  getMzDataTypeOid,
  MzDataType,
  Notice,
  SqlResult,
  TabularSqlResult,
} from "~/api/materialize/types";
import { assert } from "~/util";

type ISQLQuery = {
  ok?: string;
  error?: Error;
};

type SQLSelectQuery = ISQLQuery & {
  type: "SELECT";
  columns: Array<string>;
  rows: Array<Array<unknown>>;
  queryKey?: string;
};

type SQLSelectQueryWithColumnMetadata = ISQLQuery & {
  type: "SELECT";
  columnMetadata: Array<Column>;
  rows: Array<Array<unknown>>;
  queryKey?: string;
};

type SQLCreateQuery = ISQLQuery & {
  type: "CREATE";
};

type SQLCommitQuery = ISQLQuery & {
  type: "COMMIT";
};

type SQLDropQuery = ISQLQuery & {
  type: "DROP";
};

type SQLInsertQuery = ISQLQuery & {
  type: "INSERT";
};

type SQLSetQuery = ISQLQuery & {
  type: "SET";
};

type SQLShowQuery = ISQLQuery & {
  type: "SHOW";
  column: string;
  rows: Array<Array<unknown>>;
};

type SQLShowCreateQuery = ISQLQuery & {
  type: "SHOW CREATE";
  name: string;
  createSql: string;
};

type SQLQuery =
  | SQLSelectQuery
  | SQLSelectQueryWithColumnMetadata
  | SQLCreateQuery
  | SQLCommitQuery
  | SQLDropQuery
  | SQLInsertQuery
  | SQLSetQuery
  | SQLShowQuery
  | SQLShowCreateQuery;

export const DEFAULT_TYPE: Omit<Column, "name"> = {
  type_oid: MzDataType.text,
  type_len: -1,
  type_mod: -1,
};

const debugSqlHandlers = debug("console:msw:sql");

// for some reason error logs seem to get swallowed by vitest
debugSqlHandlers.log = console.log.bind(console);

function buildNotSurroundedByParensRegex(searchTerm: string) {
  /*
(?<!         - Negative lookbehind assertion: Assert that the previous characters do NOT match the following pattern:
  \([^)]*    - Match an opening parenthesis followed by zero or more non-closing parenthesis characters
)            - End of negative lookbehind assertion
searchTerm   - Match the searchTerm
(?!          - Negative lookahead assertion: Assert that the following characters do NOT match the following pattern:
  [^(]*      - Match zero or more non-opening parenthesis characters
  \)         - Match a closing parenthesis
)            - End of negative lookahead assertion
  */
  return new RegExp(`(?<!\\([^)]*)${searchTerm}(?![^(]*\\))`, "s");
}

/**
 * Simple implementation to extract SQL SELECT query column names.
 * There are four cases when it will extract:
 * 1) When a column name is prefixed with "AS", take the alias
 * 2) When a column name has a namespace (i.e. mz_introspection.mz_compute_exports gives "mz_compute_exports")
 * 3) Standalone column names (i.e. "SELECT id from ..." gives "id")
 * 4) When a column name is a function call (i.e. SUM(...) gives "sum")
 * Does not account for DISTINCT, ALL, and nested CTEs
 * More documentation of SELECT's syntax tree can be found here: https://materialize.com/docs/sql/select/
 */
export function extractSQLSelectColumnNames(selectQueryStr: string) {
  const selectEndPosition =
    selectQueryStr
      .toUpperCase()
      .search(buildNotSurroundedByParensRegex("SELECT")) + "SELECT".length;
  const fromStartPosition = selectQueryStr
    .toUpperCase()
    .search(buildNotSurroundedByParensRegex("FROM\\s"));

  // For commas in functions (i.e. SELECT coalesce(records, 0) ...)
  const commaNotSurroundedByParensRegex = buildNotSurroundedByParensRegex(",");

  // Separate potential columns via commas
  const targetElemTokens = selectQueryStr
    .substring(selectEndPosition, fromStartPosition)
    .split(commaNotSurroundedByParensRegex)
    .map((el) => el.trim());

  const colNames = targetElemTokens.map((targetElemToken) => {
    const words = targetElemToken.split(" ").map((el) => el.trim());

    // Check for aliases
    const checkIfAs = (val: string) => val.toUpperCase() === "AS";
    const asPosition = words.findIndex(checkIfAs);
    if (asPosition !== -1) {
      const asCount = words.filter(checkIfAs).length;

      if (asCount > 1) {
        // The column's name is 'as' and has an alias
        return words[words.length - 1].replaceAll('"', "");
      }

      if (asCount === 1 && words.length === 1) {
        // The column's name is 'as' and has no alias
        return words[0].replaceAll('"', "");
      }

      return words[asPosition + 1].replaceAll('"', "");
    }

    /* Case where the column name is just a function call (i.e. COUNT(...)) */
    const wordInParensRegex = /\(([^)]*)\)/;
    const matches = wordInParensRegex.exec(targetElemToken);
    if (matches && matches.length > 0) {
      const firstParensPosition = targetElemToken.indexOf("(");
      return targetElemToken
        .substring(0, firstParensPosition)
        .toLowerCase()
        .replaceAll('"', "");
    }

    // When a column is prefixed by a table name
    const columnName = targetElemToken.split(".").pop();
    if (columnName === undefined) {
      throw new Error("Failed to parse column name");
    }
    return columnName.replaceAll('"', "");
  });

  return colNames;
}

export function getQueryType(sqlQuery: string) {
  for (const queryType of [
    "SHOW CREATE",
    "SELECT",
    "CREATE",
    "COMMIT",
    "DROP",
    "SET",
    "SHOW",
  ]) {
    // Since aliases and column names can contain these keywords, only match when they're standalone
    if (
      sqlQuery.toUpperCase().search(new RegExp(`${queryType}(\\s|;)`)) !== -1
    ) {
      return queryType;
    }
  }
}

export type SqlHandlerOptions = {
  /**
   * Number of milliseconds or a `DelayMode` to wait before returning the response.
   * Useful for simulating network delays.
   * Calls https://mswjs.io/docs/api/delay/ internally.
   */
  waitTimeMs?: number | DelayMode;
};

/**
 * Note: Use buildSqlQueryHandlerV2 instead. buildSqlQueryHandler will be deprecated once https://github.com/MaterializeInc/console/issues/1176 is complete
 *
 * An MSW handler that intercepts API calls to our /api/sql endpoint and replaces the SQL queries
 * in the request with mock queries.
 *
 * The order and type of queries in "mockQueries" must be the same as the SQL queries in the request
 * otherwise MSW will skip this handler. This is to ensure we can mock multiple API calls during a test.
 *
 * When mocking a SELECT query, the mocked columns passed must match the column names returned from the API call.
 * You can check what these are through the networks tab.
 *
 * If this function ever returns undefined, MSW skips to the next handler.
 *
 * @param mockQueries - SQL queries we want to intercept and mock using MSW
 * @param options - see `SqlHandlerOptions`
 * @returns - An MSW handler that's response follows the endpoint's API response format: https://materialize.com/docs/integrations/http-api/.
 *
 */
export function buildSqlQueryHandler(
  mockQueries: Array<SQLQuery>,
  { waitTimeMs }: SqlHandlerOptions = {},
) {
  return http.post("*/api/sql", async (info) => {
    const results: SqlResult[] = [];

    const body = await info.request.clone().json();
    if (body == null) {
      return undefined;
    }
    const { queries: requestQueries }: ExtendedRequest = body;

    if (mockQueries.length !== requestQueries.length) {
      return undefined;
    }

    for (let i = 0; i < mockQueries.length; i++) {
      const mockQuery = mockQueries[i];
      const requestQuery = requestQueries[i].query;
      const requestQueryType = getQueryType(requestQuery);

      if (mockQuery.type !== requestQueryType) {
        return undefined;
      }

      switch (mockQuery.type) {
        case "SHOW CREATE":
          results.push({
            tag: `SELECT 1`,
            desc: {
              columns: [
                { ...DEFAULT_TYPE, name: "name" },
                { ...DEFAULT_TYPE, name: "create_sql" },
              ],
            },

            rows: [[mockQuery.name, mockQuery.createSql]],
            notices: [],
          });

          break;
        case "SHOW": {
          const regex = new RegExp("SHOW (\\w*)");
          const requestShowVariableName = regex.exec(requestQuery)?.[1];
          const { column, rows } = mockQuery;

          if (column.toUpperCase() !== requestShowVariableName?.toUpperCase()) {
            return undefined;
          }

          // Replace the response with the rows from our mock query
          results.push({
            tag: `SELECT ${rows.length}`,
            desc: {
              columns: [{ name: requestShowVariableName, ...DEFAULT_TYPE }],
            },

            rows: rows,
            notices: [],
          });

          break;
        }
        case "SELECT": {
          const nameMatch =
            "queryKey" in mockQuery &&
            requestQuery.startsWith(`--${mockQuery.queryKey}`);
          const columns =
            "columns" in mockQuery
              ? mockQuery.columns
              : mockQuery.columnMetadata.map((c) => c.name);
          if (!nameMatch) {
            const requestQueryColNames =
              extractSQLSelectColumnNames(requestQuery);
            // Ensure the column names returned from the API are the same order as the mock columns.
            if (
              JSON.stringify([...columns]).toUpperCase() !==
              JSON.stringify([...requestQueryColNames]).toUpperCase()
            ) {
              return undefined;
            }
          }

          const { rows } = mockQuery;
          // Replace the response with the rows we input in our mock query.
          results.push({
            tag: `SELECT ${mockQuery.queryKey ?? rows.length}`,
            desc: {
              columns: columns.map((name) => {
                if ("columnMetadata" in mockQuery) {
                  const col = mockQuery.columnMetadata.find(
                    (c) => c.name === name,
                  );
                  assert(col);
                  return col;
                } else {
                  return {
                    name,
                    ...DEFAULT_TYPE,
                  };
                }
              }),
            },
            rows: rows,
            notices: [],
          });

          break;
        }
        default:
          results.push({
            ok: `${mockQuery.type};`,
            notices: [],
          });
      }

      // If there's a custom 'ok' or 'error' object, replace the last result with it
      if (mockQuery.ok) {
        results.pop();
        results.push({
          ok: mockQuery.ok,
          notices: [],
        });
      } else if (mockQuery.error) {
        results.pop();
        results.push({
          error: mockQuery.error,
          notices: [],
        });
      }
    }

    // Introduce a delay to simulate the network
    await delay(waitTimeMs ?? 0);
    return HttpResponse.json({ results });
  });
}

export function buildColumns(
  columns: Array<string | Partial<Column>>,
): Column[] {
  return columns.map((c) => {
    if (typeof c === "string") {
      return { name: c, ...DEFAULT_TYPE };
    }
    return {
      name: c.name ?? "",
      type_oid: c.type_oid ?? MzDataType.text,
      type_len: c.type_len ?? -1,
      type_mod: c.type_mod ?? -1,
    };
  });
}

export function buildColumn(column: Partial<Column>): Column {
  return {
    name: column.name ?? "",
    type_oid: column.type_oid ?? MzDataType.text,
    type_len: column.type_len ?? -1,
    type_mod: column.type_mod ?? -1,
  };
}

/**
 * A wrapper over buildSqlQueryHandler.
 * Returns a handler that mocks requests from useSqlLazy and useSqlTyped.
 * @param mockQuery - SQL query to mock
 * @param options - see `SqlHandlerOptions`
 * @returns
 */

export function buildUseSqlQueryHandler(
  mockQuery: SQLQuery,
  options: SqlHandlerOptions = {},
) {
  // The hooks above use only a single query and set the cluster to "mz_catalog_server" first.
  const queries = [mockQuery];
  return buildSqlQueryHandler(queries, options);
}

function approximateColumnsFromRow(row: Record<string, any>) {
  return Object.entries(row).map(([columnName, rowValue]) => {
    let isArray = false;

    if ((isArray = Array.isArray(rowValue))) {
      rowValue = rowValue[0];
    }

    if (typeof rowValue === "boolean") {
      return buildColumn({
        name: columnName,
        type_oid: getMzDataTypeOid("bool", isArray),
      });
    }

    if (typeof rowValue === "object") {
      return buildColumn({
        name: columnName,
        type_oid: getMzDataTypeOid("jsonb", isArray),
      });
    }

    return buildColumn({
      name: columnName,
      type_oid: getMzDataTypeOid("text", isArray),
    });
  });
}

/**
 * Test utility to convert rows in the 2D array format into a 1D array format.
 * Useful for converting for the React Query migration.
 * We should delete this function after the migration.
 */
export function tabularToJson(rows: any[][], columns: Column[]) {
  return rows.map((row) => {
    const jsonObj: Record<string, any> = {};

    for (let i = 0; i < row.length; i++) {
      jsonObj[columns[i].name] = row[i];
    }

    return jsonObj;
  });
}

/**
 * Maps the 1D Kysely inferred result array to Materialize's HTTP API format.
 *
 * If no columns are passed, this function will approximate the MzDataTypes of each column using the first row.
 * Specific columns can be partially overriden via the columns parameter.
 *
 */
export function mapKyselyToTabular(kyselySqlResult: {
  rows: Array<Record<string, any>>;
  columns?: Column[];
  notices?: Notice[];
  tag?: string;
}): TabularSqlResult {
  const rowLength = kyselySqlResult.rows.length;

  if (rowLength === 0 && !kyselySqlResult.columns) {
    throw new Error(
      "You must pass in at least a set of rows or a set of columns",
    );
  }

  // We use the first row of the result to build the columns and assume all rows have the same data types.
  const firstRow = kyselySqlResult.rows[0];

  let columns = kyselySqlResult?.columns ?? [];

  if (rowLength > 0) {
    columns = approximateColumnsFromRow(firstRow);

    // Partially override approximated columns
    (kyselySqlResult?.columns ?? []).forEach((columnOverride) => {
      const colIdx = columns.findIndex(
        ({ name }) => name === columnOverride.name,
      );

      if (colIdx !== -1) {
        columns[colIdx] = columnOverride;
      }
    });
  }

  const rows = kyselySqlResult.rows.map((row) => {
    return Object.values(row);
  });

  const notices = kyselySqlResult?.notices ?? ([] as Notice[]);

  const tag = kyselySqlResult?.tag ?? `SELECT ${rows.length}`;

  return { desc: { columns }, notices, tag, rows };
}

type MockResults = {
  queryKey: QueryKey;
  results: SqlResult | ReadonlyArray<SqlResult>;
};
/**
 * An MSW handler that intercepts API calls to our /api/sql endpoint and replaces the
 * response with with mock query results. This handler intercepts via the queryKey.
 *
 * If this function ever returns undefined, MSW skips to the next handler.
 *
 *
 * @param mockResults - SQL queries we want to intercept and mock using MSW
 * @param options - see `SqlHandlerOptions`
 * @returns - An MSW handler that's response follows the endpoint's API response format: https://materialize.com/docs/integrations/http-api/.
 *
 */
export function buildSqlQueryHandlerV2(
  mockResults: MockResults,
  { waitTimeMs }: SqlHandlerOptions = {},
) {
  return http.post("*/api/sql", async (info) => {
    // TODO: Put query_key in a custom header rather than a search param
    const request = info.request;

    const url = new URL(request.url);

    const isMatch =
      url.searchParams.get("query_key") === hashKey(mockResults.queryKey);

    debugSqlHandlers({
      isMatch,
      queryKey: JSON.parse(url.searchParams.get("query_key")!),
      mockQueryKey: mockResults.queryKey,
    });

    if (!isMatch) {
      return undefined;
    }

    debugSqlHandlers({
      match: JSON.stringify(mockResults),
    });

    const results = Array.isArray(mockResults.results)
      ? mockResults.results
      : [mockResults.results];

    // Introduce a delay to simulate the network
    await delay(waitTimeMs ?? 0);
    return HttpResponse.json({ results });
  });
}
