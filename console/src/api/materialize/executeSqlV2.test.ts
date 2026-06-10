// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { sql } from "kysely";
import { http, HttpResponse } from "msw";

import {
  mapColumnToColumnMetadata,
  UNAUTHORIZED_ERROR,
} from "~/api/materialize";
import { queryBuilder } from "~/api/materialize/db";
import {
  buildColumn,
  buildSqlQueryHandlerV2,
  mapKyselyToTabular,
} from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";

import DatabaseError from "./DatabaseError";
import { executeSqlV2 } from "./executeSqlV2";
import { ErrorCode, MzDataType, Notice, OkSqlResult } from "./types";

describe("executeSqlV2", () => {
  it("Should transform a single query", async () => {
    const selectAllSecretsCompiledQuery = queryBuilder
      .selectFrom("mz_secrets")
      .selectAll()
      .compile();

    const selectAllSecretsResult = [
      {
        id: "u2373",
        oid: "72364",
        schema_id: "u3",
        name: "hi",
        owner_id: "u1",
        privileges: ["u1=U/u1"],
      },
      {
        id: "u4",
        oid: "20802",
        schema_id: "u3",
        name: "test_secret",
        owner_id: "u1",
        privileges: ["u1=U/u1"],
      },
    ];

    const mockResults = {
      queryKey: ["secrets"],
      results: mapKyselyToTabular({
        rows: selectAllSecretsResult,
        notices: [
          {
            message: "Warning: cluster OOMing.",
            severity: "Warning" as const,
          },
        ],
        columns: [
          buildColumn({ name: "privileges", type_oid: MzDataType._mz_aclitem }),
        ],
      }),
    };

    server.use(buildSqlQueryHandlerV2(mockResults));

    const result = await executeSqlV2({
      queries: selectAllSecretsCompiledQuery,
      queryKey: ["secrets"],
    });

    expect(result.tag).toEqual(mockResults.results.tag);

    expect(result.notices).toEqual(mockResults.results.notices);

    expect(result.columns).toEqual(
      mockResults.results.desc.columns.map(mapColumnToColumnMetadata),
    );

    expect(result.rows).toEqual(selectAllSecretsResult);
  });

  it("Should transform multiple queries", async () => {
    const beginCompiledQuery = sql<OkSqlResult>`BEGIN`.compile(queryBuilder);
    const createTempTableCompiledQuery =
      sql<OkSqlResult>`CREATE TEMPORARY TABLE t (name text, age numeric)`.compile(
        queryBuilder,
      );

    const selectSourcesCompiledQuery = queryBuilder
      .selectFrom("mz_sources")
      .select(["id", "name"])
      .compile();

    const selectSourcesResult = [
      { id: "u11", name: "bids" },
      { id: "u13", name: "users" },
      { id: "u9", name: "accounts" },
      { id: "u10", name: "auctions" },
    ];

    const mockResults = {
      queryKey: ["beginCreateSelect"],
      results: [
        { ok: "BEGIN", notices: [] as Notice[] },
        { ok: "CREATE TABLE", notices: [] as Notice[] },
        mapKyselyToTabular({ rows: selectSourcesResult }),
      ] as const,
    };

    server.use(buildSqlQueryHandlerV2(mockResults));

    const result = await executeSqlV2({
      queries: [
        beginCompiledQuery,
        createTempTableCompiledQuery,
        selectSourcesCompiledQuery,
      ] as const,
      queryKey: ["beginCreateSelect"],
    });

    expect(result[0].ok === mockResults.results[0].ok);

    expect(result[1].ok === mockResults.results[1].ok);

    expect(result[2].tag === mockResults.results[2].tag);
  });

  it("Should throw when the fetch call fails", async () => {
    const errorMessage =
      "Expected a keyword at the beginning of a statement, found identifier 'Invalid keyword'";
    server.use(
      http.post("*/api/sql", async () => {
        return new HttpResponse(errorMessage, {
          status: 400,
        });
      }),
    );

    expect(
      executeSqlV2({
        queries: sql`Invalid keyword`.compile(queryBuilder),
        queryKey: ["invalidQuery"],
      }),
    ).rejects.toThrow(errorMessage);
  });

  it("Should throw an 'unauthorized' error when unauthorized", async () => {
    server.use(
      http.post("*/api/sql", async () => {
        return new HttpResponse("unauthorized", {
          status: 401,
        });
      }),
    );

    expect(
      executeSqlV2({
        queries: sql`SELECT 1`.compile(queryBuilder),
        queryKey: ["unauthorized"],
      }),
    ).rejects.toThrow(UNAUTHORIZED_ERROR);
  });

  it("Should throw a database error when the query fails", async () => {
    const mockResults = {
      queryKey: ["databaseErrorQuery"],
      results: {
        error: {
          message: 'column "foo" does not exist',
          code: ErrorCode.DATA_CORRUPTED,
        },
        notices: [],
      },
    };

    server.use(buildSqlQueryHandlerV2(mockResults));

    expect(
      executeSqlV2({
        queries: sql`SELECT foo from mz_secrets;`.compile(queryBuilder),
        queryKey: ["databaseErrorQuery"],
      }),
    ).rejects.toThrowError(DatabaseError);
  });
});
