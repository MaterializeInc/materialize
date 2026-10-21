// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import * as Sentry from "@sentry/react";
import { sql } from "kysely";
import { http, HttpResponse } from "msw";

import { buildQueryKeyPart } from "~/api/buildQueryKeySchema";
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
import { executeSqlV2, queryKeyLabel, spanErrorStatus } from "./executeSqlV2";
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

    await expect(
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

    await expect(
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

    await expect(
      executeSqlV2({
        queries: sql`SELECT foo from mz_secrets;`.compile(queryBuilder),
        queryKey: ["databaseErrorQuery"],
      }),
    ).rejects.toThrowError(DatabaseError);
  });
});

describe("queryKeyLabel", () => {
  it("joins the scope of each part", () => {
    expect(
      queryKeyLabel([
        {
          scope: "clusters",
          regionId: "aws/us-east-1",
          environmentVersion: "v26.45.0",
        },
        { scope: "list", clusterId: "u3" },
      ]),
    ).toBe("clusters.list");
  });

  it("leaves out parts without a scope", () => {
    expect(
      queryKeyLabel([
        { scope: "replicaUtilization" },
        "u3",
        "deploymentLineage",
      ]),
    ).toBe("replicaUtilization");
  });

  it("names a key with no scope", () => {
    expect(queryKeyLabel(["createSecret"])).toBe("unscoped query");
  });
});

describe("spanErrorStatus", () => {
  it("reads a timeout as deadline_exceeded", () => {
    expect(
      spanErrorStatus(new DOMException("signal timed out", "TimeoutError")),
    ).toBe("deadline_exceeded");
  });

  it("reads any other failure as internal_error", () => {
    expect(spanErrorStatus(new Error("column does not exist"))).toBe(
      "internal_error",
    );
  });
});

describe("executeSqlV2 Sentry spans", () => {
  const transactions: Sentry.Event[] = [];
  const queryKey = [buildQueryKeyPart("clusters"), buildQueryKeyPart("list")];
  const queries = sql`SELECT 1`.compile(queryBuilder);

  beforeAll(() => {
    Sentry.init({
      dsn: "https://public@sentry.invalid/1",
      defaultIntegrations: false,
      tracesSampleRate: 1,
      beforeSendTransaction: (event) => {
        transactions.push(event);
        return null;
      },
    });
  });

  afterAll(() => {
    Sentry.init({});
  });

  beforeEach(() => {
    transactions.length = 0;
    server.use(
      buildSqlQueryHandlerV2({
        queryKey,
        results: mapKyselyToTabular({ rows: [], columns: [] }),
      }),
    );
  });

  // Observed: a root span started outside `startNewTrace` in this file comes
  // out unsampled, even with `tracesSampleRate: 1`.
  function inNewTrace<T>(name: string, callback: () => Promise<T>) {
    return Sentry.startNewTrace(() => Sentry.startSpan({ name }, callback));
  }

  async function sentTransaction(name: string) {
    await vi.waitFor(() =>
      expect(transactions.map((t) => t.transaction)).toContain(name),
    );
    return transactions;
  }

  it("records a span named after the query key under an active span", async () => {
    await inNewTrace("page load", () => executeSqlV2({ queries, queryKey }));

    const [pageLoad] = await sentTransaction("page load");
    expect(pageLoad.spans).toContainEqual(
      expect.objectContaining({
        description: "clusters.list",
        op: "db.sql.query",
      }),
    );
  });

  it("starts no trace when no span is active", async () => {
    await Sentry.startNewTrace(() => executeSqlV2({ queries, queryKey }));
    await inNewTrace("marker", async () => undefined);

    const sent = await sentTransaction("marker");
    expect(sent.map((t) => t.transaction)).toEqual(["marker"]);
  });

  it("marks a request the caller aborted as cancelled", async () => {
    server.use(
      buildSqlQueryHandlerV2(
        { queryKey, results: mapKyselyToTabular({ rows: [], columns: [] }) },
        { waitTimeMs: 5_000 },
      ),
    );
    const controller = new AbortController();
    setTimeout(() => controller.abort(), 20);

    await inNewTrace("page load", async () => {
      await expect(
        executeSqlV2({
          queries,
          queryKey,
          requestOptions: { signal: controller.signal },
        }),
      ).rejects.toThrow();
    });

    const [pageLoad] = await sentTransaction("page load");
    expect(pageLoad.spans).toContainEqual(
      expect.objectContaining({
        description: "clusters.list",
        status: "cancelled",
      }),
    );
  });
});
