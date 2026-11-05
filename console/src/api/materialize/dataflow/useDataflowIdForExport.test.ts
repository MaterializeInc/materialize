// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { renderHook, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";

import server from "~/api/mocks/server";
import { createProviderWrapper } from "~/test/utils";

import { useDataflowIdForExport } from "./useDataflowIdForExport";

// The export id whose dataflow the mock knows about; anything else resolves
// to no rows, the same way a dropped or never-running export would.
const KNOWN_EXPORT = "u123";

// Records what actually reached the endpoint, so a test can assert that a
// rejected export id never produced a request at all.
let requestedQueries: string[] = [];

beforeEach(() => {
  requestedQueries = [];
  server.use(
    http.post("*/api/sql", async ({ request }) => {
      const body = (await request.json()) as { queries: { query: string }[] };
      requestedQueries.push(...body.queries.map((q) => q.query));
      const found = body.queries[0].query.includes(KNOWN_EXPORT);
      return HttpResponse.json({
        results: [
          {
            desc: { columns: [{ name: "dataflowId" }] },
            rows: found ? [["7"]] : [],
          },
        ],
      });
    }),
  );
});

describe("useDataflowIdForExport", () => {
  it("resolves the dataflow id backing an export", async () => {
    const Wrapper = await createProviderWrapper();
    const { result } = renderHook(
      () =>
        useDataflowIdForExport({
          clusterName: "c",
          replicaName: "r1",
          exportId: KNOWN_EXPORT,
        }),
      { wrapper: Wrapper },
    );
    await waitFor(() => expect(result.current.dataflowId).toBe("7"));
  });

  it("reports no dataflow when the export has none running", async () => {
    const Wrapper = await createProviderWrapper();
    const { result } = renderHook(
      () =>
        useDataflowIdForExport({
          clusterName: "c",
          replicaName: "r1",
          exportId: "u999",
        }),
      { wrapper: Wrapper },
    );
    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(result.current.dataflowId).toBeNull();
    expect(result.current.error).toBeFalsy();
  });

  // Only a hand-edited URL can supply one of these, and the guard exists so
  // it never reaches the query text at all.
  it.each(["not-an-id", "u1; DROP TABLE t", "s1'", "1", ""])(
    "compiles no query for the malformed export id %j",
    async (exportId) => {
      const Wrapper = await createProviderWrapper();
      const { result } = renderHook(
        () =>
          useDataflowIdForExport({
            clusterName: "c",
            replicaName: "r1",
            exportId,
          }),
        { wrapper: Wrapper },
      );
      await waitFor(() => expect(result.current.loading).toBe(false));
      expect(result.current.dataflowId).toBeNull();
      expect(requestedQueries).toEqual([]);
    },
  );

  it("accepts a system, transient or user export id", async () => {
    for (const exportId of ["s1", "t42", "u123"]) {
      const Wrapper = await createProviderWrapper();
      renderHook(
        () =>
          useDataflowIdForExport({
            clusterName: "c",
            replicaName: "r1",
            exportId,
          }),
        { wrapper: Wrapper },
      );
      await waitFor(() =>
        expect(requestedQueries.some((q) => q.includes(exportId))).toBe(true),
      );
    }
  });
});
