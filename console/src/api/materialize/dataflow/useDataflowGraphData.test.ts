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

import { ErrorCode } from "~/api/materialize/types";
import server from "~/api/mocks/server";
import { createProviderWrapper } from "~/test/utils";

import { useDataflowGraphData } from "./useDataflowGraphData";

const FAILING_REPLICA = "r2";
const okResult = {
  desc: { columns: [] },
  rows: [],
};
// operators result with one root row so buildDataflowStructure succeeds
const operatorsResult = {
  desc: {
    columns: [
      { name: "id" },
      { name: "address" },
      { name: "name" },
      { name: "arrangementRecords" },
      { name: "arrangementSize" },
      { name: "elapsedNs" },
    ],
  },
  rows: [["10", ["7"], "Dataflow", "0", "0", "0"]],
};

beforeEach(() => {
  server.use(
    http.post("*/api/sql", async ({ request }) => {
      const options = JSON.parse(
        new URL(request.url).searchParams.get("options") ?? "{}",
      );
      if (options.cluster_replica === FAILING_REPLICA) {
        return HttpResponse.json({
          results: [
            {
              error: {
                message: "no such replica",
                code: ErrorCode.INTERNAL_ERROR,
              },
            },
          ],
        });
      }
      return HttpResponse.json({
        results: [operatorsResult, okResult, okResult, okResult, okResult],
      });
    }),
  );
});

describe("useDataflowGraphData", () => {
  it("clears data when a refetch for new params fails", async () => {
    // The hook reads the current environment off a jotai atom that resolves
    // asynchronously, so it needs the same Suspense + JotaiProvider wrapper
    // the app renders under. Without it the atom never resolves against the
    // fake environment set up in vitest.setup.ts and the query never fires.
    const Wrapper = await createProviderWrapper();
    const { result, rerender } = renderHook(
      (params) => useDataflowGraphData(params),
      {
        wrapper: Wrapper,
        initialProps: {
          clusterName: "c",
          replicaName: "r1",
          dataflowId: "7",
        } as
          | { clusterName: string; replicaName: string; dataflowId: string }
          | undefined,
      },
    );
    await waitFor(() => expect(result.current.data).not.toBeNull());

    rerender({
      clusterName: "c",
      replicaName: FAILING_REPLICA,
      dataflowId: "7",
    });
    await waitFor(() => expect(result.current.error).toBeTruthy());
    // The retained previous result must not surface for the new params.
    expect(result.current.data).toBeNull();
  });

  it("hides the previous graph while a new selection is loading", async () => {
    const Wrapper = await createProviderWrapper();
    const { result, rerender } = renderHook(
      (params) => useDataflowGraphData(params),
      {
        wrapper: Wrapper,
        initialProps: {
          clusterName: "c",
          replicaName: "r1",
          dataflowId: "7",
        } as
          | { clusterName: string; replicaName: string; dataflowId: string }
          | undefined,
      },
    );
    await waitFor(() => expect(result.current.data).not.toBeNull());
    const firstStructure = result.current.data?.structure;

    // Switch to a different valid dataflow whose fetch also succeeds. The
    // previous fetch's results are still resident, so a naive guard would tag
    // them with the new key and render the old graph. React flushes the
    // render-phase reset synchronously, so data must already be null here.
    rerender({ clusterName: "c", replicaName: "r1", dataflowId: "8" });
    expect(result.current.data).toBeNull();

    await waitFor(() => expect(result.current.data).not.toBeNull());
    expect(result.current.data?.structure).not.toBe(firstStructure);
  });

  it("threads the replica's worker count through from workers * processes", async () => {
    server.use(
      http.post("*/api/sql", async () => {
        return HttpResponse.json({
          results: [
            operatorsResult,
            okResult,
            okResult,
            okResult,
            {
              desc: { columns: [{ name: "workerCount" }] },
              rows: [["8"]],
            },
          ],
        });
      }),
    );
    const Wrapper = await createProviderWrapper();
    const { result } = renderHook(
      () =>
        useDataflowGraphData({
          clusterName: "c",
          replicaName: "r1",
          dataflowId: "7",
        }),
      { wrapper: Wrapper },
    );
    await waitFor(() => expect(result.current.data).not.toBeNull());
    // 8, not the 1-worker fallback: proves the query result actually flows
    // through, not just that the field exists with its default value.
    expect(result.current.data?.workerCount).toEqual(8);
  });

  it("rejects a non-numeric dataflow id without ever compiling a query for it", async () => {
    const Wrapper = await createProviderWrapper();
    // A malformed id is only reachable via a hand-edited URL, so this must
    // report an error the page can render, not throw and crash to the error
    // boundary over a URL typo.
    const { result } = renderHook(
      () =>
        useDataflowGraphData({
          clusterName: "c",
          replicaName: "r1",
          dataflowId: "7; DROP",
        }),
      { wrapper: Wrapper },
    );
    expect(result.current.error).toEqual("invalid dataflow id: 7; DROP");
    expect(result.current.loading).toEqual(false);
    expect(result.current.data).toBeNull();
  });
});
