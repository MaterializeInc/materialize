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

import { useDataflowList } from "./useDataflowList";

const FAILING_REPLICA = "r2";
const COLUMNS = [
  { name: "id" },
  { name: "name" },
  { name: "records" },
  { name: "size" },
  { name: "elapsedNs" },
];
const listResult = {
  desc: { columns: COLUMNS },
  rows: [["7", "Dataflow: mv", "100", "4096", "12345"]],
};
// Distinct ids from listResult: dataflow ids are per-replica, so a stale row
// from the wrong replica reads as an entirely different, meaningless entry
// rather than merely out of date.
const otherReplicaResult = {
  desc: { columns: COLUMNS },
  rows: [["9", "Dataflow: other_mv", "200", "8192", "54321"]],
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
      if (options.cluster_replica === "r3") {
        return HttpResponse.json({ results: [otherReplicaResult] });
      }
      return HttpResponse.json({ results: [listResult] });
    }),
  );
});

describe("useDataflowList", () => {
  it("maps rows to DataflowListEntry with bigint fields", async () => {
    const Wrapper = await createProviderWrapper();
    const { result } = renderHook(
      () => useDataflowList({ clusterName: "c", replicaName: "r1" }),
      { wrapper: Wrapper },
    );
    await waitFor(() => expect(result.current.data).not.toBeNull());
    expect(result.current.data?.[0]).toEqual({
      id: "7",
      name: "Dataflow: mv",
      records: 100n,
      size: 4096n,
      elapsedNs: 12345n,
    });
  });

  it("hides the previous replica's list while the new one is loading", async () => {
    const Wrapper = await createProviderWrapper();
    const { result, rerender } = renderHook(
      (replicaName: string) =>
        useDataflowList({ clusterName: "c", replicaName }),
      { wrapper: Wrapper, initialProps: "r1" },
    );
    await waitFor(() => expect(result.current.data).not.toBeNull());
    expect(result.current.data?.[0].id).toBe("7");

    // Switching replicas must not show r1's still-resident rows tagged as
    // r3's: dataflow ids are per-replica, so navigating one would be
    // meaningless, and a transient dataflow gone on r3 would crash instead.
    rerender("r3");
    expect(result.current.data).toBeNull();

    await waitFor(() => expect(result.current.data).not.toBeNull());
    expect(result.current.data?.[0].id).toBe("9");
  });

  it("clears data when the fetch fails", async () => {
    // The hook reads the current environment off a jotai atom that resolves
    // asynchronously, so it needs the same Suspense + JotaiProvider wrapper the
    // app renders under.
    const Wrapper = await createProviderWrapper();
    const { result } = renderHook(
      () => useDataflowList({ clusterName: "c", replicaName: FAILING_REPLICA }),
      { wrapper: Wrapper },
    );
    await waitFor(() => expect(result.current.error).toBeTruthy());
    expect(result.current.data).toBeNull();
  });
});
