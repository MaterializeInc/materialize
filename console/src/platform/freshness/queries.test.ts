// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { renderHook, waitFor } from "@testing-library/react";
import { delay, http, HttpResponse } from "msw";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  buildColumns,
  mapKyselyToTabular,
} from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import {
  FreshnessObject,
  useClusterFreshness,
} from "~/platform/clusters/queries";
import { createProviderWrapper } from "~/test/utils";

import { useFreshnessHydration, useLatestLag } from "./queries";

const objects: FreshnessObject[] = [
  {
    objectId: "u1",
    objectName: "orders_mv",
    schemaName: "public",
    databaseName: "materialize",
    objectType: "materialized-view",
  },
];

// Records each SQL request and never answers, so a test sees which requests
// start before any has returned.
const recordRequests = () => {
  const pending: string[] = [];
  server.use(
    http.post("*/api/sql", async ({ request }) => {
      pending.push(new URL(request.url).searchParams.get("query_key") ?? "");
      await delay("infinite");
    }),
  );
  return pending;
};

describe("freshness queries", () => {
  it("sends every request before any has answered", async () => {
    const pending = recordRequests();

    // The page's hooks, in the page's order.
    const ProviderWrapper = await createProviderWrapper();
    renderHook(
      () => {
        useClusterFreshness({ lookbackMs: 3_600_000, objects });
        useLatestLag(["u1"]);
        useFreshnessHydration(["u1"]);
      },
      { wrapper: ProviderWrapper },
    );

    await waitFor(() => expect(pending).toHaveLength(3));
  });

  it("asks only for the series when only the graph is shown", async () => {
    // Cluster Overview's card reads the series alone.
    const pending = recordRequests();

    const ProviderWrapper = await createProviderWrapper();
    renderHook(() => useClusterFreshness({ lookbackMs: 3_600_000, objects }), {
      wrapper: ProviderWrapper,
    });

    await waitFor(() => expect(pending).toHaveLength(1));
    // Long enough for a second request started in the same render to land.
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(pending).toHaveLength(1);
  });

  describe("when the page is opened again", () => {
    const DAY_MS = 86_400_000;
    let requests = 0;

    // Answers the series with no rows and counts each request.
    beforeEach(() => {
      requests = 0;
      server.use(
        http.post("*/api/sql", () => {
          requests += 1;
          return HttpResponse.json({
            results: [
              mapKyselyToTabular({
                columns: buildColumns(["bucketStart", "objectId", "lag"]),
                rows: [],
              }),
            ],
          });
        }),
      );
      vi.useFakeTimers({ toFake: ["Date"] });
    });

    afterEach(() => vi.useRealTimers());

    const openPage = async () => {
      const ProviderWrapper = await createProviderWrapper();
      const view = renderHook(
        () => useClusterFreshness({ lookbackMs: DAY_MS, objects }),
        { wrapper: ProviderWrapper },
      );
      await waitFor(() => expect(view.result.current.data).toBeDefined());
      return view;
    };

    it("refetches a series older than a reading, though its bin is 24 minutes", async () => {
      (await openPage()).unmount();
      vi.setSystemTime(Date.now() + 61_000);

      // The cached series shows at once and the refetch runs behind it.
      await openPage();
      await waitFor(() => expect(requests).toBe(2));
    });

    it("reuses a series fetched within the last reading", async () => {
      (await openPage()).unmount();
      vi.setSystemTime(Date.now() + 30_000);

      await openPage();
      expect(requests).toBe(1);
    });
  });
});
