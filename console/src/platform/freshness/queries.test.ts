// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { renderHook, waitFor } from "@testing-library/react";
import { createStore } from "jotai";
import { delay, http, HttpResponse } from "msw";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { DatabaseObject } from "~/api/materialize/objects";
import {
  buildColumns,
  mapKyselyToTabular,
} from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import {
  FreshnessObject,
  useClusterFreshness,
  useFreshnessObjectsWhere,
} from "~/platform/clusters/queries";
import { allObjects } from "~/store/allObjects";
import { createProviderWrapper } from "~/test/utils";

import { FreshnessScope } from "./constants";
import {
  inFreshnessScope,
  useFreshnessHydration,
  useLatestLag,
} from "./queries";

const objects: FreshnessObject[] = [
  {
    objectId: "u1",
    objectName: "orders_mv",
    schemaName: "public",
    databaseName: "materialize",
    schemaId: "u3",
    clusterId: "u1",
    clusterName: "quickstart",
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

const buildObject = (
  overrides: Partial<DatabaseObject> & { id: string },
): DatabaseObject =>
  ({
    name: overrides.id,
    objectType: "materialized-view",
    schemaId: "u3",
    schemaName: "public",
    databaseId: "u1",
    databaseName: "materialize",
    sourceType: null,
    isWebhookTable: "false",
    clusterId: "u1",
    clusterName: "quickstart",
    ...overrides,
  }) as DatabaseObject;

// Schema u3 holds objects on two clusters and none, of every type the page
// either shows or must skip. Schema u9 and cluster u2 hold the decoys.
const ENVIRONMENT = [
  buildObject({ id: "u10", objectType: "table", clusterId: null }),
  buildObject({ id: "u11", objectType: "materialized-view", clusterId: "u1" }),
  buildObject({ id: "u12", objectType: "index", clusterId: "u2" }),
  buildObject({ id: "u13", objectType: "source", clusterId: "u2" }),
  buildObject({ id: "u14", objectType: "sink", clusterId: "u1" }),
  buildObject({ id: "u15", objectType: "view", clusterId: null }),
  buildObject({ id: "u16", objectType: "secret", clusterId: null }),
  buildObject({ id: "u17", objectType: "connection", clusterId: null }),
  buildObject({
    id: "u18",
    objectType: "source",
    sourceType: "subsource",
    clusterId: "u2",
  }),
  buildObject({ id: "u20", schemaId: "u9", clusterId: "u1" }),
];

async function objectIdsIn(scope: FreshnessScope, scopeId: string) {
  const store = createStore();
  store.set(allObjects, {
    data: ENVIRONMENT,
    error: undefined,
    snapshotComplete: true,
  });
  const ProviderWrapper = await createProviderWrapper({ store });
  const { result } = renderHook(
    () => useFreshnessObjectsWhere(inFreshnessScope(scope, scopeId)),
    { wrapper: ProviderWrapper },
  );
  return result.current.map((object) => object.objectId);
}

describe("inFreshnessScope", () => {
  it("gathers a schema's objects that report lag, whichever cluster runs them", async () => {
    expect(await objectIdsIn("schema", "u3")).toEqual([
      "u10",
      "u11",
      "u12",
      "u13",
      "u14",
    ]);
  });

  it("gathers a cluster's objects, which never include a table", async () => {
    expect(await objectIdsIn("cluster", "u2")).toEqual(["u12", "u13"]);
  });
});
