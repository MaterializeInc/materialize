// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { renderHook, waitFor } from "@testing-library/react";
import { delay, http } from "msw";
import { describe, expect, it } from "vitest";

import server from "~/api/mocks/server";
import {
  FreshnessObject,
  useClusterFreshness,
} from "~/platform/clusters/queries";
import { createProviderWrapper } from "~/test/utils";

import { useFreshnessHydration } from "./queries";

const objects: FreshnessObject[] = [
  {
    objectId: "u1",
    objectName: "orders_mv",
    schemaName: "public",
    databaseName: "materialize",
    objectType: "materialized-view",
  },
];

describe("freshness queries", () => {
  it("sends every request before any has answered", async () => {
    const pending: string[] = [];
    server.use(
      http.post("*/api/sql", async ({ request }) => {
        pending.push(new URL(request.url).searchParams.get("query_key") ?? "");
        await delay("infinite");
      }),
    );

    // The page's hooks, in the page's order.
    const ProviderWrapper = await createProviderWrapper();
    renderHook(
      () => {
        useClusterFreshness({ lookbackMs: 3_600_000, objects });
        useFreshnessHydration(["u1"]);
      },
      { wrapper: ProviderWrapper },
    );

    await waitFor(() => expect(pending).toHaveLength(3));
  });
});
