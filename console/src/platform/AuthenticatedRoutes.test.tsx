// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen } from "@testing-library/react";
import React from "react";

import { SelfManagedAppConfig } from "~/config/AppConfig";
import { appConfigAtom } from "~/config/store";
import {
  healthyEnvironment,
  renderComponent,
  setFakeEnvironment,
} from "~/test/utils";

import { AuthenticatedRoutes } from "./AuthenticatedRoutes";

// The app shell and the page itself each fetch data this test doesn't cover,
// so stub them rather than mocking every endpoint they need.
vi.mock("~/components/AppInitializer", () => ({
  AppInitializer: () => null,
}));
vi.mock("~/layouts/BaseLayout", () => ({
  BaseLayout: ({ children }: React.PropsWithChildren) => children,
}));
vi.mock("~/platform/query-history/QueryHistoryRoutes", () => ({
  default: () => <div>Query History</div>,
}));

const SELF_MANAGED_REGION_ID = "local/flexible-deployment";
const SELF_MANAGED_REGION_SLUG = "local-flexible-deployment";

describe("AuthenticatedRoutes", () => {
  it("renders query history in self-managed deployments", async () => {
    await renderComponent(<AuthenticatedRoutes />, {
      initialRouterEntries: [
        `/regions/${SELF_MANAGED_REGION_SLUG}/query-history`,
      ],
      initializeState: async ({ set }) => {
        set(appConfigAtom, new SelfManagedAppConfig());
        await setFakeEnvironment(
          set,
          SELF_MANAGED_REGION_ID,
          healthyEnvironment,
        );
      },
    });

    expect(await screen.findByText("Query History")).toBeVisible();
  });
});
