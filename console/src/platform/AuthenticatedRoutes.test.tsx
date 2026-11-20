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

import { CloudAppConfig, SelfManagedAppConfig } from "~/config/AppConfig";
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
vi.mock("~/platform/support/SupportPage", () => ({
  default: () => <div>Support page</div>,
}));
vi.mock("~/hooks/useFlags", () => ({
  useFlags: () => ({ "support-page-cns-147": true }),
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

  it("renders the support page in Cloud", async () => {
    await renderComponent(<AuthenticatedRoutes />, {
      initialRouterEntries: ["/support"],
      initializeState: async ({ set }) => {
        set(appConfigAtom, new CloudAppConfig());
        await setFakeEnvironment(set, "aws/us-east-1", healthyEnvironment);
      },
    });

    expect(await screen.findByText("Support page")).toBeVisible();
  });

  it("keeps impersonation off the support page", async () => {
    // Impersonation sends every global API request as a GET, so opening a
    // ticket would look like it worked.
    const appConfig = new CloudAppConfig();
    appConfig.isImpersonating = true;
    await renderComponent(<AuthenticatedRoutes />, {
      initialRouterEntries: ["/support"],
      initializeState: async ({ set }) => {
        set(appConfigAtom, appConfig);
        await setFakeEnvironment(set, "aws/us-east-1", healthyEnvironment);
      },
    });

    // The page renders synchronously when it's allowed, as the Cloud test
    // above shows, so it's absent from the first render here.
    expect(screen.queryByText("Support page")).not.toBeInTheDocument();
  });
});
