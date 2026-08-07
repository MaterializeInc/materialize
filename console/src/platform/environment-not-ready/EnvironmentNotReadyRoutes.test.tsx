// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen, waitFor } from "@testing-library/react";
import React from "react";

import server from "~/api/mocks/server";
import { dummyValidUser } from "~/external-library-wrappers/__mocks__/frontegg";
import {
  disabledEnvironment,
  healthyEnvironment,
  InitializeStateFn,
  renderComponent,
  RenderWithPathname,
  setFakeEnvironment,
} from "~/test/utils";

import { EnvironmentNotReadyRoutes } from "./EnvironmentNotReadyRoutes";

const renderRoutes = (
  initialRouterEntries: string[],
  initializeState: InitializeStateFn = ({ set }) =>
    setFakeEnvironment(set, "aws/us-east-1", disabledEnvironment),
) => {
  return renderComponent(
    <RenderWithPathname>
      <EnvironmentNotReadyRoutes user={dummyValidUser} />
    </RenderWithPathname>,
    { initializeState, initialRouterEntries },
  );
};

describe("EnvironmentNotReadyRoutes", () => {
  afterEach(() => {
    server.resetHandlers();
    vi.clearAllMocks();
  });

  it("shows the region selector on the enable-region route", async () => {
    await renderRoutes(["/enable-region"]);
    expect(
      await screen.findByText(
        "Where would you like to run your Materialize environment?",
      ),
    ).toBeVisible();
  });

  it("explains what a region is and its baseline cost", async () => {
    await renderRoutes(["/enable-region"]);
    expect(
      await screen.findByText(
        "A region is a dedicated Materialize environment",
        { exact: false },
      ),
    ).toBeVisible();
    expect(
      screen.getByText("Every new region starts with a default", {
        exact: false,
      }),
    ).toBeVisible();
    expect(screen.getByRole("link", { name: "baseline cost" })).toHaveAttribute(
      "href",
      expect.stringContaining("billing"),
    );
  });

  it("redirects the root path directly to enable-region", async () => {
    await renderRoutes(["/"]);
    await waitFor(() => {
      expect(screen.getByTestId("pathname")).toHaveTextContent(
        "/enable-region",
      );
    });
    expect(
      await screen.findByText(
        "Where would you like to run your Materialize environment?",
      ),
    ).toBeVisible();
  });

  it("redirects the removed onboarding-survey route to enable-region", async () => {
    await renderRoutes(["/onboarding-survey"]);
    await waitFor(() => {
      expect(screen.getByTestId("pathname")).toHaveTextContent(
        "/enable-region",
      );
    });
  });

  it("shows account-level navigation without an enabled environment", async () => {
    await renderRoutes(["/enable-region"]);
    // Admin items are account-scoped and must stay reachable while no
    // environment is enabled.
    expect(await screen.findByText("Admin")).toBeInTheDocument();
    expect(screen.getByText("App Passwords")).toBeInTheDocument();
    expect(screen.getByText("Usage & Billing")).toBeInTheDocument();
    // Region-scoped items and object creation are hidden in this flow.
    expect(screen.queryByText("Clusters")).not.toBeInTheDocument();
    expect(screen.queryByText("SQL Shell")).not.toBeInTheDocument();
    expect(screen.queryByText("Create New")).not.toBeInTheDocument();
  });

  it("keeps the nav account-only in this flow regardless of environment health", async () => {
    // The nav in this flow is gated on the route, not on health state, so a
    // transient "crashed" reading during boot cannot flash the full sidebar.
    await renderRoutes(["/creating-environment"], ({ set }) =>
      setFakeEnvironment(set, "aws/us-east-1", {
        ...healthyEnvironment,
        status: { ...healthyEnvironment.status, health: "crashed" },
      }),
    );
    expect(await screen.findByText("Admin")).toBeInTheDocument();
    expect(screen.queryByText("Clusters")).not.toBeInTheDocument();
    expect(screen.queryByText("Create New")).not.toBeInTheDocument();
  });

  it("does not show the welcome dialog when a region becomes ready", async () => {
    // The dialog would cover the flow's own region-ready affordances (the
    // toast and the tutorial's "Open console" button).
    await renderRoutes(["/creating-environment"], ({ set }) =>
      setFakeEnvironment(set, "aws/us-east-1", healthyEnvironment),
    );
    expect(
      await screen.findByText("We’re creating your environment"),
    ).toBeVisible();
    // The dialog mounts asynchronously, so poll for it rather than checking
    // synchronously, which would pass before it had a chance to appear. The
    // explicit timeout overrides the suite-wide asyncUtilTimeout, which would
    // otherwise spend its full budget proving this negative.
    await expect(
      screen.findByTestId("welcome-dialog-close-button", undefined, {
        timeout: 2_000,
      }),
    ).rejects.toThrow();
  });
});
