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
});
