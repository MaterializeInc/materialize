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

import { dummyValidUser } from "~/external-library-wrappers/__mocks__/frontegg";
import { User } from "~/external-library-wrappers/frontegg";
import EnvironmentSelect from "~/layouts/EnvironmentSelect";
import {
  creatingEnvironment,
  disabledEnvironment,
  healthyEnvironment,
  InitializeStateFn,
  renderComponent,
  setFakeEnvironment,
} from "~/test/utils";

const renderEnvironmentSelect = ({
  initializeState,
  user,
}: {
  initializeState?: InitializeStateFn;
  user?: User;
}) => {
  return renderComponent(
    <div data-testid="wrapper">
      <EnvironmentSelect user={user ?? dummyValidUser} />
    </div>,
    { initializeState },
  );
};

describe("EnvironmentSelect", () => {
  it("shows nothing if you don't have environment permissions", async () => {
    renderEnvironmentSelect({
      user: {
        ...dummyValidUser,
        permissions: [],
      },
      initializeState: ({ set }) =>
        setFakeEnvironment(set, "aws/us-east-1", healthyEnvironment),
    });

    expect(await screen.findByTestId("wrapper")).toBeEmptyDOMElement();
    vi.resetAllMocks();
  });

  it("show nothing if you don't have any regions enabled", async () => {
    renderEnvironmentSelect({
      initializeState: ({ set }) =>
        setFakeEnvironment(set, "aws/us-east-1", disabledEnvironment),
    });

    expect(await screen.findByTestId("wrapper")).toBeEmptyDOMElement();
  });

  it("shows the current region and state", async () => {
    renderEnvironmentSelect({
      initializeState: ({ set }) =>
        setFakeEnvironment(set, "aws/us-east-1", healthyEnvironment),
    });

    expect(await screen.findByTestId("wrapper")).toBeInTheDocument();
    expect(screen.getByText("aws/us-east-1")).toBeVisible();
    expect(screen.getByTestId("health-healthy")).toBeVisible();
  });

  it("shows creating state", async () => {
    renderEnvironmentSelect({
      initializeState: ({ set }) =>
        setFakeEnvironment(set, "aws/us-east-1", creatingEnvironment),
    });

    expect(await screen.findByTestId("wrapper")).toBeInTheDocument();
    expect(screen.getByTestId("health-creating")).toBeVisible();
  });

  it("shows booting state", async () => {
    renderEnvironmentSelect({
      initializeState: ({ set }) =>
        setFakeEnvironment(set, "aws/us-east-1", {
          ...healthyEnvironment,
          status: { health: "booting", errors: [] },
        }),
    });

    expect(await screen.findByTestId("wrapper")).toBeInTheDocument();
    expect(screen.getByTestId("health-booting")).toBeVisible();
  });

  it("shows blocked state", async () => {
    renderEnvironmentSelect({
      initializeState: ({ set }) =>
        setFakeEnvironment(set, "aws/us-east-1", {
          ...healthyEnvironment,
          status: { health: "blocked", errors: [] },
        }),
    });

    expect(await screen.findByTestId("wrapper")).toBeInTheDocument();
    expect(screen.getByTestId("health-blocked")).toBeVisible();
  });

  it("shows crashed state when environment is crashed", async () => {
    renderEnvironmentSelect({
      initializeState: ({ set }) =>
        setFakeEnvironment(set, "aws/us-east-1", {
          ...healthyEnvironment,
          status: { health: "crashed", errors: [] },
        }),
    });

    expect(await screen.findByTestId("wrapper")).toBeInTheDocument();
    expect(screen.getByTestId("health-crashed")).toBeVisible();
  });
});
