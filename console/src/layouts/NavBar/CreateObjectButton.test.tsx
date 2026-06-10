// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
import { Route, Routes, useLocation } from "react-router-dom";

import { CAN_CREATE_CLUSTERS_QUERY_KEY } from "~/api/materialize/cluster/useCanCreateCluster";
import { USE_CAN_CREATE_OBJECTS_QUERY_KEY } from "~/api/materialize/useCanCreateObjects";
import { buildUseSqlQueryHandler } from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { createProviderWrapper } from "~/test/utils";

import { CreateObjectButton } from "./CreateObjectButton";

const Wrapper = await createProviderWrapper();

const renderComponent = () => {
  return render(
    <Wrapper>
      <Routes>
        <Route path="/" element={<CreateObjectButton isCollapsed={false} />} />
        <Route
          path="/regions/:region/sources/new"
          element={<div>New source form</div>}
        />
        <Route
          path="/regions/:region/clusters/new"
          element={<div>New cluster form</div>}
        />
        <Route
          path="/access/app-passwords"
          element={
            <div>
              App passwords page <RouterState />
            </div>
          }
        />
      </Routes>
    </Wrapper>,
  );
};

const RouterState = () => {
  const location = useLocation();
  return <div>{JSON.stringify(location.state)}</div>;
};

describe("CreateObjectButton", () => {
  beforeEach(() => {
    history.pushState(undefined, "", "/");
    server.use(
      buildUseSqlQueryHandler({
        type: "SELECT" as const,
        columns: ["canCreateObjects"],
        queryKey: USE_CAN_CREATE_OBJECTS_QUERY_KEY,
        rows: [[true]],
      }),
      buildUseSqlQueryHandler({
        type: "SELECT" as const,
        columns: ["canCreateCluster"],
        queryKey: CAN_CREATE_CLUSTERS_QUERY_KEY,
        rows: [[true]],
      }),
    );
  });

  it("Button opens a menu with object types", async () => {
    renderComponent();
    const user = userEvent.setup();

    const button = await screen.findByRole("button", { name: "Create New" });
    expect(button).toBeVisible();

    await user.click(button);
    expect(await screen.findByRole("link", { name: "Cluster" })).toBeVisible();
    expect(await screen.findByRole("link", { name: "Source" })).toBeVisible();
    expect(
      await screen.findByRole("link", { name: "App Password" }),
    ).toBeVisible();
  });

  it("Clicking cluster shows the new cluster form", async () => {
    renderComponent();
    const user = userEvent.setup();
    await user.click(await screen.findByRole("button", { name: "Create New" }));
    await user.click(await screen.findByRole("link", { name: "Cluster" }));
    expect(await screen.findByText("New cluster form")).toBeVisible();
  });

  it("Clicking app password shows the app password form", async () => {
    renderComponent();
    const user = userEvent.setup();
    await user.click(await screen.findByRole("button", { name: "Create New" }));
    await user.click(await screen.findByRole("link", { name: "App Password" }));
    expect(await screen.findByText("App passwords page")).toBeVisible();
    // Since this form doesn't have a dedicated url, we can only test that the state was
    // passed correctly.
    expect(await screen.findByText(`{"new":true}`)).toBeVisible();
  });
});
