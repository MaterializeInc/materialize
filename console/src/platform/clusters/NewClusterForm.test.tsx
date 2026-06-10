// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen } from "@testing-library/react";
import { userEvent } from "@testing-library/user-event";
import React from "react";
import { Route, Routes } from "react-router-dom";

import { ErrorCode } from "~/api/materialize/types";
import {
  buildColumns,
  buildSqlQueryHandler,
  buildSqlQueryHandlerV2,
  buildUseSqlQueryHandler,
  mapKyselyToTabular,
} from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { validClustersResponse } from "~/test/clusterQueryBuilders";
import { renderComponent } from "~/test/utils";

import NewClusterForm from "./NewClusterForm";
import { clusterQueryKeys } from "./queries";

const ClusterFormRoutes = ({ children }: { children: React.ReactNode }) => {
  return (
    <Routes>
      <Route path="/:id/:name" element={<div>Cluster Details</div>} />
      <Route path="/new-cluster" element={children} />
    </Routes>
  );
};

describe("NewClusterForm", () => {
  beforeEach(() => {
    server.use(validClustersResponse);
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: clusterQueryKeys.availableClusterSizes(),
        results: [
          mapKyselyToTabular({
            rows: [
              {
                allowed_cluster_replica_sizes: "",
              },
            ],
            columns: buildColumns(["allowed_cluster_replica_sizes"]),
          }),
          mapKyselyToTabular({
            rows: [
              {
                size: "3xsmall",
              },
              {
                size: "2xsmall",
              },
            ],
            columns: buildColumns(["size"]),
          }),
        ],
      }),
    );
    server.use(
      buildUseSqlQueryHandler({
        type: "SHOW" as const,
        column: "max_replicas_per_cluster",
        rows: [[5]],
      }),
    );
  });

  it("creates a cluster successfully and redirects to the cluster list", async () => {
    server.use(
      buildSqlQueryHandler([
        { type: "CREATE" as const },
        { type: "SELECT" as const, columns: ["id"], rows: [["u3"]] },
      ]),
    );
    server.use(validClustersResponse);
    const user = userEvent.setup();
    renderComponent(
      <ClusterFormRoutes>
        <NewClusterForm />
      </ClusterFormRoutes>,
      {
        initialRouterEntries: ["/new-cluster"],
      },
    );

    const clusterNameInput = await screen.findByLabelText("Name");
    await user.type(clusterNameInput, "test_cluster");

    const clusterSizeInput = await screen.findByLabelText("Size");
    await user.click(clusterSizeInput);

    await user.click(screen.getByText("3xsmall"));

    await user.click(screen.getByText("Create cluster"));

    expect(await screen.findByText("Cluster Details")).toBeVisible();
  });

  it("properly filters size list to allowed sizes", async () => {
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: clusterQueryKeys.availableClusterSizes(),
        results: [
          mapKyselyToTabular({
            rows: [
              {
                allowed_cluster_replica_sizes: '"3xsmall"',
              },
            ],
            columns: buildColumns(["allowed_cluster_replica_sizes"]),
          }),
          mapKyselyToTabular({
            rows: [
              {
                size: "3xsmall",
              },
              {
                size: "2xsmall",
              },
            ],
            columns: buildColumns(["size"]),
          }),
        ],
      }),
    );

    const user = userEvent.setup();
    renderComponent(
      <ClusterFormRoutes>
        <NewClusterForm />
      </ClusterFormRoutes>,
      {
        initialRouterEntries: ["/new-cluster"],
      },
    );

    const clusterSizeInput = await screen.findByLabelText("Size");
    await user.click(clusterSizeInput);

    expect(screen.queryByText("2xsmall")).toBeNull();
    expect(screen.queryByText("3xsmall")).not.toBeNull();
  });

  it("shows an error for missing cluster names", async () => {
    const user = userEvent.setup();
    renderComponent(
      <ClusterFormRoutes>
        <NewClusterForm />
      </ClusterFormRoutes>,
      {
        initialRouterEntries: ["/new-cluster"],
      },
    );

    await user.click(await screen.findByText("Create cluster"));

    expect(await screen.findByText("Cluster name is required.")).toBeVisible();
  });

  it("shows the database error when an unexpected error occurs ", async () => {
    server.use(
      buildSqlQueryHandler([
        {
          type: "CREATE" as const,
          error: {
            message: "some unexpected database error",
            code: ErrorCode.INTERNAL_ERROR,
          },
        },
        { type: "SELECT" as const, columns: ["id"], rows: [["u3"]] },
      ]),
    );

    const user = userEvent.setup();

    renderComponent(
      <ClusterFormRoutes>
        <NewClusterForm />
      </ClusterFormRoutes>,
      {
        initialRouterEntries: ["/new-cluster"],
      },
    );

    const clusterNameInput = await screen.findByText("Name");
    await user.type(clusterNameInput, "default");
    await user.click(screen.getByText("Create cluster"));

    expect(
      await screen.findByText("some unexpected database error"),
    ).toBeVisible();
  });
});
