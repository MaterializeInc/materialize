// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";

import { ErrorCode } from "~/api/materialize/types";
import { buildSqlQueryHandlerV2 } from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { emptyClustersResponse } from "~/test/clusterQueryBuilders";
import { buildCluster, renderComponent } from "~/test/utils";

import { AlterClusterModal } from "./AlterClusterModal";
import { clusterQueryKeys } from "./queries";

const mockOnClose = vi.fn();

const FormWithClusterProvider = () => (
  <AlterClusterModal
    cluster={buildCluster()}
    clusterSizes={[]}
    isOpen={true}
    maxReplicas={5}
    onClose={mockOnClose}
  />
);

describe("ClusterOverview", () => {
  beforeEach(() => {
    mockOnClose.mockReset();

    server.use(emptyClustersResponse);
  });

  it("Displays validation errors", async () => {
    const user = userEvent.setup();
    renderComponent(<FormWithClusterProvider />);

    const replicasInput = await screen.findByLabelText("Replicas");
    // Validation won't fire unless we focus and then blur
    replicasInput.focus();
    await user.clear(screen.getByLabelText("Name"));
    await user.clear(replicasInput);

    expect(screen.getByText("Cluster name is required.")).toBeVisible();
    expect(await screen.findByText("Replicas is required.")).toBeVisible();
    await user.type(replicasInput, "-1");
    expect(screen.getByText("Replicas cannot be negative.")).toBeVisible();
    await user.clear(replicasInput);
    await user.type(replicasInput, "6");
    expect(await screen.findByText("Replicas cannot exceed 5.")).toBeVisible();
  });

  it("Displays database error message when it fails", async () => {
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: clusterQueryKeys.alter(),
        results: {
          error: {
            message: "unknown cluster 'oops'",
            code: ErrorCode.INTERNAL_ERROR,
          },
          notices: [],
        },
      }),
    );
    const user = userEvent.setup();
    renderComponent(<FormWithClusterProvider />);

    await user.click(
      await screen.findByRole("button", { name: "Alter cluster" }),
    );
    expect(await screen.findByText("Error")).toBeVisible();
    expect(await screen.findByText("unknown cluster 'oops'")).toBeVisible();
  });

  it("Closes the modal on success", async () => {
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: clusterQueryKeys.alter(),
        results: {
          ok: "ALTER CLUSTER",
          notices: [],
        },
      }),
    );
    const user = userEvent.setup();
    renderComponent(<FormWithClusterProvider />);

    await user.click(
      await screen.findByRole("button", { name: "Alter cluster" }),
    );
    expect(mockOnClose).toHaveBeenCalled();
  });
});
