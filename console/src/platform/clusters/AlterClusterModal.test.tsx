// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { hashKey } from "@tanstack/react-query";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
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

  it("starts the replicas at the replication factor, not a running burst's replica count", async () => {
    const replica = (id: string, size: string) => ({
      id,
      name: id,
      size,
      disk: true,
      statuses: [],
    });
    renderComponent(
      <AlterClusterModal
        cluster={buildCluster({ replicationFactor: 1 }, [
          replica("r1", "25cc"),
          replica("r2", "800cc"),
        ])}
        clusterSizes={[]}
        isOpen={true}
        maxReplicas={5}
        onClose={mockOnClose}
      />,
    );

    expect(await screen.findByLabelText("Replicas")).toHaveDisplayValue("1");
  });

  describe("sends only the settings that changed, since resending the current ones cancels a resize in flight", () => {
    // Answers each alter with success, and records the statements it sent.
    const recordAlterStatements = () => {
      const sent: string[][] = [];
      server.use(
        http.post("*/api/sql", async ({ request }) => {
          const queryKey = new URL(request.url).searchParams.get("query_key");
          if (queryKey !== hashKey(clusterQueryKeys.alter())) return undefined;
          const body = (await request.json()) as {
            queries: Array<{ query: string }>;
          };
          sent.push(body.queries.map(({ query }) => query));
          return HttpResponse.json({
            results: body.queries.map(() => ({
              ok: "ALTER CLUSTER",
              notices: [],
            })),
          });
        }),
      );
      return sent;
    };

    const renderModal = () =>
      renderComponent(
        <AlterClusterModal
          cluster={buildCluster({ name: "prod", size: "100cc" })}
          clusterSizes={["100cc", "800cc"]}
          isOpen={true}
          maxReplicas={5}
          onClose={mockOnClose}
        />,
      );

    it("renames without a SET", async () => {
      const sent = recordAlterStatements();
      const user = userEvent.setup();
      renderModal();

      const name = await screen.findByLabelText("Name");
      await user.clear(name);
      await user.type(name, "prod_v2");
      await user.click(screen.getByRole("button", { name: "Alter cluster" }));

      await waitFor(() => expect(mockOnClose).toHaveBeenCalled());
      expect(sent).toEqual([[expect.stringMatching(/RENAME TO "prod_v2"/)]]);
    });

    it("sets the replication factor without the size", async () => {
      const sent = recordAlterStatements();
      const user = userEvent.setup();
      renderModal();

      const replicas = await screen.findByLabelText("Replicas");
      await user.clear(replicas);
      await user.type(replicas, "2");
      await user.click(screen.getByRole("button", { name: "Alter cluster" }));

      await waitFor(() => expect(mockOnClose).toHaveBeenCalled());
      expect(sent).toEqual([
        [
          expect.stringMatching(/SET \(\s*REPLICATION FACTOR = 2\s*\)/),
          expect.stringMatching(/RENAME TO "prod"/),
        ],
      ]);
    });
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
