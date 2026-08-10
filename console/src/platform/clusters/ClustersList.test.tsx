// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";

import { Cluster, Replica } from "~/api/materialize/cluster/clusterList";
import { getStore } from "~/jotai";
import { allClusters } from "~/store/allClusters";
import { mockSubscribeState } from "~/test/mockSubscribe";
import { renderComponent } from "~/test/utils";
import {
  formatDate,
  FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
} from "~/utils/dateFormat";

import ClustersListPage from "./ClustersList";

// Replica sub-rows are gated on this flag. The global useFlags mock in
// vitest.setup.ts returns no flags at all, so without this override getSubRows
// never produces a replica row and none of these tests reach the code.
vi.mock("~/hooks/useFlags", () => ({
  useFlags: () => ({ "usage-metrics-in-cluster-list-CNS121": true }),
}));

// The list opens a websocket subscribe to surface out-of-memory warnings.
// Stubbing it keeps these tests off the socket and, because it reports no
// error, leaves the `lastStatusChange` column visible.
vi.mock("~/api/materialize/cluster/useLatestOfflineReplica", async () => {
  const actual = await vi.importActual(
    "~/api/materialize/cluster/useLatestOfflineReplica",
  );
  return {
    ...actual,
    default: vi.fn(() => ({ data: new Map(), error: undefined })),
  };
});

const STATUS_UPDATED_AT = "2024-03-05T10:00:00.000Z";

/** The formatter the table itself uses, so assertions survive a timezone change. */
const formatted = (timestamp: string) =>
  formatDate(timestamp, FRIENDLY_DATETIME_FORMAT_NO_SECONDS);

const buildReplica = (overrides: Partial<Replica> = {}): Replica => ({
  id: "u10",
  name: "r1",
  size: "50cc",
  disk: true,
  statuses: [
    {
      replica_id: "u10",
      process_id: "0",
      reason: null,
      status: "online",
      updated_at: STATUS_UPDATED_AT,
    },
  ],
  ...overrides,
});

/**
 * A filler second replica. Only clusters with more than one replica expand, so
 * tests that need to see replica rows must supply at least two.
 */
const SECOND_REPLICA = buildReplica({
  id: "u11",
  name: "r2",
  size: "100cc",
});

// User cluster ids ("u" prefix) matter: system clusters are filtered out of the
// list unless the "show system objects" toggle is on.
const buildCluster = (overrides: Partial<Cluster> = {}): Cluster => ({
  id: "u1",
  name: "compute",
  size: "50cc",
  disk: true,
  managed: true,
  ownerId: "u1",
  replicas: [buildReplica(), SECOND_REPLICA],
  latestStatusUpdate: STATUS_UPDATED_AT,
  ...overrides,
});

const renderClustersList = async (clusters: Cluster[]) => {
  getStore().set(allClusters, mockSubscribeState({ data: clusters }));
  return renderComponent(<ClustersListPage />);
};

/**
 * Expands a cluster's sub-rows via the keyboard. The name cell is a link to the
 * cluster detail page, so clicking it would navigate as well as toggle.
 */
const expandCluster = async (
  user: ReturnType<typeof userEvent.setup>,
  clusterName: string,
) => {
  const row = screen.getByText(clusterName).closest("tr");
  if (!row) throw new Error(`no row found for cluster "${clusterName}"`);
  row.focus();
  await user.keyboard("{Enter}");
};

/** Text of every visible cell in the row containing `rowLabel`. */
const cellsForRow = (rowLabel: string) => {
  const row = screen.getByText(rowLabel).closest("tr");
  if (!row) throw new Error(`no row found containing "${rowLabel}"`);
  return within(row)
    .getAllByRole("cell")
    .map((cell) => cell.textContent);
};

describe("ClustersList replica rows", () => {
  it("hides replica rows until the cluster is expanded", async () => {
    await renderClustersList([buildCluster()]);

    expect(screen.getByText("compute")).toBeInTheDocument();
    expect(screen.queryByText("r1")).not.toBeInTheDocument();
  });

  it("renders a replica's name, size and last status change once expanded", async () => {
    const user = userEvent.setup();
    await renderClustersList([buildCluster()]);

    await expandCluster(user, "compute");

    // Columns are name, replica count, size, last status change, actions. The
    // count and actions columns only apply to clusters, hence the dashes.
    expect(cellsForRow("r1")).toEqual([
      "r1",
      "-",
      "50cc",
      formatted(STATUS_UPDATED_AT),
      "",
    ]);
  });

  it("renders every replica of a cluster", async () => {
    const user = userEvent.setup();
    await renderClustersList([buildCluster()]);

    await expandCluster(user, "compute");

    expect(cellsForRow("r1")[2]).toBe("50cc");
    expect(cellsForRow("r2")[2]).toBe("100cc");
  });

  it("renders the most recent status when a replica has several processes", async () => {
    const user = userEvent.setup();
    const newest = "2024-03-07T09:00:00.000Z";
    await renderClustersList([
      buildCluster({
        replicas: [
          buildReplica({
            // Deliberately not in chronological order: the query does not sort
            // these, so position must not decide which one is shown.
            statuses: [
              {
                replica_id: "u10",
                process_id: "0",
                reason: null,
                status: "online",
                updated_at: "2024-03-01T08:00:00.000Z",
              },
              {
                replica_id: "u10",
                process_id: "1",
                reason: null,
                status: "online",
                updated_at: newest,
              },
              {
                replica_id: "u10",
                process_id: "2",
                reason: null,
                status: "online",
                updated_at: "2024-03-04T12:00:00.000Z",
              },
            ],
          }),
          SECOND_REPLICA,
        ],
      }),
    ]);

    await expandCluster(user, "compute");

    expect(cellsForRow("r1")[3]).toBe(formatted(newest));
  });

  it("renders a dash when the replica has no size", async () => {
    const user = userEvent.setup();
    await renderClustersList([
      buildCluster({
        replicas: [buildReplica({ size: null }), SECOND_REPLICA],
      }),
    ]);

    await expandCluster(user, "compute");

    expect(cellsForRow("r1")[2]).toBe("-");
  });

  it("renders a dash when the replica has no statuses", async () => {
    const user = userEvent.setup();
    await renderClustersList([
      buildCluster({
        replicas: [buildReplica({ statuses: [] }), SECOND_REPLICA],
      }),
    ]);

    await expandCluster(user, "compute");

    expect(cellsForRow("r1")[3]).toBe("-");
  });

  it("does not make a cluster without replicas expandable", async () => {
    await renderClustersList([buildCluster({ replicas: [] })]);

    const row = screen.getByText("compute").closest("tr");
    expect(row).not.toHaveAttribute("aria-expanded");
    expect(cellsForRow("compute")[1]).toBe("0");
  });

  it("makes a cluster with a single replica expandable", async () => {
    const user = userEvent.setup();
    await renderClustersList([buildCluster({ replicas: [buildReplica()] })]);

    const row = screen.getByText("compute").closest("tr");
    expect(row).toHaveAttribute("aria-expanded", "false");
    expect(cellsForRow("compute")[1]).toBe("1");

    await expandCluster(user, "compute");
    expect(cellsForRow("r1")[2]).toBe("50cc");
  });

  it("leaves cluster rows showing their own aggregates", async () => {
    await renderClustersList([buildCluster()]);

    const cells = cellsForRow("compute");
    expect(cells[1]).toBe("2");
    expect(cells[2]).toBe("50cc, 100cc");
    expect(cells[3]).toBe(formatted(STATUS_UPDATED_AT));
  });
});
