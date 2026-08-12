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
import { renderComponent, RenderWithPathname } from "~/test/utils";
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

// A row's actions menu renders only for clusters the user owns, and `useOwners`
// reports "not the owner" until its query resolves. Claiming ownership up front
// keeps the menu in the DOM without standing up the roles query.
vi.mock("./queries", async () => {
  const actual = await vi.importActual("./queries");
  return { ...actual, useOwners: () => ({ isOwner: () => true }) };
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
  cpuPercent: 12.5,
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

/** A second replica, so the default cluster covers the multi-replica case. */
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
  const rendered = renderComponent(
    <RenderWithPathname>
      <ClustersListPage />
    </RenderWithPathname>,
  );
  await screen.findByRole("table");
  return rendered;
};

/** The caret that expands `clusterName`, or null when the cluster has none. */
const caretFor = (clusterName: string) =>
  screen.queryByRole("button", { name: `Show replicas of ${clusterName}` });

const expandCluster = async (
  user: ReturnType<typeof userEvent.setup>,
  clusterName: string,
) => {
  const caret = caretFor(clusterName);
  if (!caret) throw new Error(`no expand caret found for "${clusterName}"`);
  await user.click(caret);
};

/**
 * Position of each visible column, so assertions name what they read instead of
 * hard-coding an index that shifts whenever a column is added.
 */
const COLUMN = {
  name: 0,
  replicaCount: 1,
  size: 2,
  cpu: 3,
  lastStatusChange: 4,
  actions: 5,
} as const;

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

    // The replica count and actions columns only apply to clusters, hence the
    // dashes.
    expect(cellsForRow("r1")).toEqual([
      "r1",
      "-",
      "50cc",
      "12.5%",
      formatted(STATUS_UPDATED_AT),
      "",
    ]);
  });

  it("renders every replica of a cluster", async () => {
    const user = userEvent.setup();
    await renderClustersList([buildCluster()]);

    await expandCluster(user, "compute");

    expect(cellsForRow("r1")[COLUMN.size]).toBe("50cc");
    expect(cellsForRow("r2")[COLUMN.size]).toBe("100cc");
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

    expect(cellsForRow("r1")[COLUMN.lastStatusChange]).toBe(formatted(newest));
  });

  it("renders zero CPU as a percentage rather than blank", async () => {
    const user = userEvent.setup();
    await renderClustersList([
      buildCluster({
        replicas: [buildReplica({ cpuPercent: 0 }), SECOND_REPLICA],
      }),
    ]);

    await expandCluster(user, "compute");

    // An idle replica genuinely reports 0. Treating that as "no reading" would
    // leave the cell empty and imply the metric is unavailable.
    expect(cellsForRow("r1")[COLUMN.cpu]).toBe("0.0%");
  });

  it("renders a dash when the replica has no CPU sample", async () => {
    const user = userEvent.setup();
    await renderClustersList([
      buildCluster({
        replicas: [buildReplica({ cpuPercent: null }), SECOND_REPLICA],
      }),
    ]);

    await expandCluster(user, "compute");

    expect(cellsForRow("r1")[COLUMN.cpu]).toBe("-");
  });

  it("leaves the CPU cell empty on cluster rows", async () => {
    await renderClustersList([buildCluster()]);

    expect(cellsForRow("compute")[COLUMN.cpu]).toBe("");
  });

  it("renders a dash when the replica has no size", async () => {
    const user = userEvent.setup();
    await renderClustersList([
      buildCluster({
        replicas: [buildReplica({ size: null }), SECOND_REPLICA],
      }),
    ]);

    await expandCluster(user, "compute");

    expect(cellsForRow("r1")[COLUMN.size]).toBe("-");
  });

  it("renders a dash when the replica has no statuses", async () => {
    const user = userEvent.setup();
    await renderClustersList([
      buildCluster({
        replicas: [buildReplica({ statuses: [] }), SECOND_REPLICA],
      }),
    ]);

    await expandCluster(user, "compute");

    expect(cellsForRow("r1")[COLUMN.lastStatusChange]).toBe("-");
  });

  it("does not make a cluster without replicas expandable", async () => {
    await renderClustersList([buildCluster({ replicas: [] })]);

    expect(caretFor("compute")).not.toBeInTheDocument();
    expect(cellsForRow("compute")[1]).toBe("0");
  });

  it("makes a cluster with a single replica expandable", async () => {
    const user = userEvent.setup();
    await renderClustersList([buildCluster({ replicas: [buildReplica()] })]);

    expect(caretFor("compute")).toHaveAttribute("aria-expanded", "false");
    expect(cellsForRow("compute")[1]).toBe("1");

    await expandCluster(user, "compute");
    expect(cellsForRow("r1")[2]).toBe("50cc");
  });

  it("leaves cluster rows showing their own aggregates", async () => {
    await renderClustersList([buildCluster()]);

    const cells = cellsForRow("compute");
    expect(cells[COLUMN.replicaCount]).toBe("2");
    expect(cells[COLUMN.size]).toBe("50cc, 100cc");
    expect(cells[COLUMN.lastStatusChange]).toBe(formatted(STATUS_UPDATED_AT));
  });
});

describe("ClustersList keyboard navigation", () => {
  // A cluster with replicas puts an expand caret ahead of its name, which would
  // shift every tab stop in the row. These tests are about the name and the
  // actions menu, so they leave the caret out.
  const unexpandableCluster = () => buildCluster({ replicas: [] });

  const clusterNameLink = () =>
    screen.getByRole("link", {
      name: "View detailed information about cluster compute",
    });

  it("tabs from the page controls to the cluster name, then its actions", async () => {
    const user = userEvent.setup();
    await renderClustersList([unexpandableCluster()]);

    // The header's system-objects switch and the table's search box precede the
    // rows in document order.
    await user.tab();
    expect(screen.getByLabelText("Show system clusters")).toHaveFocus();

    await user.tab();
    expect(screen.getByLabelText("Search clusters...")).toHaveFocus();

    await user.tab();
    expect(clusterNameLink()).toHaveFocus();

    await user.tab();
    expect(screen.getByRole("button", { name: "More actions" })).toHaveFocus();
  });

  it("opens the cluster detail view on Enter", async () => {
    const user = userEvent.setup();
    await renderClustersList([unexpandableCluster()]);

    clusterNameLink().focus();
    await user.keyboard("{Enter}");

    // `relativeClusterPath`: the cluster's id, then its name.
    expect(screen.getByTestId("pathname")).toHaveTextContent("/u1/compute");
  });

  it("opens the actions menu on Enter", async () => {
    const user = userEvent.setup();
    await renderClustersList([unexpandableCluster()]);

    const actionsButton = screen.getByRole("button", { name: "More actions" });
    actionsButton.focus();
    await user.keyboard("{Enter}");

    expect(actionsButton).toHaveAttribute("aria-expanded", "true");
    expect(
      await screen.findByRole("menuitem", { name: "Alter cluster" }),
    ).toBeVisible();
    expect(
      screen.getByRole("menuitem", { name: "Drop cluster" }),
    ).toBeVisible();
  });
});
