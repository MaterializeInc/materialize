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
// error, leaves the `lastStatusChange` column visible. Hoisted so tests can
// seed entries: `vi.mock` factories run before module-level `const`s exist.
const { offlineReplicas } = vi.hoisted(() => ({
  offlineReplicas: new Map<
    string,
    { shouldSurfaceOom: boolean; lastOfflineAt: Date }
  >(),
}));

vi.mock("~/api/materialize/cluster/useLatestOfflineReplica", async () => {
  const actual = await vi.importActual(
    "~/api/materialize/cluster/useLatestOfflineReplica",
  );
  return {
    ...actual,
    default: vi.fn(() => ({ data: offlineReplicas, error: undefined })),
  };
});

beforeEach(() => {
  offlineReplicas.clear();
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

/** Rows start expanded, so clicking the caret collapses rather than expands. */
const toggleCluster = async (
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
  /** Grouped tables lead with a caret column, empty on replica rows. */
  caret: 0,
  name: 1,
  size: 2,
  cpu: 3,
  lastStatusChange: 4,
  actions: 5,
} as const;

const rowFor = (rowLabel: string) => {
  const row = screen.getByText(rowLabel).closest("tr");
  if (!row) throw new Error(`no row found containing "${rowLabel}"`);
  return row;
};

/** Text of every visible cell in the row containing `rowLabel`. */
const cellsForRow = (rowLabel: string) =>
  within(rowFor(rowLabel))
    .getAllByRole("cell")
    .map((cell) => cell.textContent);

/**
 * Name-column text of every body row, in render order, so cluster rows and the
 * replicas nested under them appear in one flat list.
 */
const rowOrder = () =>
  screen
    .getAllByRole("row")
    // The header row is a row too, and has no data cells.
    .slice(1)
    .map((row) => within(row).getAllByRole("cell")[COLUMN.name].textContent);

/** Applies `sort`, then reads the resulting row order. */
const rowOrderAfter = async (
  sort: (user: ReturnType<typeof userEvent.setup>) => Promise<void>,
  user: ReturnType<typeof userEvent.setup>,
) => {
  await sort(user);
  return rowOrder();
};

describe("ClustersList replica rows", () => {
  it("renders replica rows without requiring a click", async () => {
    await renderClustersList([buildCluster()]);

    expect(screen.getByText("compute")).toBeInTheDocument();
    expect(screen.getByText("r1")).toBeInTheDocument();
    expect(caretFor("compute")).toHaveAttribute("aria-expanded", "true");
  });

  it("collapses a cluster's replicas when its caret is clicked", async () => {
    const user = userEvent.setup();
    await renderClustersList([buildCluster()]);

    await toggleCluster(user, "compute");

    expect(screen.queryByText("r1")).not.toBeInTheDocument();
    expect(caretFor("compute")).toHaveAttribute("aria-expanded", "false");
  });

  it("renders a replica's name, size, CPU and last status change", async () => {
    await renderClustersList([buildCluster()]);

    // The caret column is empty on a replica row, and actions are
    // cluster-scoped, so that cell stays blank too.
    expect(cellsForRow("r1")).toEqual([
      "",
      "r1",
      "50cc",
      "12.5%",
      formatted(STATUS_UPDATED_AT),
      "",
    ]);
  });

  it("renders every replica of a cluster", async () => {
    await renderClustersList([buildCluster()]);

    expect(cellsForRow("r1")[COLUMN.size]).toBe("50cc");
    expect(cellsForRow("r2")[COLUMN.size]).toBe("100cc");
  });

  it("renders the most recent status when a replica has several processes", async () => {
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

    expect(cellsForRow("r1")[COLUMN.lastStatusChange]).toBe(formatted(newest));
  });

  it("renders zero CPU as a percentage rather than blank", async () => {
    await renderClustersList([
      buildCluster({
        replicas: [buildReplica({ cpuPercent: 0 }), SECOND_REPLICA],
      }),
    ]);

    // An idle replica genuinely reports 0. Treating that as "no reading" would
    // leave the cell empty and imply the metric is unavailable.
    expect(cellsForRow("r1")[COLUMN.cpu]).toBe("0.0%");
  });

  it("renders a dash when the replica has no CPU sample", async () => {
    await renderClustersList([
      buildCluster({
        replicas: [buildReplica({ cpuPercent: null }), SECOND_REPLICA],
      }),
    ]);

    expect(cellsForRow("r1")[COLUMN.cpu]).toBe("-");
  });

  it("leaves the CPU cell empty on cluster rows", async () => {
    await renderClustersList([buildCluster()]);

    expect(cellsForRow("compute")[COLUMN.cpu]).toBe("");
  });

  it("renders a dash when the replica has no size", async () => {
    await renderClustersList([
      buildCluster({
        replicas: [buildReplica({ size: null }), SECOND_REPLICA],
      }),
    ]);

    expect(cellsForRow("r1")[COLUMN.size]).toBe("-");
  });

  it("renders a dash when the replica has no statuses", async () => {
    await renderClustersList([
      buildCluster({
        replicas: [buildReplica({ statuses: [] }), SECOND_REPLICA],
      }),
    ]);

    expect(cellsForRow("r1")[COLUMN.lastStatusChange]).toBe("-");
  });

  it("warns on the replica that ran out of memory, not its siblings", async () => {
    // Keyed by replica id: r1 is u10, r2 is u11.
    offlineReplicas.set("u10", {
      shouldSurfaceOom: true,
      lastOfflineAt: new Date(STATUS_UPDATED_AT),
    });
    await renderClustersList([buildCluster()]);

    const warning = () =>
      screen.queryAllByRole("img", { name: "Ran out of memory" });
    expect(warning()).toHaveLength(1);

    const r1Row = screen.getByText("r1").closest("tr");
    if (!r1Row) throw new Error("no row for r1");
    expect(
      within(r1Row).getByRole("img", { name: "Ran out of memory" }),
    ).toBeInTheDocument();
  });

  it("does not warn when the replica's outage was not an OOM", async () => {
    offlineReplicas.set("u10", {
      shouldSurfaceOom: false,
      lastOfflineAt: new Date(STATUS_UPDATED_AT),
    });
    await renderClustersList([buildCluster()]);

    expect(
      screen.queryByRole("img", { name: "Ran out of memory" }),
    ).not.toBeInTheDocument();
  });

  // The heading styling hangs off this class rather than off `[data-group-row]`,
  // which only marks rows that can expand. jsdom does not apply emotion's
  // stylesheet, so these assert which rows carry the class, not how it looks.
  describe("cluster row class", () => {
    it("marks cluster rows and not replica rows", async () => {
      await renderClustersList([buildCluster()]);

      expect(rowFor("compute")).toHaveClass("cluster-row");
      expect(rowFor("r1")).not.toHaveClass("cluster-row");
      expect(rowFor("r2")).not.toHaveClass("cluster-row");
    });

    it("marks a cluster with no replicas, which is not a group row", async () => {
      await renderClustersList([buildCluster({ replicas: [] })]);

      // No caret, so `[data-group-row]` is absent. The class still applies.
      expect(caretFor("compute")).not.toBeInTheDocument();
      expect(rowFor("compute")).not.toHaveAttribute("data-group-row");
      expect(rowFor("compute")).toHaveClass("cluster-row");
    });

    it("keeps Chakra's generated class alongside it", async () => {
      await renderClustersList([buildCluster()]);

      // `&.cluster-row td` compiles to a compound of both classes, so losing
      // either one silently drops the styling.
      const classes = rowFor("compute").className.split(/\s+/);
      expect(classes).toContain("cluster-row");
      expect(classes.some((name) => name.startsWith("css-"))).toBe(true);
    });
  });

  it("does not make a cluster without replicas expandable", async () => {
    await renderClustersList([buildCluster({ replicas: [] })]);

    expect(caretFor("compute")).not.toBeInTheDocument();
  });

  it("shows a single replica's row without a click", async () => {
    await renderClustersList([buildCluster({ replicas: [buildReplica()] })]);

    expect(caretFor("compute")).toHaveAttribute("aria-expanded", "true");
    expect(cellsForRow("r1")[COLUMN.size]).toBe("50cc");
  });

  it("shows nothing but the name on a cluster row", async () => {
    await renderClustersList([buildCluster()]);

    // A cluster row is a heading: its replicas carry the data. Only the name
    // and the actions menu belong to it.
    const cells = cellsForRow("compute");
    expect(cells[COLUMN.name]).toContain("compute");
    expect(cells[COLUMN.size]).toBe("");
    expect(cells[COLUMN.cpu]).toBe("");
    expect(cells[COLUMN.lastStatusChange]).toBe("");
  });
});

const clickHeader = (user: ReturnType<typeof userEvent.setup>, name: RegExp) =>
  user.click(screen.getByRole("columnheader", { name }));
/**
 * Every sorting fixture below names its clusters in alphabetical order and then
 * arranges their replica values so that neither the ascending nor the descending
 * result matches that order.
 *
 * This matters because `orderedClusters` hands the table its clusters sorted by
 * name, and TanStack breaks ties by row index. A cluster accessor that returns a
 * constant therefore reproduces alphabetical order exactly, so a fixture whose
 * expected order happens to be alphabetical passes even when the aggregate is
 * missing entirely. Three clusters are the minimum that can defeat this in both
 * directions.
 */

describe("ClustersList CPU sorting", () => {
  const clickCpuHeader = (user: ReturnType<typeof userEvent.setup>) =>
    clickHeader(user, /^CPU/);

  // Each per-replica column pins its own first sort direction, and CPU's is
  // descending.
  const sortByCpuDescending = clickCpuHeader;

  const sortByCpuAscending = async (
    user: ReturnType<typeof userEvent.setup>,
  ) => {
    await clickCpuHeader(user);
    await clickCpuHeader(user);
  };

  /**
   * Ranked by peak the order is bravo (31), alpha (50), charlie (90). Ranked by
   * floor or by mean it is alpha, bravo, charlie, which is also the alphabetical
   * order, so a min-based, mean-based, or missing aggregate all fail visibly.
   */
  const interleavedClusters = () => [
    buildCluster({
      id: "u1",
      name: "alpha",
      replicas: [
        buildReplica({ id: "u10", name: "a-0", cpuPercent: 0 }),
        buildReplica({ id: "u11", name: "a-50", cpuPercent: 50 }),
      ],
    }),
    buildCluster({
      id: "u2",
      name: "bravo",
      replicas: [
        buildReplica({ id: "u20", name: "b-30", cpuPercent: 30 }),
        buildReplica({ id: "u21", name: "b-31", cpuPercent: 31 }),
      ],
    }),
    buildCluster({
      id: "u3",
      name: "charlie",
      replicas: [
        buildReplica({ id: "u30", name: "c-89", cpuPercent: 89 }),
        buildReplica({ id: "u31", name: "c-90", cpuPercent: 90 }),
      ],
    }),
  ];

  it("orders clusters by their busiest replica", async () => {
    const user = userEvent.setup();
    await renderClustersList(interleavedClusters());

    expect(await rowOrderAfter(sortByCpuDescending, user)).toEqual([
      "charlie",
      "c-90",
      "c-89",
      "alpha",
      "a-50",
      "a-0",
      "bravo",
      "b-31",
      "b-30",
    ]);
  });

  it("reverses clusters and their replicas together when sorted ascending", async () => {
    const user = userEvent.setup();
    await renderClustersList(interleavedClusters());

    expect(await rowOrderAfter(sortByCpuAscending, user)).toEqual([
      "bravo",
      "b-30",
      "b-31",
      "alpha",
      "a-0",
      "a-50",
      "charlie",
      "c-89",
      "c-90",
    ]);
  });

  it("keeps each cluster's replicas contiguous beneath it", async () => {
    const user = userEvent.setup();
    await renderClustersList(interleavedClusters());

    const order = await rowOrderAfter(sortByCpuAscending, user);

    // Sorted flat, the replicas would run 0, 30, 31, 50, 89, 90, splitting
    // alpha's pair around bravo's.
    const alphaAt = order.indexOf("alpha");
    expect(order.slice(alphaAt, alphaAt + 3)).toEqual(["alpha", "a-0", "a-50"]);
  });

  it("compares cluster maxima numerically rather than as text", async () => {
    const user = userEvent.setup();
    await renderClustersList([
      buildCluster({
        id: "u1",
        name: "alpha",
        replicas: [buildReplica({ id: "u10", name: "a-1", cpuPercent: 12.48 })],
      }),
      buildCluster({
        id: "u2",
        name: "bravo",
        replicas: [buildReplica({ id: "u20", name: "b-1", cpuPercent: 12.5 })],
      }),
    ]);

    // Text collation reads these as (12, 48) and (12, 5) and would rank 12.48
    // above 12.5, leaving alpha first.
    expect(await rowOrderAfter(sortByCpuDescending, user)).toEqual([
      "bravo",
      "b-1",
      "alpha",
      "a-1",
    ]);
  });

  // Nulls trail the sampled clusters ascending and lead them descending, which
  // is how `nullsLast` behaves for every column in this table.
  it("sorts a cluster whose replicas report no CPU after the sampled ones", async () => {
    const user = userEvent.setup();
    await renderClustersList([
      buildCluster({
        id: "u1",
        name: "alpha-unsampled",
        replicas: [
          buildReplica({ id: "u10", name: "a-1", cpuPercent: null }),
          buildReplica({ id: "u11", name: "a-2", cpuPercent: null }),
        ],
      }),
      buildCluster({
        id: "u2",
        name: "bravo-sampled",
        replicas: [buildReplica({ id: "u20", name: "b-1", cpuPercent: 3 })],
      }),
    ]);

    expect(await rowOrderAfter(sortByCpuAscending, user)).toEqual([
      "bravo-sampled",
      "b-1",
      "alpha-unsampled",
      "a-1",
      "a-2",
    ]);
  });

  it("sorts a cluster with no replicas at all after the sampled ones", async () => {
    const user = userEvent.setup();
    await renderClustersList([
      buildCluster({ id: "u1", name: "alpha-empty", replicas: [] }),
      buildCluster({
        id: "u2",
        name: "bravo-sampled",
        replicas: [buildReplica({ id: "u20", name: "b-1", cpuPercent: 3 })],
      }),
    ]);

    expect(await rowOrderAfter(sortByCpuAscending, user)).toEqual([
      "bravo-sampled",
      "b-1",
      "alpha-empty",
    ]);
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
