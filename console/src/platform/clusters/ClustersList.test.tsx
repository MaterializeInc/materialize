// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
import { useLocation } from "react-router-dom";

import { Cluster, Replica } from "~/api/materialize/cluster/clusterList";
import { ReplicaUtilization } from "~/api/materialize/cluster/replicaUtilization";
import { getStore } from "~/jotai";
import { allClusters } from "~/store/allClusters";
import { mockSubscribeState } from "~/test/mockSubscribe";
import { renderComponent, RenderWithPathname } from "~/test/utils";
import {
  formatDate,
  FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
} from "~/utils/dateFormat";

import ClustersListPage from "./ClustersList";

// The per-replica table is gated on this flag. The global useFlags mock in
// vitest.setup.ts returns no flags at all, so without this override the list
// renders the one-row-per-cluster table and none of these tests reach the code.
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
// Utilization arrives from its own polled query, so the fixtures register it
// here and the table reads it back through the mocked hook.
const replicaUtilization = new Map<string, ReplicaUtilization>();

vi.mock("./queries", async () => {
  const actual = await vi.importActual("./queries");
  return {
    ...actual,
    useOwners: () => ({ isOwner: () => true }),
    useReplicaUtilization: () => ({ data: replicaUtilization }),
  };
});

const STATUS_UPDATED_AT = "2024-03-05T10:00:00.000Z";

/** The formatter the table itself uses, so assertions survive a timezone change. */
const formatted = (timestamp: string) =>
  formatDate(timestamp, FRIENDLY_DATETIME_FORMAT_NO_SECONDS);

/**
 * Builds a replica and registers its utilization under the replica's id.
 *
 * Utilization is not part of the replica payload, but every call site wants to
 * state a replica and its readings together, so the builder splits them: the
 * replica is returned, the readings go into `replicaUtilization`. Readings are
 * fractions of the allocation, as the view reports them.
 */
const buildReplica = ({
  cpuPercent = 0.125,
  memoryPercent = 0.4,
  diskPercent = 0.25,
  heapPercent = 0.45,
  ...overrides
}: Partial<Replica> & Partial<Omit<ReplicaUtilization, "replicaId">> = {}) => {
  const replica: Replica = {
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
  };
  replicaUtilization.set(replica.id, {
    replicaId: replica.id,
    cpuPercent,
    memoryPercent,
    diskPercent,
    heapPercent,
  });
  return replica;
};

/**
 * A second replica, so the default cluster covers the multi-replica case.
 *
 * A function rather than a constant: building it also registers its
 * utilization, and a module-scope constant would register once and then be
 * overwritten by any test that reuses id `u11`.
 */
const secondReplica = () =>
  buildReplica({
    id: "u11",
    name: "r2",
    size: "100cc",
  });

// User cluster ids ("u" prefix) matter: system clusters are filtered out of the
// list unless the "show system objects" toggle is on.
//
// `replicas` is destructured rather than defaulted in the literal: building a
// replica also registers its utilization, so an eagerly evaluated default would
// re-register the default ids and overwrite whatever the caller just set.
const buildCluster = ({
  replicas,
  ...overrides
}: Partial<Cluster> = {}): Cluster => ({
  id: "u1",
  name: "compute",
  size: "50cc",
  disk: true,
  managed: true,
  ownerId: "u1",
  replicas: replicas ?? [buildReplica(), secondReplica()],
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

/**
 * Position of each visible column, so assertions name what they read instead of
 * hard-coding an index that shifts whenever a column is added.
 */
const COLUMN = {
  cluster: 0,
  replica: 1,
  size: 2,
  cpu: 3,
  memory: 4,
  disk: 5,
  heap: 6,
  lastStatusChange: 7,
  actions: 8,
} as const;

// `queryAllByRole`, not `getAllByRole`: when the search or a filter excludes
// every replica the table is replaced by a message, so there are no rows at all
// rather than a header row on its own.
const bodyRows = () =>
  screen
    .queryAllByRole("row")
    // The header row is a row too, and has no data cells.
    .slice(1);

const NO_MATCHES_MESSAGE = "No replicas match the current search and filters";

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

/** Text of `column` in every body row, in render order. */
const columnOrder = (column: number) =>
  bodyRows().map((row) => within(row).getAllByRole("cell")[column].textContent);

/** Replica-column text of every body row, in render order. */
const rowOrder = () => columnOrder(COLUMN.replica);

/** Applies `sort`, then reads the resulting row order. */
const rowOrderAfter = async (
  sort: (user: ReturnType<typeof userEvent.setup>) => Promise<void>,
  user: ReturnType<typeof userEvent.setup>,
) => {
  await sort(user);
  return rowOrder();
};

/** The toolbar control for the utilization column headed `label`. */
const filterTrigger = (label: string) =>
  screen.getByRole("button", { name: new RegExp(`^${label}`) });

/** Opens a control's panel, returning its Apply button once mounted. */
const openFilter = async (
  user: ReturnType<typeof userEvent.setup>,
  label: string,
) => {
  await user.click(filterTrigger(label));
  return screen.findByRole("button", { name: "Apply" });
};

/** Opens the control for `label`, sets `comparison` and `percent`, applies. */
const applyFilter = async (
  user: ReturnType<typeof userEvent.setup>,
  label: string,
  comparison: ">" | "<",
  percent: string,
) => {
  const apply = await openFilter(user, label);
  await user.selectOptions(
    screen.getByLabelText(`${label} comparison`),
    comparison,
  );
  await user.clear(screen.getByLabelText(`${label} threshold percentage`));
  await user.type(
    screen.getByLabelText(`${label} threshold percentage`),
    percent,
  );
  await user.click(apply);
};

describe("ClustersList replica rows", () => {
  it("renders one row per replica, naming the cluster on each", async () => {
    await renderClustersList([buildCluster()]);

    expect(rowOrder()).toEqual(["r1", "r2"]);
    expect(columnOrder(COLUMN.cluster)).toEqual(["compute", "compute"]);
  });

  it("renders a replica's cluster, name, size, utilization and last status change", async () => {
    await renderClustersList([buildCluster()]);

    // The actions cell holds an icon-only menu button, so it reads as empty.
    expect(cellsForRow("r1")).toEqual([
      "compute",
      "r1",
      "50cc",
      "12.5%",
      "40.0%",
      "25.0%",
      "45.0%",
      formatted(STATUS_UPDATED_AT),
      "",
    ]);
  });

  it("renders every replica of a cluster", async () => {
    await renderClustersList([buildCluster()]);

    expect(cellsForRow("r1")[COLUMN.size]).toBe("50cc");
    expect(cellsForRow("r2")[COLUMN.size]).toBe("100cc");
  });

  it("interleaves nothing: each cluster's replicas carry its name", async () => {
    await renderClustersList([
      buildCluster({
        id: "u1",
        name: "compute",
        replicas: [buildReplica({ id: "u10", name: "alpha" })],
      }),
      buildCluster({
        id: "u2",
        name: "ingest",
        replicas: [buildReplica({ id: "u20", name: "beta" })],
      }),
    ]);

    expect(cellsForRow("alpha")[COLUMN.cluster]).toBe("compute");
    expect(cellsForRow("beta")[COLUMN.cluster]).toBe("ingest");
  });

  it("keeps a cluster with no replicas in the list", async () => {
    await renderClustersList([buildCluster({ replicas: [] })]);

    // Dropping the row would hide the cluster from the clusters list entirely.
    const cells = cellsForRow("compute");
    expect(cells[COLUMN.cluster]).toBe("compute");
    expect(cells[COLUMN.replica]).toBe("-");
    expect(cells[COLUMN.size]).toBe("-");
    expect(cells[COLUMN.cpu]).toBe("-");
    expect(cells[COLUMN.lastStatusChange]).toBe("-");
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
          secondReplica(),
        ],
      }),
    ]);

    expect(cellsForRow("r1")[COLUMN.lastStatusChange]).toBe(formatted(newest));
  });

  // The utilization columns are built by one shared factory, so each rendering
  // rule is asserted against all of them rather than CPU alone.
  describe.each([
    ["CPU", COLUMN.cpu, (value: number | null) => ({ cpuPercent: value })],
    [
      "Memory",
      COLUMN.memory,
      (value: number | null) => ({ memoryPercent: value }),
    ],
    ["Disk", COLUMN.disk, (value: number | null) => ({ diskPercent: value })],
    ["Heap", COLUMN.heap, (value: number | null) => ({ heapPercent: value })],
  ])("the %s column", (_label, column, withValue) => {
    it("renders zero as a percentage rather than blank", async () => {
      await renderClustersList([
        buildCluster({
          replicas: [buildReplica(withValue(0)), secondReplica()],
        }),
      ]);

      // An idle replica genuinely reports 0. Treating that as "no reading"
      // would leave the cell empty and imply the metric is unavailable.
      expect(cellsForRow("r1")[column]).toBe("0.0%");
    });

    it("renders a dash when the replica has no sample", async () => {
      await renderClustersList([
        buildCluster({
          replicas: [buildReplica(withValue(null)), secondReplica()],
        }),
      ]);

      expect(cellsForRow("r1")[column]).toBe("-");
    });
  });

  it("renders a dash when the replica has no size", async () => {
    await renderClustersList([
      buildCluster({
        replicas: [buildReplica({ size: null }), secondReplica()],
      }),
    ]);

    expect(cellsForRow("r1")[COLUMN.size]).toBe("-");
  });

  it("renders a dash when the replica has no statuses", async () => {
    await renderClustersList([
      buildCluster({
        replicas: [buildReplica({ statuses: [] }), secondReplica()],
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

    expect(
      screen.queryAllByRole("img", { name: "Ran out of memory" }),
    ).toHaveLength(1);
    expect(
      within(rowFor("r1")).getByRole("img", { name: "Ran out of memory" }),
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

  it("does not nest rows under an expandable cluster row", async () => {
    await renderClustersList([buildCluster()]);

    // A flat table has no caret column, so nothing can be collapsed away.
    expect(
      screen.queryByRole("button", { name: /^Show replicas of/ }),
    ).not.toBeInTheDocument();
    expect(bodyRows()).toHaveLength(2);
  });
});

const clickHeader = (user: ReturnType<typeof userEvent.setup>, name: RegExp) =>
  user.click(screen.getByRole("columnheader", { name }));

/**
 * Every sorting fixture below names its replicas so that neither the ascending
 * nor the descending result matches the order the rows arrive in.
 *
 * This matters because `orderedClusters` hands the table its clusters sorted by
 * name, and TanStack breaks ties by row index. A fixture whose expected order
 * happens to match arrival order therefore passes even when the column sorts on
 * nothing at all.
 */

describe("ClustersList Cluster sorting", () => {
  const clickClusterHeader = (user: ReturnType<typeof userEvent.setup>) =>
    clickHeader(user, /^Cluster/);

  const twoClusters = () => [
    buildCluster({
      id: "u1",
      name: "alpha",
      replicas: [buildReplica({ id: "u10", name: "a-1" })],
    }),
    buildCluster({
      id: "u2",
      name: "bravo",
      replicas: [buildReplica({ id: "u20", name: "b-1" })],
    }),
  ];

  it("sorts by cluster name, ascending by default", async () => {
    await renderClustersList(twoClusters());

    expect(columnOrder(COLUMN.cluster)).toEqual(["alpha", "bravo"]);
  });

  it("reverses the clusters when the header is clicked", async () => {
    const user = userEvent.setup();
    await renderClustersList(twoClusters());

    await clickClusterHeader(user);

    expect(columnOrder(COLUMN.cluster)).toEqual(["bravo", "alpha"]);
  });

  it("keeps a cluster's replicas together", async () => {
    await renderClustersList([
      buildCluster({
        id: "u1",
        name: "alpha",
        replicas: [
          buildReplica({ id: "u10", name: "a-1" }),
          buildReplica({ id: "u11", name: "a-2" }),
        ],
      }),
      buildCluster({
        id: "u2",
        name: "bravo",
        replicas: [buildReplica({ id: "u20", name: "b-1" })],
      }),
    ]);

    expect(rowOrder()).toEqual(["a-1", "a-2", "b-1"]);
  });
});

describe("ClustersList CPU sorting", () => {
  const clickCpuHeader = (user: ReturnType<typeof userEvent.setup>) =>
    clickHeader(user, /^CPU/);

  // Each utilization column pins its own first sort direction, and CPU's is
  // descending.
  const sortByCpuDescending = clickCpuHeader;

  const sortByCpuAscending = async (
    user: ReturnType<typeof userEvent.setup>,
  ) => {
    await clickCpuHeader(user);
    await clickCpuHeader(user);
  };

  /**
   * Two clusters whose replicas interleave by CPU, so a sort that ranked
   * clusters first and replicas within them could not produce the flat order.
   */
  const interleavedClusters = () => [
    buildCluster({
      id: "u1",
      name: "alpha",
      replicas: [
        buildReplica({ id: "u10", name: "a-0", cpuPercent: 0.0 }),
        buildReplica({ id: "u11", name: "a-50", cpuPercent: 0.5 }),
      ],
    }),
    buildCluster({
      id: "u2",
      name: "bravo",
      replicas: [
        buildReplica({ id: "u20", name: "b-30", cpuPercent: 0.3 }),
        buildReplica({ id: "u21", name: "b-90", cpuPercent: 0.9 }),
      ],
    }),
  ];

  it("orders every replica by CPU, across clusters", async () => {
    const user = userEvent.setup();
    await renderClustersList(interleavedClusters());

    expect(await rowOrderAfter(sortByCpuDescending, user)).toEqual([
      "b-90",
      "a-50",
      "b-30",
      "a-0",
    ]);
  });

  it("reverses the replicas when sorted ascending", async () => {
    const user = userEvent.setup();
    await renderClustersList(interleavedClusters());

    expect(await rowOrderAfter(sortByCpuAscending, user)).toEqual([
      "a-0",
      "b-30",
      "a-50",
      "b-90",
    ]);
  });

  it("compares readings numerically rather than as text", async () => {
    const user = userEvent.setup();
    await renderClustersList([
      buildCluster({
        id: "u1",
        name: "alpha",
        replicas: [
          buildReplica({ id: "u10", name: "a-1", cpuPercent: 0.1248 }),
        ],
      }),
      buildCluster({
        id: "u2",
        name: "bravo",
        replicas: [buildReplica({ id: "u20", name: "b-1", cpuPercent: 0.125 })],
      }),
    ]);

    // Text collation reads these as (12, 48) and (12, 5) and would rank 12.48
    // above 12.5, leaving a-1 first.
    expect(await rowOrderAfter(sortByCpuDescending, user)).toEqual([
      "b-1",
      "a-1",
    ]);
  });

  // Nulls trail the sampled replicas ascending and lead them descending, which
  // is how `nullsLast` behaves for every column in this table.
  it("sorts an unsampled replica after the sampled ones", async () => {
    const user = userEvent.setup();
    await renderClustersList([
      buildCluster({
        id: "u1",
        name: "alpha",
        replicas: [buildReplica({ id: "u10", name: "a-1", cpuPercent: null })],
      }),
      buildCluster({
        id: "u2",
        name: "bravo",
        replicas: [buildReplica({ id: "u20", name: "b-1", cpuPercent: 0.03 })],
      }),
    ]);

    expect(await rowOrderAfter(sortByCpuAscending, user)).toEqual([
      "b-1",
      "a-1",
    ]);
  });

  it("sorts a cluster with no replicas after the sampled ones", async () => {
    const user = userEvent.setup();
    await renderClustersList([
      buildCluster({ id: "u1", name: "alpha-empty", replicas: [] }),
      buildCluster({
        id: "u2",
        name: "bravo-sampled",
        replicas: [buildReplica({ id: "u20", name: "b-1", cpuPercent: 0.03 })],
      }),
    ]);

    expect(await rowOrderAfter(sortByCpuAscending, user)).toEqual(["b-1", "-"]);
  });
});

describe("ClustersList Size sorting", () => {
  const clickSizeHeader = (user: ReturnType<typeof userEvent.setup>) =>
    clickHeader(user, /^Size/);

  const sortBySizeDescending = clickSizeHeader;

  const sortBySizeAscending = async (
    user: ReturnType<typeof userEvent.setup>,
  ) => {
    await clickSizeHeader(user);
    await clickSizeHeader(user);
  };

  const interleavedClusters = () => [
    buildCluster({
      id: "u1",
      name: "alpha",
      replicas: [
        buildReplica({ id: "u10", name: "a-25", size: "25cc" }),
        buildReplica({ id: "u11", name: "a-400", size: "400cc" }),
      ],
    }),
    buildCluster({
      id: "u2",
      name: "bravo",
      replicas: [
        buildReplica({ id: "u20", name: "b-100", size: "100cc" }),
        buildReplica({ id: "u21", name: "b-800", size: "800cc" }),
      ],
    }),
  ];

  it("orders every replica by size, across clusters", async () => {
    const user = userEvent.setup();
    await renderClustersList(interleavedClusters());

    expect(await rowOrderAfter(sortBySizeDescending, user)).toEqual([
      "b-800",
      "a-400",
      "b-100",
      "a-25",
    ]);
  });

  it("compares sizes numerically rather than lexicographically", async () => {
    const user = userEvent.setup();
    await renderClustersList([
      buildCluster({
        id: "u1",
        name: "alpha",
        replicas: [buildReplica({ id: "u10", name: "a-1", size: "100cc" })],
      }),
      buildCluster({
        id: "u2",
        name: "bravo",
        replicas: [buildReplica({ id: "u20", name: "b-1", size: "50cc" })],
      }),
    ]);

    // Character by character "100cc" precedes "50cc", which ascending would put
    // a-1 first.
    expect(await rowOrderAfter(sortBySizeAscending, user)).toEqual([
      "b-1",
      "a-1",
    ]);
  });

  it("sorts a replica with no size after the sized ones", async () => {
    const user = userEvent.setup();
    await renderClustersList([
      buildCluster({
        id: "u1",
        name: "alpha",
        replicas: [buildReplica({ id: "u10", name: "a-1", size: null })],
      }),
      buildCluster({
        id: "u2",
        name: "bravo",
        replicas: [buildReplica({ id: "u20", name: "b-1", size: "50cc" })],
      }),
    ]);

    expect(await rowOrderAfter(sortBySizeAscending, user)).toEqual([
      "b-1",
      "a-1",
    ]);
  });
});

describe("ClustersList Last status change sorting", () => {
  const clickStatusHeader = (user: ReturnType<typeof userEvent.setup>) =>
    clickHeader(user, /^Last status change/);

  const sortByStatusAscending = clickStatusHeader;

  const sortByStatusDescending = async (
    user: ReturnType<typeof userEvent.setup>,
  ) => {
    await clickStatusHeader(user);
    await clickStatusHeader(user);
  };

  /** A replica whose single process last changed status at `updatedAt`. */
  const replicaAt = (id: string, name: string, updatedAt: string) =>
    buildReplica({
      id,
      name,
      statuses: [
        {
          replica_id: id,
          process_id: "0",
          reason: null,
          status: "online",
          updated_at: updatedAt,
        },
      ],
    });

  const interleavedClusters = () => [
    buildCluster({
      id: "u1",
      name: "alpha",
      replicas: [
        replicaAt("u10", "a-01", "2024-03-01T08:00:00.000Z"),
        replicaAt("u11", "a-20", "2024-03-20T08:00:00.000Z"),
      ],
    }),
    buildCluster({
      id: "u2",
      name: "bravo",
      replicas: [
        replicaAt("u20", "b-10", "2024-03-10T08:00:00.000Z"),
        replicaAt("u21", "b-28", "2024-03-28T08:00:00.000Z"),
      ],
    }),
  ];

  it("orders every replica by its last status change, across clusters", async () => {
    const user = userEvent.setup();
    await renderClustersList(interleavedClusters());

    expect(await rowOrderAfter(sortByStatusAscending, user)).toEqual([
      "a-01",
      "b-10",
      "a-20",
      "b-28",
    ]);
  });

  it("reverses the replicas when sorted descending", async () => {
    const user = userEvent.setup();
    await renderClustersList(interleavedClusters());

    expect(await rowOrderAfter(sortByStatusDescending, user)).toEqual([
      "b-28",
      "a-20",
      "b-10",
      "a-01",
    ]);
  });

  it("ranks a replica by its own status, not its cluster's latestStatusUpdate", async () => {
    const user = userEvent.setup();
    await renderClustersList([
      buildCluster({
        id: "u1",
        name: "live",
        latestStatusUpdate: "2000-01-01T00:00:00.000Z",
        replicas: [replicaAt("u10", "l-1", "2024-03-05T08:00:00.000Z")],
      }),
      buildCluster({
        id: "u2",
        name: "stale-history",
        // The status history reaches far past anything its replicas report,
        // which is what a dropped replica leaves behind.
        latestStatusUpdate: "2099-01-01T00:00:00.000Z",
        replicas: [replicaAt("u20", "h-1", "2024-03-01T08:00:00.000Z")],
      }),
    ]);

    // Ranking on latestStatusUpdate would leave l-1 first.
    expect(await rowOrderAfter(sortByStatusAscending, user)).toEqual([
      "h-1",
      "l-1",
    ]);
  });

  it("sorts a replica with no statuses after the rest", async () => {
    const user = userEvent.setup();
    await renderClustersList([
      buildCluster({
        id: "u1",
        name: "alpha-silent",
        replicas: [buildReplica({ id: "u10", name: "a-1", statuses: [] })],
      }),
      buildCluster({
        id: "u2",
        name: "bravo-reporting",
        replicas: [replicaAt("u20", "b-1", "2024-03-05T08:00:00.000Z")],
      }),
    ]);

    expect(await rowOrderAfter(sortByStatusAscending, user)).toEqual([
      "b-1",
      "a-1",
    ]);
  });
});

describe("ClustersList search", () => {
  const twoClusters = () => [
    buildCluster({
      id: "u1",
      name: "compute",
      replicas: [
        buildReplica({ id: "u10", name: "alpha", size: "50cc" }),
        buildReplica({ id: "u11", name: "beta", size: "100cc" }),
      ],
    }),
    buildCluster({
      id: "u2",
      name: "ingest",
      replicas: [buildReplica({ id: "u20", name: "gamma", size: "50cc" })],
    }),
  ];

  /** Types `term`, then waits out the search box's debounce. */
  const expectRowsMatching = async (
    user: ReturnType<typeof userEvent.setup>,
    term: string,
    expected: (string | null)[],
  ) => {
    await user.type(screen.getByLabelText("Search clusters..."), term);
    await waitFor(() => expect(rowOrder()).toEqual(expected));
  };

  it("matches a replica by name", async () => {
    const user = userEvent.setup();
    await renderClustersList(twoClusters());

    await expectRowsMatching(user, "alpha", ["alpha"]);
  });

  it("keeps every replica of a cluster whose name matches", async () => {
    const user = userEvent.setup();
    await renderClustersList(twoClusters());

    await expectRowsMatching(user, "compute", ["alpha", "beta"]);
  });

  it("leaves other clusters out when one cluster matches", async () => {
    const user = userEvent.setup();
    await renderClustersList(twoClusters());

    await expectRowsMatching(user, "ingest", ["gamma"]);
  });

  it("matches replicas on columns other than the name", async () => {
    const user = userEvent.setup();
    await renderClustersList(twoClusters());

    await expectRowsMatching(user, "100cc", ["beta"]);
  });

  it("replaces the table with a message when nothing matches", async () => {
    const user = userEvent.setup();
    await renderClustersList(twoClusters());

    await expectRowsMatching(user, "nonesuch", []);

    expect(screen.getByText(NO_MATCHES_MESSAGE)).toBeInTheDocument();
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
  });
});

describe("ClustersList keyboard navigation", () => {
  const singleReplicaCluster = () =>
    buildCluster({ replicas: [buildReplica()] });

  const clusterNameLink = () =>
    screen.getByRole("link", {
      name: "View detailed information about cluster compute",
    });

  it("tabs from the page controls to the cluster name, then its actions", async () => {
    const user = userEvent.setup();
    await renderClustersList([singleReplicaCluster()]);

    // The header's system-objects switch and the table's toolbar precede the
    // rows in document order.
    await user.tab();
    expect(screen.getByLabelText("Show system clusters")).toHaveFocus();

    await user.tab();
    expect(screen.getByLabelText("Search clusters...")).toHaveFocus();

    // One control per utilization column, in the order the table shows them.
    for (const label of ["CPU", "Memory", "Disk", "Heap"]) {
      await user.tab();
      expect(filterTrigger(label)).toHaveFocus();
    }

    await user.tab();
    expect(clusterNameLink()).toHaveFocus();

    await user.tab();
    expect(screen.getByRole("button", { name: "More actions" })).toHaveFocus();
  });

  it("opens the cluster detail view on Enter", async () => {
    const user = userEvent.setup();
    await renderClustersList([singleReplicaCluster()]);

    clusterNameLink().focus();
    await user.keyboard("{Enter}");

    // `relativeClusterPath`: the cluster's id, then its name.
    expect(screen.getByTestId("pathname")).toHaveTextContent("/u1/compute");
  });

  it("opens the actions menu on Enter", async () => {
    const user = userEvent.setup();
    await renderClustersList([singleReplicaCluster()]);

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

  it("offers the cluster's actions on each of its replica rows", async () => {
    await renderClustersList([buildCluster()]);

    // The menu acts on the cluster, so both replica rows carry one.
    expect(
      screen.getAllByRole("button", { name: "More actions" }),
    ).toHaveLength(2);
  });
});

describe("ClustersList row identity", () => {
  /**
   * A replica appearing or going away shifts the position of every row after
   * it. Rows are keyed by their id and each one owns state that outlives a
   * re-render: the open/closed actions menu, and the Alter and Drop dialogs it
   * opens. Positional ids would leave that state behind on the index while the
   * cluster under it changed, so a dialog opened for one cluster could submit
   * against another.
   *
   * `alpha` holds the replica that goes away, and the row whose state is under
   * test sits between two clusters so a shift lands a different cluster on its
   * index rather than dropping it off the end.
   */
  const threeClusters = (alphaReplicas: Replica[]) => [
    buildCluster({ id: "u1", name: "alpha", replicas: alphaReplicas }),
    buildCluster({
      id: "u2",
      name: "bravo",
      replicas: [buildReplica({ id: "u20", name: "b-1" })],
    }),
    buildCluster({
      id: "u3",
      name: "charlie",
      replicas: [buildReplica({ id: "u30", name: "c-1" })],
    }),
  ];

  const alphaPair = () => [
    buildReplica({ id: "u10", name: "a-1" }),
    buildReplica({ id: "u11", name: "a-2" }),
  ];

  /** Pushes a new subscribe snapshot, as the websocket does. */
  const pushClusters = (clusters: Cluster[]) =>
    getStore().set(allClusters, mockSubscribeState({ data: clusters }));

  /** The row holding the one open actions menu. */
  const rowWithOpenMenu = () => {
    const button = screen.getByRole("button", {
      name: "More actions",
      expanded: true,
    });
    const row = button.closest("tr");
    if (!row) throw new Error("open menu is not inside a row");
    return row;
  };

  it("keeps an open actions menu with the cluster it was opened for", async () => {
    const user = userEvent.setup();
    await renderClustersList(threeClusters(alphaPair()));

    await user.click(within(rowFor("b-1")).getByRole("button"));
    expect(
      within(rowWithOpenMenu()).getAllByRole("cell")[COLUMN.cluster],
    ).toHaveTextContent("bravo");

    // alpha loses a replica, so bravo's row moves up into the index charlie's
    // row now vacates.
    pushClusters(threeClusters([buildReplica({ id: "u10", name: "a-1" })]));
    await waitFor(() => expect(rowOrder()).toEqual(["a-1", "b-1", "c-1"]));

    expect(
      within(rowWithOpenMenu()).getAllByRole("cell")[COLUMN.cluster],
    ).toHaveTextContent("bravo");
  });

  it("keeps an open Drop dialog on the cluster it was opened for", async () => {
    const user = userEvent.setup();
    await renderClustersList(threeClusters(alphaPair()));

    await user.click(within(rowFor("b-1")).getByRole("button"));
    await user.click(
      await screen.findByRole("menuitem", { name: "Drop cluster" }),
    );
    const dialog = await screen.findByRole("dialog");
    expect(within(dialog).getByText(/^Drop bravo$/)).toBeInTheDocument();

    pushClusters(threeClusters([buildReplica({ id: "u10", name: "a-1" })]));
    // An open modal hides the rest of the app from the accessibility tree, so
    // the rows are unreachable by role while it is up. Counting them in the DOM
    // is what confirms the shift landed before the dialog is inspected.
    await waitFor(() =>
      expect(document.querySelectorAll("tbody tr")).toHaveLength(3),
    );

    // The dialog reads its subject from the row it belongs to, so a row that
    // took on another cluster would retitle the dialog under the user and drop
    // a cluster they never picked.
    const shifted = screen.getByRole("dialog");
    expect(within(shifted).getByText(/^Drop bravo$/)).toBeInTheDocument();
    expect(within(shifted).queryByText(/charlie/)).not.toBeInTheDocument();
  });
});

describe("ClustersList CPU filter", () => {
  /**
   * Three replicas spread across two clusters, so a threshold has to cut
   * through both rather than keeping or dropping whole clusters.
   */
  const twoClusters = () => [
    buildCluster({
      id: "u1",
      name: "compute",
      replicas: [
        buildReplica({ id: "u10", name: "idle", cpuPercent: 0.05 }),
        buildReplica({ id: "u11", name: "busy", cpuPercent: 0.9 }),
      ],
    }),
    buildCluster({
      id: "u2",
      name: "ingest",
      replicas: [
        buildReplica({ id: "u20", name: "middling", cpuPercent: 0.5 }),
      ],
    }),
  ];

  const cpuTrigger = () => filterTrigger("CPU");

  const openCpuFilter = (user: ReturnType<typeof userEvent.setup>) =>
    openFilter(user, "CPU");

  const applyCpuFilter = (
    user: ReturnType<typeof userEvent.setup>,
    comparison: ">" | "<",
    percent: string,
  ) => applyFilter(user, "CPU", comparison, percent);

  it("renders a control labelled by its column", async () => {
    await renderClustersList(twoClusters());

    expect(cpuTrigger()).toBeInTheDocument();
  });

  it("keeps only the replicas above the threshold", async () => {
    const user = userEvent.setup();
    await renderClustersList(twoClusters());

    await applyCpuFilter(user, ">", "40");

    expect(rowOrder()).toEqual(["busy", "middling"]);
  });

  it("keeps only the replicas below the threshold", async () => {
    const user = userEvent.setup();
    await renderClustersList(twoClusters());

    await applyCpuFilter(user, "<", "40");

    expect(rowOrder()).toEqual(["idle"]);
  });

  it("compares the reading, not its rounded display value", async () => {
    const user = userEvent.setup();
    await renderClustersList([
      buildCluster({
        replicas: [
          // Renders as "80.0%", but sits below a threshold of 80.
          buildReplica({ id: "u10", name: "just-under", cpuPercent: 0.7996 }),
          buildReplica({ id: "u11", name: "just-over", cpuPercent: 0.8004 }),
        ],
      }),
    ]);

    await applyCpuFilter(user, ">", "80");

    expect(rowOrder()).toEqual(["just-over"]);
  });

  it("drops a replica with no CPU sample", async () => {
    const user = userEvent.setup();
    await renderClustersList([
      buildCluster({
        replicas: [
          buildReplica({ id: "u10", name: "sampled", cpuPercent: 0.9 }),
          buildReplica({ id: "u11", name: "unsampled", cpuPercent: null }),
        ],
      }),
    ]);

    // An unsampled replica sits on neither side of the threshold, so it is out
    // of a filtered list either way.
    await applyCpuFilter(user, "<", "50");

    expect(rowOrder()).toEqual([]);
    expect(screen.getByText(NO_MATCHES_MESSAGE)).toBeInTheDocument();
  });

  it("drops a cluster with no replicas", async () => {
    const user = userEvent.setup();
    await renderClustersList([
      buildCluster({ id: "u1", name: "empty", replicas: [] }),
      buildCluster({
        id: "u2",
        name: "ingest",
        replicas: [buildReplica({ id: "u20", name: "busy", cpuPercent: 0.9 })],
      }),
    ]);

    await applyCpuFilter(user, ">", "50");

    expect(rowOrder()).toEqual(["busy"]);
  });

  it("states the applied condition on the control", async () => {
    const user = userEvent.setup();
    await renderClustersList(twoClusters());

    await applyCpuFilter(user, ">", "40");

    // Readable without reopening the panel.
    expect(
      screen.getByRole("button", { name: /^CPU > 40%/ }),
    ).toBeInTheDocument();
  });

  it("leaves the table alone until Apply is clicked", async () => {
    const user = userEvent.setup();
    await renderClustersList(twoClusters());

    await openCpuFilter(user);
    await user.clear(screen.getByLabelText("CPU threshold percentage"));
    await user.type(screen.getByLabelText("CPU threshold percentage"), "40");

    // A half-typed threshold would otherwise reorder the table on every
    // keystroke.
    expect(rowOrder()).toEqual(["idle", "busy", "middling"]);
  });

  it("keeps the control reachable when the filter empties the table", async () => {
    const user = userEvent.setup();
    await renderClustersList(twoClusters());

    await applyCpuFilter(user, ">", "99");

    // The message replaces the table, not the toolbar: clearing the filter has
    // to stay possible.
    expect(screen.getByText(NO_MATCHES_MESSAGE)).toBeInTheDocument();
    await user.click(cpuTrigger());
    await user.click(await screen.findByRole("button", { name: "Clear" }));

    expect(rowOrder()).toEqual(["idle", "busy", "middling"]);
  });

  it("restores every row when the filter is cleared", async () => {
    const user = userEvent.setup();
    await renderClustersList(twoClusters());

    await applyCpuFilter(user, ">", "40");
    await user.click(cpuTrigger());
    await user.click(await screen.findByRole("button", { name: "Clear" }));

    expect(rowOrder()).toEqual(["idle", "busy", "middling"]);
    expect(cpuTrigger()).toHaveTextContent(/^CPU$/);
  });

  it("reopens showing the filter in force", async () => {
    const user = userEvent.setup();
    await renderClustersList(twoClusters());

    await applyCpuFilter(user, "<", "40");
    await openCpuFilter(user);

    expect(screen.getByLabelText("CPU comparison")).toHaveValue("<");
    expect(screen.getByLabelText("CPU threshold percentage")).toHaveValue("40");
  });

  it("cannot be applied with an empty threshold", async () => {
    const user = userEvent.setup();
    await renderClustersList(twoClusters());

    const apply = await openCpuFilter(user);
    await user.clear(screen.getByLabelText("CPU threshold percentage"));

    expect(apply).toBeDisabled();
  });

  it("narrows the search results rather than replacing them", async () => {
    const user = userEvent.setup();
    await renderClustersList(twoClusters());

    // Searched first, then filtered: closing the panel hands focus back to its
    // trigger on the next frame, which would swallow keystrokes typed into the
    // search box in the same tick.
    await user.type(screen.getByLabelText("Search clusters..."), "compute");
    await waitFor(() => expect(rowOrder()).toEqual(["idle", "busy"]));

    await applyCpuFilter(user, ">", "40");

    // Both constraints hold: only compute's busy replica clears each.
    expect(rowOrder()).toEqual(["busy"]);
  });
});

describe("ClustersList utilization filters", () => {
  /**
   * One control per utilization column, each paired with the reading it filters
   * on. The label is the table heading verbatim, which is what ties a control
   * to its column for the user.
   */
  const CONTROLS = [
    ["CPU", (value: number) => ({ cpuPercent: value })],
    ["Memory", (value: number) => ({ memoryPercent: value })],
    ["Disk", (value: number) => ({ diskPercent: value })],
    ["Heap", (value: number) => ({ heapPercent: value })],
  ] as const;

  it("labels each control with its column heading", async () => {
    await renderClustersList([buildCluster()]);

    for (const [label] of CONTROLS) {
      expect(
        screen.getByRole("columnheader", { name: new RegExp(`^${label}`) }),
      ).toBeInTheDocument();
      expect(filterTrigger(label)).toBeInTheDocument();
    }
  });

  describe.each(CONTROLS)("the %s control", (label, withValue) => {
    /**
     * Two replicas differing only in the reading under test. Every other
     * reading keeps `buildReplica`'s default, so a control wired to the wrong
     * column sees one value on both rows and cannot produce this split.
     */
    const pair = () =>
      buildCluster({
        replicas: [
          buildReplica({ id: "u10", name: "high", ...withValue(0.9) }),
          buildReplica({ id: "u11", name: "low", ...withValue(0.05) }),
        ],
      });

    it("filters on its own column's reading", async () => {
      const user = userEvent.setup();
      await renderClustersList([pair()]);

      await applyFilter(user, label, ">", "50");

      expect(rowOrder()).toEqual(["high"]);
    });

    it("states the applied condition on its own control only", async () => {
      const user = userEvent.setup();
      await renderClustersList([pair()]);

      await applyFilter(user, label, ">", "50");

      expect(filterTrigger(label)).toHaveTextContent(`${label} > 50%`);
      for (const [other] of CONTROLS.filter(([name]) => name !== label)) {
        expect(filterTrigger(other)).toHaveTextContent(
          new RegExp(`^${other}$`),
        );
      }
    });

    it("is cleared without disturbing the other columns", async () => {
      const user = userEvent.setup();
      await renderClustersList([pair()]);

      await applyFilter(user, label, ">", "50");
      await user.click(filterTrigger(label));
      await user.click(await screen.findByRole("button", { name: "Clear" }));

      expect(rowOrder()).toEqual(["high", "low"]);
    });
  });

  it("applies every filter at once", async () => {
    const user = userEvent.setup();
    await renderClustersList([
      buildCluster({
        replicas: [
          buildReplica({
            id: "u10",
            name: "hot-both",
            cpuPercent: 0.9,
            memoryPercent: 0.9,
          }),
          buildReplica({
            id: "u11",
            name: "hot-cpu-only",
            cpuPercent: 0.9,
            memoryPercent: 0.1,
          }),
          buildReplica({
            id: "u12",
            name: "hot-memory-only",
            cpuPercent: 0.1,
            memoryPercent: 0.9,
          }),
        ],
      }),
    ]);

    await applyFilter(user, "CPU", ">", "50");
    await applyFilter(user, "Memory", ">", "50");

    // Filters narrow each other rather than replacing one another.
    expect(rowOrder()).toEqual(["hot-both"]);
  });
});

/**
 * Renders the router's query string, so what the table writes to the URL is
 * assertable. Only the URL tests mount this: the rendered text would otherwise
 * be one more place `getByText` could match a cluster or replica name.
 */
const RenderWithSearch = ({ children }: { children: React.ReactNode }) => {
  const { search } = useLocation();
  return (
    <>
      {children}
      <div data-testid="search">{search}</div>
    </>
  );
};

describe("ClustersList filter URL state", () => {
  const twoClusters = () => [
    buildCluster({
      id: "u1",
      name: "compute",
      replicas: [
        buildReplica({
          id: "u10",
          name: "idle",
          cpuPercent: 0.05,
          memoryPercent: 0.05,
        }),
        buildReplica({
          id: "u11",
          name: "busy",
          cpuPercent: 0.9,
          memoryPercent: 0.9,
        }),
      ],
    }),
    buildCluster({
      id: "u2",
      name: "ingest",
      replicas: [
        buildReplica({
          id: "u20",
          name: "middling",
          cpuPercent: 0.5,
          memoryPercent: 0.5,
        }),
      ],
    }),
  ];

  /** Renders the list at `url`, so a bookmarked query string can be replayed. */
  const renderAt = async (clusters: Cluster[], url = "/") => {
    getStore().set(allClusters, mockSubscribeState({ data: clusters }));
    const rendered = renderComponent(
      <RenderWithSearch>
        <ClustersListPage />
      </RenderWithSearch>,
      { initialRouterEntries: [url] },
    );
    await screen.findByRole("table");
    return rendered;
  };

  const currentSearch = () =>
    new URLSearchParams(screen.getByTestId("search").textContent ?? "");

  it("writes an applied filter to the URL", async () => {
    const user = userEvent.setup();
    await renderAt(twoClusters());

    await applyFilter(user, "CPU", ">", "40");

    await waitFor(() => expect(currentSearch().get("cpu")).toBe("gt.40"));
  });

  it("spells the comparison as a word rather than percent-encoding it", async () => {
    const user = userEvent.setup();
    await renderAt(twoClusters());

    await applyFilter(user, "CPU", "<", "40");

    // A raw ">" or "<" would reach the user's bookmark bar as %3E or %3C.
    await waitFor(() => expect(currentSearch().get("cpu")).toBe("lt.40"));
    expect(screen.getByTestId("search").textContent).not.toContain("%3");
  });

  it("writes one parameter per filtered column", async () => {
    const user = userEvent.setup();
    await renderAt(twoClusters());

    await applyFilter(user, "CPU", ">", "40");
    await applyFilter(user, "Memory", "<", "80");

    await waitFor(() => {
      const params = currentSearch();
      expect(params.get("cpu")).toBe("gt.40");
      expect(params.get("memory")).toBe("lt.80");
    });
  });

  it("drops a cleared filter from the URL", async () => {
    const user = userEvent.setup();
    await renderAt(twoClusters());

    await applyFilter(user, "CPU", ">", "40");
    await waitFor(() => expect(currentSearch().get("cpu")).toBe("gt.40"));

    await user.click(filterTrigger("CPU"));
    await user.click(await screen.findByRole("button", { name: "Clear" }));

    await waitFor(() => expect(currentSearch().has("cpu")).toBe(false));
  });

  it("restores a bookmarked filter, in the rows and on the control", async () => {
    await renderAt(twoClusters(), "/?cpu=gt.40");

    expect(rowOrder()).toEqual(["busy", "middling"]);
    expect(filterTrigger("CPU")).toHaveTextContent("CPU > 40%");
  });

  it("restores a bookmarked filter for every column at once", async () => {
    await renderAt(twoClusters(), "/?cpu=gt.40&memory=lt.80");

    // busy clears CPU > 40 but not Memory < 80; middling clears both.
    expect(rowOrder()).toEqual(["middling"]);
    expect(filterTrigger("CPU")).toHaveTextContent("CPU > 40%");
    expect(filterTrigger("Memory")).toHaveTextContent("Memory < 80%");
  });

  it("opens the panel on a bookmarked filter's own values", async () => {
    const user = userEvent.setup();
    await renderAt(twoClusters(), "/?cpu=lt.40");

    await openFilter(user, "CPU");

    expect(screen.getByLabelText("CPU comparison")).toHaveValue("<");
    expect(screen.getByLabelText("CPU threshold percentage")).toHaveValue("40");
  });

  it("restores a bookmarked search term in the search box", async () => {
    await renderAt(twoClusters(), "/?q=ingest");

    // The box has to show the term it is filtering by, or the table looks
    // broken rather than filtered.
    expect(screen.getByLabelText("Search clusters...")).toHaveValue("ingest");
    expect(rowOrder()).toEqual(["middling"]);
  });

  it("keeps a bookmarked sort", async () => {
    await renderAt(twoClusters(), "/?sort=cpuPercent&dir=desc");

    expect(rowOrder()).toEqual(["busy", "middling", "idle"]);
  });

  describe.each([
    ["an unknown comparison", "/?cpu=ge.40"],
    ["a missing threshold", "/?cpu=gt."],
    ["a non-numeric threshold", "/?cpu=gt.abc"],
    ["a bare number", "/?cpu=40"],
    ["an empty value", "/?cpu="],
  ])("given %s", (_label, url) => {
    it("ignores it and leaves the table unfiltered", async () => {
      await renderAt(twoClusters(), url);

      // A hand-edited or stale link must not strand the user behind a filter
      // the control cannot show or clear.
      expect(rowOrder()).toEqual(["idle", "busy", "middling"]);
      expect(filterTrigger("CPU")).toHaveTextContent(/^CPU$/);
    });
  });

  it("clamps a bookmarked page that is past the last page", async () => {
    await renderAt(twoClusters(), "/?page=2");

    // Three replicas fit on one page, so page 2 does not exist. Slicing from
    // the stored index would render a header with no rows, and TablePagination
    // hides itself at one page, leaving nothing to click back with.
    expect(rowOrder()).toEqual(["idle", "busy", "middling"]);
  });

  /** 21 replicas: two pages at a page size of 20. */
  const twoPagesOfReplicas = (count = 21) => [
    buildCluster({
      id: "u1",
      name: "compute",
      replicas: Array.from({ length: count }, (_, i) =>
        buildReplica({
          id: `u${100 + i}`,
          name: `r-${i}`,
          // Only the last replica clears a 50% threshold.
          cpuPercent: i === 20 ? 0.9 : 0.1,
        }),
      ),
    }),
  ];

  it("clamps the page when the rows shrink underneath it", async () => {
    const user = userEvent.setup();
    await renderAt(twoPagesOfReplicas());

    await user.click(screen.getByRole("button", { name: "Next page" }));
    expect(rowOrder()).toEqual(["r-20"]);

    // A subscribe update, not a filter change, so nothing resets the page:
    // `useUniversalTable` turns off TanStack's automatic reset so a background
    // refresh cannot yank the user back to page 1.
    getStore().set(
      allClusters,
      mockSubscribeState({ data: twoPagesOfReplicas(3) }),
    );

    await waitFor(() => expect(rowOrder()).toEqual(["r-0", "r-1", "r-2"]));
  });

  it("resets the page when a filter shrinks the row count", async () => {
    const user = userEvent.setup();
    // 21 replicas: two pages at a page size of 20.
    await renderAt([
      buildCluster({
        id: "u1",
        name: "compute",
        replicas: Array.from({ length: 21 }, (_, i) =>
          buildReplica({
            id: `u${100 + i}`,
            name: `r-${i}`,
            // Only the last replica clears a 50% threshold.
            cpuPercent: i === 20 ? 0.9 : 0.1,
          }),
        ),
      }),
    ]);

    await user.click(screen.getByRole("button", { name: "Next page" }));
    expect(rowOrder()).toEqual(["r-20"]);

    await applyFilter(user, "CPU", ">", "50");

    expect(rowOrder()).toEqual(["r-20"]);
  });

  it("accepts a fractional threshold", async () => {
    await renderAt(twoClusters(), "/?cpu=gt.7.5");

    expect(filterTrigger("CPU")).toHaveTextContent("CPU > 7.5%");
    expect(rowOrder()).toEqual(["busy", "middling"]);
  });
});
