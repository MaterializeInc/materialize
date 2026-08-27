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

const bodyRows = () =>
  screen
    .getAllByRole("row")
    // The header row is a row too, and has no data cells.
    .slice(1);

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

  it("shows no rows when nothing matches", async () => {
    const user = userEvent.setup();
    await renderClustersList(twoClusters());

    await expectRowsMatching(user, "nonesuch", []);
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
