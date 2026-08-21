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

// The flat table is what the page renders with the usage-metrics flag off. The
// global useFlags mock in vitest.setup.ts already returns no flags, so this
// override only documents which side of the branch these tests exercise.
vi.mock("~/hooks/useFlags", () => ({
  useFlags: () => ({ "usage-metrics-in-cluster-list-CNS121": false }),
}));

// Hoisted so tests can seed entries: `vi.mock` factories run before
// module-level `const`s exist.
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

vi.mock("./queries", async () => {
  const actual = await vi.importActual("./queries");
  return { ...actual, useOwners: () => ({ isOwner: () => true }) };
});

const STATUS_UPDATED_AT = "2024-03-05T10:00:00.000Z";

const formatted = (timestamp: string | Date) =>
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

const buildCluster = (overrides: Partial<Cluster> = {}): Cluster => ({
  id: "u1",
  name: "compute",
  size: "50cc",
  disk: true,
  managed: true,
  ownerId: "u1",
  replicas: [buildReplica(), buildReplica({ id: "u11", name: "r2" })],
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
 * Position of each visible column. The flat table summarizes replicas as a count
 * and a size list, and has no caret column because it defines no sub-rows.
 */
const COLUMN = {
  name: 0,
  replicaCount: 1,
  sizes: 2,
  lastStatusChange: 3,
  actions: 4,
} as const;

const rowFor = (rowLabel: string) => {
  const row = screen.getByText(rowLabel).closest("tr");
  if (!row) throw new Error(`no row found containing "${rowLabel}"`);
  return row;
};

const cellsForRow = (rowLabel: string) =>
  within(rowFor(rowLabel))
    .getAllByRole("cell")
    .map((cell) => cell.textContent);

const oomWarnings = () =>
  screen.queryAllByRole("img", { name: "Ran out of memory" });

/**
 * Text of the warning tooltip on `rowLabel`'s row. The label is only reachable
 * on hover, so asserting on it without hovering first passes vacuously.
 */
const oomTooltipText = async (
  user: ReturnType<typeof userEvent.setup>,
  rowLabel: string,
) => {
  await user.hover(
    within(rowFor(rowLabel)).getByRole("img", { name: "Ran out of memory" }),
  );
  return (await screen.findByRole("tooltip")).textContent;
};

describe("ClusterTable", () => {
  it("renders one row per cluster, with no replica rows", async () => {
    await renderClustersList([buildCluster()]);

    expect(screen.getByText("compute")).toBeInTheDocument();
    expect(screen.queryByText("r1")).not.toBeInTheDocument();
    expect(screen.queryByText("r2")).not.toBeInTheDocument();
  });

  it("summarizes a cluster's replicas as a count and a size list", async () => {
    await renderClustersList([
      buildCluster({
        replicas: [
          buildReplica({ size: "50cc" }),
          buildReplica({ id: "u11", name: "r2", size: "100cc" }),
        ],
      }),
    ]);

    const cells = cellsForRow("compute");
    expect(cells[COLUMN.replicaCount]).toBe("2");
    expect(cells[COLUMN.sizes]).toBe("50cc, 100cc");
    expect(cells[COLUMN.lastStatusChange]).toContain(
      formatted(STATUS_UPDATED_AT),
    );
  });

  it("collapses duplicate replica sizes into one entry", async () => {
    await renderClustersList([buildCluster()]);

    // Both default replicas are 50cc, and the column lists distinct sizes.
    expect(cellsForRow("compute")[COLUMN.sizes]).toBe("50cc");
  });

  it("does not make a cluster row expandable", async () => {
    await renderClustersList([buildCluster()]);

    expect(
      screen.queryByRole("button", { name: /Show replicas of/ }),
    ).not.toBeInTheDocument();
  });

  // The offline map is keyed by replica id. A cluster-level warning has to roll
  // its replicas up: looking up the cluster's own id finds nothing, which would
  // drop every warning silently.
  describe("out-of-memory warning", () => {
    it("warns on the cluster when one of its replicas ran out of memory", async () => {
      offlineReplicas.set("u11", {
        shouldSurfaceOom: true,
        lastOfflineAt: new Date(STATUS_UPDATED_AT),
      });
      await renderClustersList([buildCluster()]);

      expect(oomWarnings()).toHaveLength(1);
      expect(
        within(rowFor("compute")).getByRole("img", {
          name: "Ran out of memory",
        }),
      ).toBeInTheDocument();
    });

    it("does not warn when the cluster's own id is the only match", async () => {
      // Keyed by cluster id rather than replica id, which is what the map held
      // before it was re-keyed. Nothing should be found.
      offlineReplicas.set("u1", {
        shouldSurfaceOom: true,
        lastOfflineAt: new Date(STATUS_UPDATED_AT),
      });
      await renderClustersList([buildCluster()]);

      expect(oomWarnings()).toHaveLength(0);
    });

    it("does not warn when the replica's outage was not an OOM", async () => {
      offlineReplicas.set("u10", {
        shouldSurfaceOom: false,
        lastOfflineAt: new Date(STATUS_UPDATED_AT),
      });
      await renderClustersList([buildCluster()]);

      expect(oomWarnings()).toHaveLength(0);
    });

    it("does not warn when no replica has been offline", async () => {
      await renderClustersList([buildCluster()]);

      expect(oomWarnings()).toHaveLength(0);
    });

    it("names the most recent outage when several replicas were killed", async () => {
      const user = userEvent.setup();
      const older = new Date("2024-03-01T08:00:00.000Z");
      const newest = new Date("2024-03-04T12:00:00.000Z");
      offlineReplicas.set("u10", {
        shouldSurfaceOom: true,
        lastOfflineAt: older,
      });
      offlineReplicas.set("u11", {
        shouldSurfaceOom: true,
        lastOfflineAt: newest,
      });
      await renderClustersList([buildCluster()]);

      // One warning for the cluster, not one per killed replica.
      expect(oomWarnings()).toHaveLength(1);
      expect(await oomTooltipText(user, "compute")).toBe(
        `A replica ran out of memory on ${formatted(newest)}`,
      );
    });

    it("ignores a killed replica that is not worth surfacing", async () => {
      const user = userEvent.setup();
      // A stale outage sits in the map with shouldSurfaceOom false. The roll-up
      // must skip it rather than let it win on recency.
      const surfaced = new Date("2024-03-01T08:00:00.000Z");
      offlineReplicas.set("u10", {
        shouldSurfaceOom: true,
        lastOfflineAt: surfaced,
      });
      offlineReplicas.set("u11", {
        shouldSurfaceOom: false,
        lastOfflineAt: new Date("2024-03-09T08:00:00.000Z"),
      });
      await renderClustersList([buildCluster()]);

      expect(oomWarnings()).toHaveLength(1);
      expect(await oomTooltipText(user, "compute")).toBe(
        `A replica ran out of memory on ${formatted(surfaced)}`,
      );
    });

    it("warns on the right cluster when several are listed", async () => {
      offlineReplicas.set("u21", {
        shouldSurfaceOom: true,
        lastOfflineAt: new Date(STATUS_UPDATED_AT),
      });
      await renderClustersList([
        buildCluster({ id: "u1", name: "quiet", replicas: [buildReplica()] }),
        buildCluster({
          id: "u2",
          name: "killed",
          replicas: [buildReplica({ id: "u21", name: "k1" })],
        }),
      ]);

      expect(oomWarnings()).toHaveLength(1);
      expect(
        within(rowFor("killed")).getByRole("img", {
          name: "Ran out of memory",
        }),
      ).toBeInTheDocument();
    });
  });
});
