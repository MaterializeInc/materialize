// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { hashKey, QueryKey } from "@tanstack/react-query";
import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
import { onTestFinished } from "vitest";

import { Cluster } from "~/api/materialize/cluster/clusterList";
import { ErrorCode, MzDataType } from "~/api/materialize/types";
import {
  buildColumn,
  buildColumns,
  buildSqlQueryHandlerV2,
  mapKyselyToTabular,
} from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { getStore } from "~/jotai";
import { buildClusterServerResponse } from "~/platform/clusters/clustersTestUtils";
import { clusterQueryKeys } from "~/platform/clusters/queries";
import { roleQueryKeys } from "~/platform/roles/queries";
import {
  healthyEnvironment,
  MSW_HANDLER_LOADING_WAIT_TIME,
  renderComponent,
  setFakeEnvironment,
} from "~/test/utils";
import {
  formatDate,
  FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
} from "~/utils/dateFormat";
import { parseDbVersion } from "~/version/api";

import { ClusterConfigurationDrawer } from "./ClusterConfigurationDrawer";
import { ConfigurationSection } from "./clusterConfigurationModel";

const environmentAt = (version: string) => ({
  ...healthyEnvironment,
  status: {
    ...healthyEnvironment.status,
    version: parseDbVersion(`v${version} (0000000000)`),
  },
});

type AutoScalingRow = { strategy: unknown; state: unknown };

const CLUSTER_CREATED_MS = Date.UTC(2026, 6, 23, 22, 26);
const REPLICA_CREATED_MS = Date.UTC(2026, 6, 24, 9, 0);

const replica = (id: string, name = "r1", size = "400cc") => ({
  id,
  name,
  size,
  disk: false,
  statuses: [
    {
      replica_id: id,
      process_id: "0",
      reason: null,
      status: "online",
      updated_at: "2026-07-23T22:26:00.000Z",
    },
  ],
});

const MANAGED_CLUSTER = buildClusterServerResponse(
  { id: "u9", name: "prod", size: "400cc", managed: true, ownerId: "u1" },
  [replica("u3")],
);

const UNMANAGED_CLUSTER = buildClusterServerResponse(
  { id: "u9", name: "prod", size: null, managed: false, ownerId: "u1" },
  [replica("u3")],
);

const SYSTEM_CLUSTER = buildClusterServerResponse(
  { id: "s2", name: "mz_catalog_server", managed: true, ownerId: "u1" },
  [replica("s3")],
);

const configurationHandler = (
  cluster: Cluster,
  autoScaling: AutoScalingRow[] | null,
  fails: boolean,
) => {
  // Cluster and replica ids come from separate counters, so each id here is
  // also an object of the other type, created at another time.
  const creationTimes = mapKyselyToTabular({
    rows: [
      {
        id: cluster.id,
        objectType: "cluster",
        occurredAt: String(CLUSTER_CREATED_MS),
      },
      {
        id: cluster.id,
        objectType: "cluster-replica",
        occurredAt: String(Date.UTC(2025, 0, 1)),
      },
      ...cluster.replicas.flatMap(({ id }) => [
        {
          id,
          objectType: "cluster-replica",
          occurredAt: String(REPLICA_CREATED_MS),
        },
        {
          id,
          objectType: "cluster",
          occurredAt: String(Date.UTC(2024, 4, 1)),
        },
      ]),
    ],
    columns: buildColumns([
      "id",
      "objectType",
      { name: "occurredAt", type_oid: MzDataType.timestamptz },
    ]),
  });
  return buildSqlQueryHandlerV2({
    queryKey: clusterQueryKeys.configuration({
      clusterId: cluster.id,
      replicaIds: cluster.replicas.map(({ id }) => id),
      includeAutoScaling: autoScaling !== null,
    }),
    results: fails
      ? {
          error: {
            message: "Something went wrong",
            code: ErrorCode.INTERNAL_ERROR,
          },
          notices: [],
        }
      : autoScaling === null
        ? [creationTimes]
        : [
            creationTimes,
            mapKyselyToTabular({
              rows: autoScaling,
              columns: [
                buildColumn({ name: "strategy", type_oid: MzDataType.jsonb }),
                buildColumn({ name: "state", type_oid: MzDataType.jsonb }),
              ],
            }),
          ],
  });
};

const ownersHandler = (isOwner: boolean) =>
  buildSqlQueryHandlerV2({
    queryKey: roleQueryKeys.owners(),
    results: mapKyselyToTabular({
      columns: buildColumns([
        "id",
        "name",
        { name: "isOwner", type_oid: MzDataType.bool },
      ]),
      rows: [{ id: "u1", name: "owner@example.com", isOwner }],
    }),
  });

const maxReplicasHandler = () =>
  buildSqlQueryHandlerV2({
    queryKey: clusterQueryKeys.maxReplicasPerCluster(),
    results: mapKyselyToTabular({
      columns: buildColumns(["max_replicas_per_cluster"]),
      rows: [{ max_replicas_per_cluster: "5" }],
    }),
  });

const clusterSizesHandler = (isLoading: boolean) =>
  buildSqlQueryHandlerV2(
    {
      queryKey: clusterQueryKeys.availableClusterSizes(),
      results: [
        mapKyselyToTabular({
          rows: [{ allowed_cluster_replica_sizes: "400cc, 800cc" }],
          columns: buildColumns(["allowed_cluster_replica_sizes"]),
        }),
        mapKyselyToTabular({
          rows: [{ size: "400cc" }, { size: "800cc" }],
          columns: buildColumns(["size"]),
        }),
      ],
    },
    isLoading ? { waitTimeMs: MSW_HANDLER_LOADING_WAIT_TIME } : {},
  );

// Counts the SQL requests sent under each query key. Call the returned
// function after `renderDrawer`, since query keys embed the environment
// version.
const countRequests = () => {
  const counts = new Map<string, number>();
  const onRequest = ({ request }: { request: Request }) => {
    const key = new URL(request.url).searchParams.get("query_key") ?? "";
    counts.set(key, (counts.get(key) ?? 0) + 1);
  };
  server.events.on("request:start", onRequest);
  onTestFinished(() => {
    server.events.removeListener("request:start", onRequest);
  });
  return (queryKey: QueryKey) => counts.get(hashKey(queryKey)) ?? 0;
};

// Query keys embed the environment version, so set the environment before
// building the handlers that match on them.
const renderDrawer = async ({
  section,
  cluster = MANAGED_CLUSTER,
  isOwner = true,
  version = "26.45.0",
  autoScaling = [],
  configurationFails = false,
  clusterSizesLoading = false,
}: {
  section: ConfigurationSection;
  cluster?: Cluster;
  isOwner?: boolean;
  version?: string;
  /** Null for an environment without autoscaling. */
  autoScaling?: AutoScalingRow[] | null;
  configurationFails?: boolean;
  clusterSizesLoading?: boolean;
}) => {
  const environment = environmentAt(version);
  await setFakeEnvironment(getStore().set, "aws/us-east-1", environment);
  server.use(
    configurationHandler(cluster, autoScaling, configurationFails),
    ownersHandler(isOwner),
    maxReplicasHandler(),
    clusterSizesHandler(clusterSizesLoading),
  );
  const Harness = () => {
    const [shown, setShown] = React.useState<ConfigurationSection>(section);
    return (
      <ClusterConfigurationDrawer
        isOpen
        onClose={() => {}}
        cluster={cluster}
        section={shown}
        onSectionChange={setShown}
      />
    );
  };
  return renderComponent(<Harness />, {
    initializeState: ({ set }) =>
      setFakeEnvironment(set, "aws/us-east-1", environment),
  });
};

const formatCreated = (ms: number) =>
  formatDate(new Date(ms), FRIENDLY_DATETIME_FORMAT_NO_SECONDS);

describe("ClusterConfigurationDrawer", () => {
  it("shows the cluster's details, with its own creation time and not a replica's", async () => {
    await renderDrawer({
      section: "overview",
      cluster: buildClusterServerResponse(
        {
          id: "u9",
          name: "prod",
          size: "400cc",
          managed: true,
          ownerId: "u1",
          replicationFactor: 2,
        },
        [replica("u3")],
      ),
    });

    const drawer = await screen.findByRole("dialog", {
      name: "Configure prod",
    });
    expect(await within(drawer).findByText("owner@example.com")).toBeVisible();
    expect(
      await within(drawer).findByText(formatCreated(CLUSTER_CREATED_MS)),
    ).toBeVisible();
    expect(within(drawer).getByText("2")).toBeVisible();
    expect(within(drawer).getByText("Managed")).toBeVisible();
    expect(within(drawer).getByRole("button", { name: "Edit" })).toBeVisible();
    expect(
      within(drawer).getByRole("button", { name: "Drop cluster" }),
    ).toBeVisible();
  });

  it("disables Edit while the cluster sizes load", async () => {
    await renderDrawer({ section: "overview", clusterSizesLoading: true });

    expect(await screen.findByRole("button", { name: "Edit" })).toBeDisabled();
  });

  it("opens the alter cluster modal from Edit once the cluster sizes load", async () => {
    const user = userEvent.setup();
    await renderDrawer({ section: "overview" });

    const edit = await screen.findByRole("button", { name: "Edit" });
    await waitFor(() => expect(edit).toBeEnabled());
    await user.click(edit);

    expect(
      await screen.findByRole("dialog", { name: "Alter cluster" }),
    ).toBeVisible();
  });

  it("edits from the replication factor, not a running burst replica's count", async () => {
    const user = userEvent.setup();
    const cluster = buildClusterServerResponse(
      { id: "u9", name: "prod", size: "400cc", managed: true, ownerId: "u1" },
      [replica("u3"), replica("u4", "r2", "800cc")],
    );
    await renderDrawer({ section: "overview", cluster });

    const edit = await screen.findByRole("button", { name: "Edit" });
    await waitFor(() => expect(edit).toBeEnabled());
    await user.click(edit);

    const dialog = await screen.findByRole("dialog", { name: "Alter cluster" });
    expect(within(dialog).getByLabelText(/Replicas/)).toHaveDisplayValue("1");
  });

  it("reads the owners, replica limit and sizes once however often the section changes", async () => {
    const user = userEvent.setup();
    const requests = countRequests();
    await renderDrawer({ section: "overview" });

    expect(await screen.findByText("owner@example.com")).toBeVisible();
    const edit = await screen.findByRole("button", { name: "Edit" });
    await waitFor(() => expect(edit).toBeEnabled());
    for (let trip = 0; trip < 3; trip++) {
      await user.click(screen.getByRole("button", { name: /^Replicas/ }));
      expect(await screen.findByRole("table")).toBeVisible();
      await user.click(screen.getByRole("button", { name: /^Overview/ }));
      expect(await screen.findByRole("button", { name: "Edit" })).toBeEnabled();
    }

    expect(requests(roleQueryKeys.owners())).toBe(1);
    expect(requests(clusterQueryKeys.maxReplicasPerCluster())).toBe(1);
    expect(requests(clusterQueryKeys.availableClusterSizes())).toBe(1);
  });

  it("offers dropping the cluster only to its owner", async () => {
    await renderDrawer({ section: "overview", isOwner: false });

    expect(await screen.findByText("owner@example.com")).toBeVisible();
    expect(
      screen.queryByRole("button", { name: "Drop cluster" }),
    ).not.toBeInTheDocument();
  });

  it("offers neither edit, drop nor statements on a system cluster", async () => {
    const user = userEvent.setup();
    const requests = countRequests();
    await renderDrawer({ section: "overview", cluster: SYSTEM_CLUSTER });

    expect(await screen.findByText("owner@example.com")).toBeVisible();
    expect(requests(clusterQueryKeys.availableClusterSizes())).toBe(0);
    expect(
      screen.queryByRole("button", { name: "Edit" }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: "Drop cluster" }),
    ).not.toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: /Auto-Scaling/ }));
    expect(await screen.findByText(/burst size/)).toBeVisible();
    expect(screen.queryByText("To turn it on, run:")).not.toBeInTheDocument();
  });

  it("switches to the replicas from their card, with each replica's own creation time", async () => {
    const user = userEvent.setup();
    await renderDrawer({ section: "overview" });

    await user.click(
      await screen.findByRole("button", { name: /Replicas 1 online/ }),
    );

    const table = await screen.findByRole("table");
    expect(within(table).getByText("r1")).toBeVisible();
    expect(within(table).getByText("Online")).toBeVisible();
    expect(
      await within(table).findByText(formatCreated(REPLICA_CREATED_MS)),
    ).toBeVisible();
    expect(
      screen.getByText(
        /A managed cluster runs 1 replica of its size.* Change the factor with Edit on the overview\./,
      ),
    ).toBeVisible();
    expect(
      screen.queryByRole("button", { name: "New replica" }),
    ).not.toBeInTheDocument();
  });

  it("counts a managed cluster's replicas from its replication factor, not a running burst replica", async () => {
    const cluster = buildClusterServerResponse(
      { id: "u9", name: "prod", size: "400cc", managed: true, ownerId: "u1" },
      [replica("u3"), replica("u4", "r2", "800cc")],
    );
    await renderDrawer({ section: "replicas", cluster });

    expect(
      await screen.findByText(/A managed cluster runs 1 replica of its size/),
    ).toBeVisible();
    expect(within(screen.getByRole("table")).getByText("r2")).toBeVisible();
  });

  it("describes a managed cluster's replicas without a count when its replication factor is unknown", async () => {
    await renderDrawer({
      section: "replicas",
      cluster: buildClusterServerResponse(
        {
          id: "u9",
          name: "prod",
          size: "400cc",
          managed: true,
          ownerId: "u1",
          replicationFactor: null,
        },
        [replica("u3")],
      ),
    });

    expect(
      await screen.findByText(
        /A managed cluster runs one replica of its size per unit of replication factor\./,
      ),
    ).toBeVisible();
  });

  it("doesn't point a system cluster's replicas at an Edit it lacks", async () => {
    await renderDrawer({ section: "replicas", cluster: SYSTEM_CLUSTER });

    expect(
      await screen.findByText(/A managed cluster runs 1 replica of its size/),
    ).toBeVisible();
    expect(screen.queryByText(/Change the factor/)).not.toBeInTheDocument();
  });

  it("lets the owner of an unmanaged cluster add replicas, and says autoscaling needs a managed one", async () => {
    const user = userEvent.setup();
    await renderDrawer({ section: "replicas", cluster: UNMANAGED_CLUSTER });

    expect(
      await screen.findByRole("button", { name: "New replica" }),
    ).toBeVisible();

    await user.click(screen.getByRole("button", { name: /Auto-Scaling/ }));
    expect(
      await screen.findByText("Auto-scaling needs a managed cluster."),
    ).toBeVisible();
  });

  it("lets the owner of an unmanaged cluster drop a replica from its row", async () => {
    const user = userEvent.setup();
    await renderDrawer({ section: "replicas", cluster: UNMANAGED_CLUSTER });

    const table = await screen.findByRole("table");
    await user.click(
      await within(table).findByRole("button", { name: "More actions" }),
    );
    await user.click(
      await screen.findByRole("menuitem", { name: "Drop cluster replica" }),
    );

    expect(
      await screen.findByRole("dialog", { name: "Drop r1" }),
    ).toBeVisible();
  });

  it("shows a configured auto-scaling policy and how to change or remove it", async () => {
    await renderDrawer({
      section: "auto-scaling",
      autoScaling: [
        {
          strategy: {
            on_hydration: {
              hydration_size: "800cc",
              linger_duration: { secs: 120, nanos: 0 },
            },
          },
          state: null,
        },
      ],
    });

    expect(await screen.findByText("800cc")).toBeVisible();
    expect(screen.getByText("2m")).toBeVisible();
    expect(
      screen.getByText(/HYDRATION SIZE = '800cc', LINGER DURATION = '120s'/),
    ).toBeVisible();
    expect(
      screen.getByText(`ALTER CLUSTER "prod" RESET (AUTO SCALING STRATEGY);`),
    ).toBeVisible();
  });

  it("keeps a zero linger at zero", async () => {
    await renderDrawer({
      section: "auto-scaling",
      autoScaling: [
        {
          strategy: {
            on_hydration: {
              hydration_size: "800cc",
              linger_duration: { secs: 0, nanos: 0 },
            },
          },
          state: null,
        },
      ],
    });

    expect(await screen.findByText("0s")).toBeVisible();
    expect(screen.getByText(/LINGER DURATION = '0s'/)).toBeVisible();
  });

  it("says auto-scaling is off and how to turn it on", async () => {
    await renderDrawer({ section: "auto-scaling" });

    expect(await screen.findByText("To turn it on, run:")).toBeVisible();
    expect(screen.getByText(/HYDRATION SIZE = '<size>'/)).toBeVisible();
  });

  it("notes a running burst replica after its policy is removed", async () => {
    await renderDrawer({
      section: "auto-scaling",
      autoScaling: [
        {
          strategy: null,
          state: {
            burst: {
              burst_size: "800cc",
              linger_duration: { secs: 15, nanos: 0 },
              steady_ready_at: null,
            },
          },
        },
      ],
    });

    expect(
      await screen.findByText(
        "A burst replica of size 800cc is running while objects hydrate.",
      ),
    ).toBeVisible();
    expect(screen.getByText("To turn it on, run:")).toBeVisible();
  });

  it("says when the configuration can't be read, and keeps what the cluster list has", async () => {
    await renderDrawer({ section: "overview", configurationFails: true });

    expect(
      await screen.findByText(
        "Some of this cluster's configuration is unavailable.",
      ),
    ).toBeVisible();
    expect(screen.getByText("Managed")).toBeVisible();
  });

  it("has no auto-scaling card before autoscaling shipped, and links to it open the overview", async () => {
    await renderDrawer({
      section: "auto-scaling",
      version: "26.33.0",
      autoScaling: null,
    });

    expect(
      await screen.findByText(formatCreated(CLUSTER_CREATED_MS)),
    ).toBeVisible();
    expect(screen.getByRole("button", { name: /^Overview/ })).toHaveAttribute(
      "aria-pressed",
      "true",
    );
    expect(
      screen.queryByRole("button", { name: /Auto-Scaling/ }),
    ).not.toBeInTheDocument();
  });
});
