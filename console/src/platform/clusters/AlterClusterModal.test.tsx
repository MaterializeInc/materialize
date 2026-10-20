// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { hashKey } from "@tanstack/react-query";
import { fireEvent, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { delay, http, HttpResponse } from "msw";
import React from "react";
import { onTestFinished } from "vitest";

import { Cluster } from "~/api/materialize/cluster/clusterList";
import {
  ErrorCode,
  ExtendedRequest,
  MzDataType,
} from "~/api/materialize/types";
import {
  buildColumn,
  buildSqlQueryHandlerV2,
  mapKyselyToTabular,
} from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { getStore } from "~/jotai";
import { getQueryClient } from "~/queryClient";
import { emptyClustersResponse } from "~/test/clusterQueryBuilders";
import {
  buildCluster,
  healthyEnvironment,
  renderComponent,
  setFakeEnvironment,
} from "~/test/utils";
import { parseDbVersion } from "~/version/api";

import { AlterClusterModal, AlterClusterModalProps } from "./AlterClusterModal";
import { clusterQueryKeys } from "./queries";

const mockOnClose = vi.fn();

const PROD = buildCluster({
  id: "u9",
  name: "prod",
  size: "400cc",
  managed: true,
});

const POLICY_ROW = {
  strategy: {
    on_hydration: {
      hydration_size: "800cc",
      linger_duration: { secs: 120, nanos: 0 },
    },
  },
  state: null,
};

const SIZES = ["100cc", "400cc", "800cc", "1600cc"];
const RESET_PROD = `ALTER CLUSTER "prod" RESET (AUTO SCALING STRATEGY)`;

const environmentAt = (version: string) => ({
  ...healthyEnvironment,
  status: {
    ...healthyEnvironment.status,
    version: parseDbVersion(`v${version} (0000000000)`),
  },
});

// Query keys embed the environment version, so set the environment before
// building the handlers that match on them. The policy read answers from
// `rows`, which each alter's statements update as the server would.
const serveAutoScaling = async ({
  cluster = PROD,
  version = "26.45.0",
  rows: initialRows = [],
  resizeTargetSize,
  readFails = false,
  readDelayMs = 0,
}: {
  cluster?: Cluster;
  version?: string;
  /** The cluster's `mz_cluster_auto_scaling_strategies` rows. */
  rows?: Record<string, unknown>[];
  /** The target size of the cluster's in-progress resize. */
  resizeTargetSize?: string;
  readFails?: boolean;
  readDelayMs?: number;
} = {}) => {
  const environment = environmentAt(version);
  await setFakeEnvironment(getStore().set, "aws/us-east-1", environment);
  const autoScalingKey = hashKey(
    clusterQueryKeys.autoScaling({ clusterId: cluster.id }),
  );
  let autoScalingReads = 0;
  const onRequest = ({ request }: { request: Request }) => {
    if (new URL(request.url).searchParams.get("query_key") === autoScalingKey) {
      autoScalingReads++;
    }
  };
  server.events.on("request:start", onRequest);
  onTestFinished(() => {
    server.events.removeListener("request:start", onRequest);
  });
  let rows = initialRows;
  // The statements of each alter request, in order.
  const sent: string[][] = [];
  const alterKey = hashKey(clusterQueryKeys.alter());
  server.use(
    http.post("*/api/sql", async ({ request }) => {
      const key = new URL(request.url).searchParams.get("query_key");
      if (key === autoScalingKey) {
        await delay(readDelayMs);
        if (readFails) {
          return HttpResponse.json({
            results: [
              {
                error: {
                  message: "Something went wrong",
                  code: ErrorCode.INTERNAL_ERROR,
                },
                notices: [],
              },
            ],
          });
        }
        return HttpResponse.json({
          results: [
            mapKyselyToTabular({
              rows,
              columns: [
                buildColumn({ name: "strategy", type_oid: MzDataType.jsonb }),
                buildColumn({ name: "state", type_oid: MzDataType.jsonb }),
              ],
            }),
            mapKyselyToTabular({
              rows: resizeTargetSize ? [{ size: resizeTargetSize }] : [],
              columns: [buildColumn({ name: "size" })],
            }),
          ],
        });
      }
      if (key !== alterKey) return undefined;
      const { queries }: ExtendedRequest = await request.clone().json();
      sent.push(queries.map(({ query }) => query.trim()));
      for (const { query } of queries) {
        if (
          query.includes("RESET (AUTO SCALING STRATEGY)") ||
          query.includes("AUTO SCALING STRATEGY = ()")
        ) {
          rows = [];
        }
        const burst = /HYDRATION SIZE = '([^']*)'/.exec(query)?.[1];
        if (burst) {
          rows = [
            {
              strategy: { on_hydration: { hydration_size: burst } },
              state: null,
            },
          ];
        }
      }
      return HttpResponse.json({
        results: queries.map(() => ({ ok: "ALTER CLUSTER", notices: [] })),
      });
    }),
  );
  return {
    sent,
    autoScalingReads: () => autoScalingReads,
    /** Changes the policy rows as an alter made elsewhere would. */
    setRows: (next: Record<string, unknown>[]) => {
      rows = next;
    },
    render: (element: React.ReactElement) =>
      renderComponent(element, {
        initializeState: ({ set }) =>
          setFakeEnvironment(set, "aws/us-east-1", environment),
      }),
  };
};

const renderWithAutoScaling = async ({
  cluster = PROD,
  version,
  rows,
  resizeTargetSize,
  readFails,
  ...props
}: Parameters<typeof serveAutoScaling>[0] &
  Partial<AlterClusterModalProps> = {}) => {
  const { render, ...served } = await serveAutoScaling({
    cluster,
    version,
    rows,
    resizeTargetSize,
    readFails,
  });
  await render(
    <AlterClusterModal
      cluster={cluster}
      clusterSizes={SIZES}
      isOpen
      maxReplicas={5}
      onClose={mockOnClose}
      {...props}
    />,
  );
  return served;
};

// Mounts the modal only while open, as `AlterClusterMenuItem` does.
const OpenOnClick = ({ cluster }: { cluster: Cluster }) => {
  const [isOpen, setIsOpen] = React.useState(false);
  return (
    <>
      <button onClick={() => setIsOpen(true)}>Open</button>
      {isOpen && (
        <AlterClusterModal
          cluster={cluster}
          clusterSizes={SIZES}
          isOpen
          maxReplicas={5}
          onClose={() => setIsOpen(false)}
        />
      )}
    </>
  );
};

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

    await user.type(await screen.findByLabelText("Name"), "_renamed");
    await user.click(screen.getByRole("button", { name: "Alter cluster" }));
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

    await user.type(await screen.findByLabelText("Name"), "_renamed");
    await user.click(screen.getByRole("button", { name: "Alter cluster" }));
    await waitFor(() => expect(mockOnClose).toHaveBeenCalled());
  });

  it("prefills the auto-scaling section from the cluster's policy", async () => {
    const { autoScalingReads } = await renderWithAutoScaling({
      rows: [POLICY_ROW],
    });

    expect(
      await screen.findByRole("checkbox", { name: "Enable auto-scaling" }),
    ).toBeChecked();
    expect(screen.getByLabelText("Burst size")).toHaveDisplayValue("800cc");
    expect(screen.getByLabelText("Linger duration")).toHaveDisplayValue("2m");
    expect(autoScalingReads()).toBe(1);
  });

  it("leaves the auto-scaling section out if the policy can't be read", async () => {
    const { autoScalingReads } = await renderWithAutoScaling({
      readFails: true,
    });

    expect(await screen.findByLabelText("Replicas")).toBeInTheDocument();
    expect(
      screen.queryByRole("group", { name: "Auto-Scaling Policy" }),
    ).not.toBeInTheDocument();
    expect(autoScalingReads()).toBeGreaterThan(0);
  });

  it("labels the auto-scaling section and describes its checkbox", async () => {
    await renderWithAutoScaling();

    const section = await screen.findByRole("group", {
      name: "Auto-Scaling Policy",
    });
    expect(
      within(section).getByRole("checkbox", { name: "Enable auto-scaling" }),
    ).toHaveAccessibleDescription(/adds a replica of the burst size/);
  });

  it("sets the policy in the SET it previews when enabled", async () => {
    const user = userEvent.setup();
    const { sent } = await renderWithAutoScaling();

    await user.click(
      await screen.findByRole("checkbox", { name: "Enable auto-scaling" }),
    );
    // The burst size starts at the size after the cluster's.
    expect(screen.getByLabelText("Burst size")).toHaveDisplayValue("800cc");
    await user.selectOptions(screen.getByLabelText("Burst size"), "1600cc");
    await user.type(screen.getByLabelText("Linger duration"), "1m 30s");

    // The preview renders as one line in jsdom, so match within it.
    expect(
      await screen.findByText(
        /ON HYDRATION \(HYDRATION SIZE = '1600cc', LINGER DURATION = '90s'\)/,
      ),
    ).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Alter cluster" }));
    await waitFor(() => expect(mockOnClose).toHaveBeenCalled());
    expect(sent).toEqual([
      [
        [
          `ALTER CLUSTER "prod" SET (`,
          "  AUTO SCALING STRATEGY = (",
          "    ON HYDRATION (HYDRATION SIZE = '1600cc', LINGER DURATION = '90s')",
          "  )",
          ")",
        ].join("\n"),
      ],
    ]);
  });

  it("removes a policy that is turned off alone with a RESET", async () => {
    const user = userEvent.setup();
    const { sent } = await renderWithAutoScaling({ rows: [POLICY_ROW] });

    await user.click(
      await screen.findByRole("checkbox", { name: "Enable auto-scaling" }),
    );
    expect(screen.queryByLabelText("Burst size")).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Alter cluster" }));

    await waitFor(() => expect(mockOnClose).toHaveBeenCalled());
    expect(sent).toEqual([[RESET_PROD]]);
  });

  it("removes a policy in the same SET as a resize to its burst size", async () => {
    const user = userEvent.setup();
    const { sent } = await renderWithAutoScaling({ rows: [POLICY_ROW] });

    await user.click(
      await screen.findByRole("checkbox", { name: "Enable auto-scaling" }),
    );
    await user.selectOptions(screen.getByLabelText("Size"), "800cc");
    await user.click(screen.getByRole("button", { name: "Alter cluster" }));

    await waitFor(() => expect(mockOnClose).toHaveBeenCalled());
    expect(sent).toEqual([
      [
        `ALTER CLUSTER "prod" SET (\n  SIZE = '800cc',\n  AUTO SCALING STRATEGY = ()\n)`,
      ],
    ]);
  });

  it("sends nothing and says so when nothing changes", async () => {
    const user = userEvent.setup();
    const { sent } = await renderWithAutoScaling({ rows: [POLICY_ROW] });

    expect(await screen.findByText("No changes to apply.")).toBeVisible();
    await user.click(screen.getByRole("button", { name: "Alter cluster" }));

    await waitFor(() => expect(mockOnClose).toHaveBeenCalled());
    expect(sent).toEqual([]);
  });

  it("leaves the replication factor out while Replicas is empty", async () => {
    const user = userEvent.setup();
    await renderWithAutoScaling({ rows: [POLICY_ROW] });

    await user.clear(await screen.findByLabelText("Replicas"));
    expect(screen.queryByText(/REPLICATION FACTOR/)).not.toBeInTheDocument();
    expect(screen.getByText("No changes to apply.")).toBeVisible();
  });

  it("names only the size or replication factor that changes", async () => {
    const user = userEvent.setup();
    const { sent } = await renderWithAutoScaling({ rows: [POLICY_ROW] });

    await user.selectOptions(await screen.findByLabelText("Size"), "1600cc");
    await user.click(screen.getByRole("button", { name: "Alter cluster" }));
    await waitFor(() => expect(sent).toHaveLength(1));
    await user.selectOptions(screen.getByLabelText("Size"), "400cc");
    await user.clear(screen.getByLabelText("Replicas"));
    await user.type(screen.getByLabelText("Replicas"), "2");
    await user.click(screen.getByRole("button", { name: "Alter cluster" }));

    await waitFor(() => expect(sent).toHaveLength(2));
    expect(sent).toEqual([
      [`ALTER CLUSTER "prod" SET (\n  SIZE = '1600cc'\n)`],
      [`ALTER CLUSTER "prod" SET (\n  REPLICATION FACTOR = 2\n)`],
    ]);
  });

  it("renames alone, as its preview shows", async () => {
    const user = userEvent.setup();
    const { sent } = await renderWithAutoScaling({ rows: [POLICY_ROW] });

    const name = await screen.findByLabelText("Name");
    await user.clear(name);
    await user.type(name, "renamed");

    // The preview renders as one line in jsdom.
    expect(
      screen.getByText(`ALTER CLUSTER "prod" RENAME TO "renamed";`),
    ).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Alter cluster" }));
    await waitFor(() => expect(mockOnClose).toHaveBeenCalled());
    expect(sent).toEqual([[`ALTER CLUSTER "prod" RENAME TO "renamed"`]]);
  });

  it("diffs against the values it opened with when they change under it", async () => {
    const user = userEvent.setup();
    const { render, sent, setRows } = await serveAutoScaling();
    // A resize completes, and a policy set elsewhere is refetched, after the
    // form opened.
    const Harness = () => {
      const [landed, setLanded] = React.useState(false);
      return (
        <>
          <button
            onClick={() => {
              setRows([POLICY_ROW]);
              getQueryClient().invalidateQueries({
                queryKey: clusterQueryKeys.autoScaling({ clusterId: PROD.id }),
              });
              setLanded(true);
            }}
          >
            Land
          </button>
          <AlterClusterModal
            cluster={landed ? { ...PROD, size: "800cc" } : PROD}
            clusterSizes={SIZES}
            isOpen
            maxReplicas={5}
            onClose={mockOnClose}
          />
        </>
      );
    };
    await render(<Harness />);
    expect(
      await screen.findByRole("checkbox", { name: "Enable auto-scaling" }),
    ).not.toBeChecked();

    // The modal hides the rest of the page from pointer and role queries.
    fireEvent.click(screen.getByText("Land"));
    await user.clear(screen.getByLabelText("Replicas"));
    await user.type(screen.getByLabelText("Replicas"), "2");
    await user.click(screen.getByRole("button", { name: "Alter cluster" }));

    await waitFor(() => expect(mockOnClose).toHaveBeenCalled());
    expect(sent).toEqual([
      [`ALTER CLUSTER "prod" SET (\n  REPLICATION FACTOR = 2\n)`],
    ]);
  });

  it("reopens on the policy it just set, and keeps it on the next submit", async () => {
    const user = userEvent.setup();
    const { render, sent } = await serveAutoScaling({ readDelayMs: 100 });
    await render(<OpenOnClick cluster={PROD} />);

    await user.click(screen.getByRole("button", { name: "Open" }));
    await user.click(
      await screen.findByRole("checkbox", { name: "Enable auto-scaling" }),
    );
    await user.click(screen.getByRole("button", { name: "Alter cluster" }));
    await waitFor(() =>
      expect(
        screen.queryByRole("dialog", { name: "Alter cluster" }),
      ).not.toBeInTheDocument(),
    );

    // Reopened while the alter's refetch of the policy would still run.
    await user.click(screen.getByRole("button", { name: "Open" }));
    expect(
      await screen.findByRole("checkbox", { name: "Enable auto-scaling" }),
    ).toBeChecked();
    await user.clear(screen.getByLabelText("Replicas"));
    await user.type(screen.getByLabelText("Replicas"), "2");
    await user.click(screen.getByRole("button", { name: "Alter cluster" }));

    await waitFor(() => expect(sent).toHaveLength(2));
    expect(sent[1]).toEqual([
      `ALTER CLUSTER "prod" SET (\n  REPLICATION FACTOR = 2\n)`,
    ]);
  });

  it("reopens on the policy it just removed, and keeps it off on the next submit", async () => {
    const user = userEvent.setup();
    const { render, sent } = await serveAutoScaling({
      rows: [POLICY_ROW],
      readDelayMs: 100,
    });
    await render(<OpenOnClick cluster={PROD} />);

    await user.click(screen.getByRole("button", { name: "Open" }));
    await user.click(
      await screen.findByRole("checkbox", { name: "Enable auto-scaling" }),
    );
    await user.click(screen.getByRole("button", { name: "Alter cluster" }));
    await waitFor(() =>
      expect(
        screen.queryByRole("dialog", { name: "Alter cluster" }),
      ).not.toBeInTheDocument(),
    );
    expect(sent).toEqual([[RESET_PROD]]);

    await user.click(screen.getByRole("button", { name: "Open" }));
    expect(
      await screen.findByRole("checkbox", { name: "Enable auto-scaling" }),
    ).not.toBeChecked();
    await user.clear(screen.getByLabelText("Replicas"));
    await user.type(screen.getByLabelText("Replicas"), "2");
    await user.click(screen.getByRole("button", { name: "Alter cluster" }));

    await waitFor(() => expect(sent).toHaveLength(2));
    expect(sent[1]).toEqual([
      `ALTER CLUSTER "prod" SET (\n  REPLICATION FACTOR = 2\n)`,
    ]);
  });

  it("rejects an unreadable linger", async () => {
    const user = userEvent.setup();
    const { sent } = await renderWithAutoScaling({ rows: [POLICY_ROW] });

    const linger = await screen.findByLabelText("Linger duration");
    await user.clear(linger);
    await user.type(linger, "2 minutes");
    await user.click(screen.getByRole("button", { name: "Alter cluster" }));

    expect(
      await screen.findByText("Enter a duration like 15s, 2m or 1500ms."),
    ).toBeVisible();
    expect(sent).toEqual([]);
  });

  it("rejects a burst size equal to the cluster's size", async () => {
    const user = userEvent.setup();
    const { sent } = await renderWithAutoScaling({ rows: [POLICY_ROW] });

    await user.selectOptions(
      await screen.findByLabelText("Burst size"),
      "400cc",
    );
    await user.click(screen.getByRole("button", { name: "Alter cluster" }));

    expect(
      await screen.findByText(
        "Burst size must differ from the cluster's size.",
      ),
    ).toBeVisible();
    expect(sent).toEqual([]);
  });

  it("starts the burst past an in-progress resize's target, and rejects the target", async () => {
    const user = userEvent.setup();
    const { sent } = await renderWithAutoScaling({ resizeTargetSize: "800cc" });

    await user.click(
      await screen.findByRole("checkbox", { name: "Enable auto-scaling" }),
    );
    expect(screen.getByLabelText("Burst size")).toHaveDisplayValue("1600cc");
    await user.selectOptions(screen.getByLabelText("Burst size"), "800cc");
    await user.click(screen.getByRole("button", { name: "Alter cluster" }));

    expect(
      await screen.findByText(
        "Burst size must differ from the size the cluster is resizing to.",
      ),
    ).toBeVisible();
    expect(sent).toEqual([]);

    // A new size retargets the resize, so the old target is a valid burst.
    await user.selectOptions(screen.getByLabelText("Size"), "1600cc");
    await user.click(screen.getByRole("button", { name: "Alter cluster" }));
    await waitFor(() => expect(sent).toHaveLength(1));
    expect(sent[0][0]).toMatch(
      /SIZE = '1600cc',\n {2}AUTO SCALING STRATEGY = \(\n {4}ON HYDRATION \(HYDRATION SIZE = '800cc'\)/,
    );
  });

  it("says a resize keeps the cluster serving", async () => {
    const user = userEvent.setup();
    await renderWithAutoScaling();
    await user.selectOptions(await screen.findByLabelText("Size"), "800cc");
    expect(
      screen.getByText(
        "The cluster keeps serving from its current replicas until replicas of the new size catch up.",
      ),
    ).toBeVisible();
    expect(
      screen.queryByText(/temporarily unavailable/),
    ).not.toBeInTheDocument();
  });

  it("warns of downtime when resizing before graceful resizes", async () => {
    const user = userEvent.setup();
    await renderWithAutoScaling({ version: "26.34.0" });
    await user.selectOptions(await screen.findByLabelText("Size"), "800cc");
    expect(
      screen.getByText(
        "Changing the cluster's size will cause the cluster to be temporarily unavailable while the replica of the new size catches up.",
      ),
    ).toBeVisible();
  });

  it("has no auto-scaling section before autoscaling shipped", async () => {
    const { autoScalingReads } = await renderWithAutoScaling({
      version: "26.33.0",
    });

    expect(await screen.findByLabelText("Replicas")).toBeInTheDocument();
    expect(screen.getByText("Equivalent code")).toBeInTheDocument();
    expect(
      screen.queryByRole("checkbox", { name: "Enable auto-scaling" }),
    ).not.toBeInTheDocument();
    expect(autoScalingReads()).toBe(0);
  });

  it("has no auto-scaling section for an unmanaged cluster", async () => {
    const { autoScalingReads } = await renderWithAutoScaling({
      cluster: buildCluster({ id: "u9", name: "prod", managed: false }),
    });

    expect(await screen.findByLabelText("Replicas")).toBeInTheDocument();
    expect(
      screen.queryByRole("checkbox", { name: "Enable auto-scaling" }),
    ).not.toBeInTheDocument();
    expect(autoScalingReads()).toBe(0);
  });
});
