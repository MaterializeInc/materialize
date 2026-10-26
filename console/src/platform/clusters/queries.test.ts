// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { focusManager } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import { millisecondsInMinute } from "date-fns/constants";
import { createStore } from "jotai";
import { delay, http, HttpResponse } from "msw";
import parse from "postgres-interval";

import { OOM_REASON } from "~/api/materialize/cluster/replicaStatusTimeline";
import { DatabaseObject } from "~/api/materialize/objects";
import { ErrorCode, MzDataType } from "~/api/materialize/types";
import {
  buildColumn,
  buildColumns,
  buildSqlQueryHandlerV2,
  mapKyselyToTabular,
} from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { roleQueryKeys } from "~/platform/roles/queries";
import { getQueryClient } from "~/queryClient";
import { allObjects } from "~/store/allObjects";
import {
  buildDatabaseObject,
  createProviderWrapper,
  defaultRegionId,
  healthyEnvironment,
  setFakeEnvironment,
} from "~/test/utils";
import { parseDbVersion } from "~/version/api";

import {
  buildFreshnessData,
  clusterQueryKeys,
  replicaStatusesUpdatedAt,
  useClusterLineageIds,
  useFreshnessObjects,
  useObjectHydrationStretches,
  useOwners,
  useReplicaHeapBytes,
  useReplicaHeapLimits,
  useReplicaOfflineEvents,
  useReplicaStatusHistory,
  useReplicaUtilizationHistory,
  useRollingWindowStartMs,
} from "./queries";

/**
 * A websocket that never opens, for hooks that mount an idle subscribe. The
 * test environment has no websocket server.
 */
class PendingWebSocket {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;
  readonly readyState = PendingWebSocket.CONNECTING;
  addEventListener() {}
  removeEventListener() {}
  send() {}
  close() {}
}

const ownersColumns = buildColumns([
  "id",
  "name",
  { name: "isOwner", type_oid: MzDataType.bool },
]);

const OWNED_ROLE_ID = "u1";
const UNOWNED_ROLE_ID = "u2";
const UNKNOWN_ROLE_ID = "u404";

function buildOwnersHandler(waitTimeMs?: number) {
  return buildSqlQueryHandlerV2(
    {
      queryKey: roleQueryKeys.owners(),
      results: mapKyselyToTabular({
        columns: ownersColumns,
        rows: [
          { id: OWNED_ROLE_ID, name: "my_role", isOwner: true },
          { id: UNOWNED_ROLE_ID, name: "someone_elses_role", isOwner: false },
        ],
      }),
    },
    { waitTimeMs },
  );
}

const errorOwnersHandler = buildSqlQueryHandlerV2({
  queryKey: roleQueryKeys.owners(),
  results: {
    error: {
      message: "Something went wrong",
      code: ErrorCode.INTERNAL_ERROR,
    },
    notices: [],
  },
});

async function renderUseOwners() {
  const ProviderWrapper = await createProviderWrapper();
  return renderHook(() => useOwners(), { wrapper: ProviderWrapper });
}

describe("useOwners", () => {
  it("returns true for a role the user can act as", async () => {
    server.use(buildOwnersHandler());
    const { result } = await renderUseOwners();

    await waitFor(() =>
      expect(result.current.isOwner(OWNED_ROLE_ID)).toBe(true),
    );
  });

  it("returns false for a role the user cannot act as", async () => {
    server.use(buildOwnersHandler());
    const { result } = await renderUseOwners();

    // Settle on a known owner first, so this asserts the resolved answer rather
    // than the in-flight default.
    await waitFor(() =>
      expect(result.current.isOwner(OWNED_ROLE_ID)).toBe(true),
    );
    expect(result.current.isOwner(UNOWNED_ROLE_ID)).toBe(false);
  });

  it("returns false for an owner id missing from the result set", async () => {
    server.use(buildOwnersHandler());
    const { result } = await renderUseOwners();

    await waitFor(() =>
      expect(result.current.isOwner(OWNED_ROLE_ID)).toBe(true),
    );
    expect(result.current.isOwner(UNKNOWN_ROLE_ID)).toBe(false);
  });

  it("returns false until the query resolves", async () => {
    server.use(buildOwnersHandler(50));
    const { result } = await renderUseOwners();

    // An owner must not read as an owner before the query settles, otherwise
    // owner-only controls would appear and then disappear.
    expect(result.current.isOwner(OWNED_ROLE_ID)).toBe(false);

    await waitFor(() =>
      expect(result.current.isOwner(OWNED_ROLE_ID)).toBe(true),
    );
  });

  it("returns false when the query fails", async () => {
    server.use(errorOwnersHandler);
    const { result } = await renderUseOwners();

    // A failed query leaves no ownership data, so isOwner has no flip to wait
    // on. Wait on the query reaching its error state instead.
    await waitFor(() =>
      expect(
        getQueryClient().getQueryState(roleQueryKeys.owners())?.status,
      ).toEqual("error"),
    );
    expect(result.current.isOwner(OWNED_ROLE_ID)).toBe(false);
  });

  it("keeps a stable isOwner reference across re-renders", async () => {
    server.use(buildOwnersHandler());
    const { result, rerender } = await renderUseOwners();

    await waitFor(() =>
      expect(result.current.isOwner(OWNED_ROLE_ID)).toBe(true),
    );
    // Consumers pass isOwner to useMemo dependency arrays, so a new reference on
    // every render would defeat their memoization.
    const firstReference = result.current.isOwner;
    rerender();
    expect(result.current.isOwner).toBe(firstReference);
  });
});

const lineageColumns = buildColumns([
  "clusterId",
  "currentDeploymentClusterId",
  "clusterName",
]);

function buildLineageHandler(
  clusterIdsKey: string,
  rows: {
    clusterId: string;
    currentDeploymentClusterId: string;
    clusterName: string;
  }[],
) {
  return buildSqlQueryHandlerV2({
    queryKey: clusterQueryKeys.deploymentLineage({ clusterIdsKey }),
    results: mapKyselyToTabular({ columns: lineageColumns, rows }),
  });
}

describe("useClusterLineageIds", () => {
  it("resolves a cluster to itself and its past blue-green deployments", async () => {
    server.use(
      buildLineageHandler("u3", [
        { clusterId: "u3", currentDeploymentClusterId: "u3", clusterName: "a" },
        { clusterId: "u2", currentDeploymentClusterId: "u3", clusterName: "a" },
      ]),
    );
    const ProviderWrapper = await createProviderWrapper();
    const { result } = renderHook(() => useClusterLineageIds(["u3"], true), {
      wrapper: ProviderWrapper,
    });

    await waitFor(() => expect(result.current.data).toEqual(["u2", "u3"]));
  });

  it("resolves a system cluster, which has no lineage rows, to itself", async () => {
    server.use(buildLineageHandler("s2", []));
    const ProviderWrapper = await createProviderWrapper();
    const { result } = renderHook(() => useClusterLineageIds(["s2"], true), {
      wrapper: ProviderWrapper,
    });

    await waitFor(() => expect(result.current.data).toEqual(["s2"]));
  });
});

const offlineEventColumns = buildColumns([
  "replicaId",
  "occurredAt",
  "status",
  "reason",
]);

const TIME_PERIOD_MINUTES = 60;

const offlineEvent = (replicaId: string) => ({
  replicaId,
  occurredAt: "2030-01-01 00:00:30+00",
  status: "offline",
  reason: "oom-killed",
});

function buildOfflineEventsHandler(
  replicaIdsKey: string,
  rows: ReturnType<typeof offlineEvent>[],
  waitTimeMs?: number,
) {
  return buildSqlQueryHandlerV2(
    {
      queryKey: clusterQueryKeys.replicaOfflineEvents({
        replicaIdsKey,
        timePeriodMinutes: TIME_PERIOD_MINUTES,
      }),
      results: mapKyselyToTabular({ columns: offlineEventColumns, rows }),
    },
    { waitTimeMs },
  );
}

async function renderUseReplicaOfflineEvents(
  initialSamples: { replicaId: string }[],
) {
  const ProviderWrapper = await createProviderWrapper();
  return renderHook(
    ({ samples }) =>
      useReplicaOfflineEvents(samples, TIME_PERIOD_MINUTES, true),
    { wrapper: ProviderWrapper, initialProps: { samples: initialSamples } },
  );
}

describe("useReplicaOfflineEvents", () => {
  it("fetches events for the sorted set of sampled replicas", async () => {
    // The handler only answers the sorted, deduplicated key.
    server.use(buildOfflineEventsHandler("u1,u2", [offlineEvent("u2")]));
    const { result } = await renderUseReplicaOfflineEvents([
      { replicaId: "u2" },
      { replicaId: "u1" },
      { replicaId: "u2" },
    ]);

    await waitFor(() => expect(result.current).toEqual([offlineEvent("u2")]));
  });

  it("stays idle while there are no samples", async () => {
    const { result } = await renderUseReplicaOfflineEvents([]);

    expect(
      getQueryClient().getQueryState(
        clusterQueryKeys.replicaOfflineEvents({
          replicaIdsKey: "",
          timePeriodMinutes: TIME_PERIOD_MINUTES,
        }),
      )?.fetchStatus,
    ).toEqual("idle");
    expect(result.current).toBeUndefined();
  });

  it("keeps the previous events while a changed replica set loads", async () => {
    server.use(
      buildOfflineEventsHandler("u1", [offlineEvent("u1")]),
      buildOfflineEventsHandler(
        "u1,u2",
        [offlineEvent("u1"), offlineEvent("u2")],
        50,
      ),
    );
    const { result, rerender } = await renderUseReplicaOfflineEvents([
      { replicaId: "u1" },
    ]);
    await waitFor(() => expect(result.current).toEqual([offlineEvent("u1")]));

    rerender({ samples: [{ replicaId: "u1" }, { replicaId: "u2" }] });
    expect(result.current).toEqual([offlineEvent("u1")]);

    await waitFor(() =>
      expect(result.current).toEqual([offlineEvent("u1"), offlineEvent("u2")]),
    );
  });
});

describe("useReplicaHeapLimits", () => {
  let requests: number;

  const answerHeapLimits = (row: {
    heapLimit: string | null;
    memoryBytes: string | null;
  }) => {
    requests = 0;
    server.use(
      http.post("*/api/sql", ({ request }) => {
        const queryKey = JSON.parse(
          new URL(request.url).searchParams.get("query_key") ?? "[]",
        );
        if (queryKey[1]?.scope !== "replicaHeapLimits") return undefined;
        requests += 1;
        return HttpResponse.json({
          results: [
            mapKyselyToTabular({
              rows: [{ replicaId: "u1", processId: "0", ...row }],
              columns: buildColumns([
                "replicaId",
                "processId",
                "heapLimit",
                "memoryBytes",
              ]),
            }),
          ],
        });
      }),
    );
  };

  const pollFiveMinutes = async () => {
    vi.useFakeTimers({
      toFake: [
        "Date",
        "setTimeout",
        "clearTimeout",
        "setInterval",
        "clearInterval",
      ],
    });
    const ProviderWrapper = await createProviderWrapper();
    renderHook(() => useReplicaHeapLimits(["u1"]), {
      wrapper: ProviderWrapper,
    });
    await vi.advanceTimersByTimeAsync(1_000);
    expect(requests).toBe(1);
    await vi.advanceTimersByTimeAsync(5 * millisecondsInMinute);
  };

  afterEach(() => {
    vi.useRealTimers();
  });

  it("stops polling for a replica that reports memory but no heap limit", async () => {
    answerHeapLimits({ heapLimit: null, memoryBytes: "1073741824" });

    await pollFiveMinutes();

    expect(requests).toBe(1);
  });

  it("keeps polling while a replica's metrics fetch fails", async () => {
    answerHeapLimits({ heapLimit: null, memoryBytes: null });

    await pollFiveMinutes();

    expect(requests).toBeGreaterThan(1);
  });
});

describe("useReplicaHeapBytes", () => {
  let requests: number;

  const answerHeapMetrics = (
    rows: Array<{
      replicaId: string;
      processId: string;
      heapBytes: string | null;
    }>,
  ) => {
    requests = 0;
    server.use(
      http.post("*/api/sql", ({ request }) => {
        const queryKey = JSON.parse(
          new URL(request.url).searchParams.get("query_key") ?? "[]",
        );
        if (queryKey[1]?.scope !== "replicaHeapLimits") return undefined;
        requests += 1;
        return HttpResponse.json({
          results: [
            mapKyselyToTabular({
              rows: rows.map((row) => ({
                ...row,
                heapLimit: "1073741824",
                memoryBytes: "1073741824",
              })),
              columns: buildColumns([
                "replicaId",
                "processId",
                "heapLimit",
                "memoryBytes",
                "heapBytes",
              ]),
            }),
          ],
        });
      }),
    );
  };

  afterEach(() => {
    vi.useRealTimers();
  });

  it("sums each replica's processes, leaving out a replica with a process that reports none", async () => {
    answerHeapMetrics([
      { replicaId: "u1", processId: "0", heapBytes: "400" },
      { replicaId: "u1", processId: "1", heapBytes: null },
      { replicaId: "u2", processId: "0", heapBytes: "500" },
      { replicaId: "u2", processId: "1", heapBytes: "600" },
    ]);
    const ProviderWrapper = await createProviderWrapper();
    const { result } = renderHook(() => useReplicaHeapBytes(["u1", "u2"]), {
      wrapper: ProviderWrapper,
    });

    await waitFor(() =>
      expect(result.current.data).toEqual(new Map([["u2", 1100]])),
    );
  });

  it("keeps polling once every heap limit is in", async () => {
    answerHeapMetrics([{ replicaId: "u1", processId: "0", heapBytes: "400" }]);
    vi.useFakeTimers({
      toFake: [
        "Date",
        "setTimeout",
        "clearTimeout",
        "setInterval",
        "clearInterval",
      ],
    });
    const ProviderWrapper = await createProviderWrapper();
    renderHook(
      () => {
        useReplicaHeapLimits(["u1"]);
        useReplicaHeapBytes(["u1"]);
      },
      { wrapper: ProviderWrapper },
    );
    await vi.advanceTimersByTimeAsync(1_000);
    expect(requests).toBe(1);

    await vi.advanceTimersByTimeAsync(5 * millisecondsInMinute);

    expect(requests).toBeGreaterThan(1);
  });
});

describe("replicaStatusesUpdatedAt", () => {
  it("reads the latest change of any replica's process, system replicas included", () => {
    const status = (
      replicaId: string,
      processId: string,
      updatedAt: string,
    ) => ({
      replica_id: replicaId,
      process_id: processId,
      reason: null,
      status: "online",
      updated_at: updatedAt,
    });
    const replica = (id: string, statuses: ReturnType<typeof status>[]) => ({
      id,
      name: id,
      size: "25cc",
      disk: false,
      statuses,
    });

    expect(
      replicaStatusesUpdatedAt([
        replica("s1", [
          status("s1", "0", "2026-10-20T00:00:00.000Z"),
          status("s1", "1", "2026-10-20T00:07:00.000Z"),
        ]),
        replica("s2", [status("s2", "0", "2026-10-20T00:03:00.000Z")]),
      ]),
    ).toEqual(Date.UTC(2026, 9, 20, 0, 7));
    expect(replicaStatusesUpdatedAt([replica("s1", [])])).toBeUndefined();
  });
});

describe("useReplicaStatusHistory", () => {
  const FIRST_UPDATE = Date.UTC(2026, 9, 20, 0, 0);
  const NEXT_UPDATE = Date.UTC(2026, 9, 20, 0, 5);
  const statusRow = (atMs: number, reason: string | null = null) => ({
    replicaId: "u1",
    processId: "0",
    occurredAt: String(atMs),
    status: reason ? "offline" : "online",
    reason,
  });
  let requests: number;

  // Answers each status history request with one more row than the last, after
  // the first, so a test can tell the answers apart.
  beforeEach(() => {
    requests = 0;
    server.use(
      http.post("*/api/sql", async ({ request }) => {
        const queryKey = JSON.parse(
          new URL(request.url).searchParams.get("query_key") ?? "[]",
        );
        if (queryKey[1]?.scope !== "replicaStatusHistory") return undefined;
        requests += 1;
        const rows = [
          statusRow(1_000),
          ...(requests > 1 ? [statusRow(2_000, OOM_REASON)] : []),
        ];
        if (requests > 1) await delay(50);
        return HttpResponse.json({
          results: [
            mapKyselyToTabular({
              rows,
              columns: [
                buildColumn({
                  name: "occurredAt",
                  type_oid: MzDataType.timestamptz,
                }),
              ],
            }),
          ],
        });
      }),
    );
  });

  afterEach(() => {
    focusManager.setFocused(undefined);
    vi.useRealTimers();
  });

  const renderStatusHistory = async () => {
    const ProviderWrapper = await createProviderWrapper();
    const view = renderHook(
      ({
        replicaIds,
        statusesUpdatedAt,
      }: {
        replicaIds: string[];
        statusesUpdatedAt: number;
      }) => useReplicaStatusHistory(replicaIds, statusesUpdatedAt),
      {
        wrapper: ProviderWrapper,
        initialProps: {
          replicaIds: ["u1"],
          statusesUpdatedAt: FIRST_UPDATE,
        },
      },
    );
    await waitFor(() => expect(view.result.current.data).toBeDefined());
    return view;
  };

  it("fetches again only when the latest status update changes", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    const { rerender } = await renderStatusHistory();

    // Long past any poll, a re-render and a window focus find the history
    // still fresh.
    vi.setSystemTime(Date.now() + 10 * millisecondsInMinute);
    rerender({ replicaIds: ["u1"], statusesUpdatedAt: FIRST_UPDATE });
    act(() => {
      focusManager.setFocused(false);
      focusManager.setFocused(true);
    });
    // Long enough for a request started above to land.
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(requests).toBe(1);

    rerender({ replicaIds: ["u1"], statusesUpdatedAt: NEXT_UPDATE });
    await waitFor(() => expect(requests).toBe(2));
  });

  it("keeps the same replicas' history while a new status update loads", async () => {
    const { result, rerender } = await renderStatusHistory();
    const first = result.current.data;

    rerender({ replicaIds: ["u1"], statusesUpdatedAt: NEXT_UPDATE });
    expect(result.current.data).toEqual(first);

    await waitFor(() =>
      expect(result.current.data?.oomKills).toEqual([
        expect.objectContaining({ occurredAt: new Date(2_000) }),
      ]),
    );
  });

  it("shows no history for a new set of replicas until it loads", async () => {
    const { result, rerender } = await renderStatusHistory();

    rerender({ replicaIds: ["u1", "u2"], statusesUpdatedAt: NEXT_UPDATE });

    expect(result.current.data).toBeUndefined();
  });
});

describe("useObjectHydrationStretches", () => {
  const CREATED_AT = {
    u11: new Date(Date.UTC(2026, 9, 20, 0, 0)),
    u12: new Date(Date.UTC(2026, 9, 20, 0, 5)),
  };

  const hydrationTimesHandler = (
    objects: Array<{ id: string; createdAt: Date }>,
    rows: Array<{
      replicaId: string;
      objectId: string;
      hydrationMs: string | null;
    }>,
    environmentStartedAt = new Date(Date.UTC(2026, 9, 19)),
  ) =>
    // The handler only answers the sorted key.
    buildSqlQueryHandlerV2({
      queryKey: clusterQueryKeys.objectHydrationTimes({
        replicaIds: ["u1", "u2"],
        objects: objects.map(({ id, createdAt }) => ({
          id,
          createdAtMs: createdAt.getTime(),
        })),
      }),
      results: [
        mapKyselyToTabular({
          rows,
          columns: [
            buildColumn({ name: "hydrationMs", type_oid: MzDataType.float8 }),
          ],
        }),
        mapKyselyToTabular({
          rows: [{ startedAt: String(environmentStartedAt.getTime()) }],
          columns: [
            buildColumn({
              name: "startedAt",
              type_oid: MzDataType.timestamptz,
            }),
          ],
        }),
      ],
    });

  const renderStretches = async (
    objects: Array<{ id: string; createdAt: Date }>,
  ) => {
    const ProviderWrapper = await createProviderWrapper();
    return renderHook(
      () => useObjectHydrationStretches({ replicaIds: ["u2", "u1"], objects }),
      { wrapper: ProviderWrapper },
    );
  };

  it("groups each replica's hydration times with the objects' creation times", async () => {
    server.use(
      hydrationTimesHandler(
        [
          { id: "u11", createdAt: CREATED_AT.u11 },
          { id: "u12", createdAt: CREATED_AT.u12 },
        ],
        [
          { replicaId: "u1", objectId: "u11", hydrationMs: "1500" },
          { replicaId: "u1", objectId: "u12", hydrationMs: null },
          { replicaId: "u2", objectId: "u11", hydrationMs: "2500" },
        ],
      ),
    );
    const { result } = await renderStretches([
      { id: "u12", createdAt: CREATED_AT.u12 },
      { id: "u11", createdAt: CREATED_AT.u11 },
    ]);

    await waitFor(() =>
      expect(result.current.data).toEqual(
        new Map([
          [
            "u1",
            [
              {
                objectId: "u11",
                createdAtMs: CREATED_AT.u11.getTime(),
                hydrationMs: 1500,
              },
              {
                objectId: "u12",
                createdAtMs: CREATED_AT.u12.getTime(),
                hydrationMs: null,
              },
            ],
          ],
          [
            "u2",
            [
              {
                objectId: "u11",
                createdAtMs: CREATED_AT.u11.getTime(),
                hydrationMs: 2500,
              },
            ],
          ],
        ]),
      ),
    );
  });

  it("leaves out objects created before environmentd started", async () => {
    const objects = [
      { id: "u11", createdAt: CREATED_AT.u11 },
      { id: "u12", createdAt: CREATED_AT.u12 },
    ];
    server.use(
      hydrationTimesHandler(
        objects,
        [
          { replicaId: "u1", objectId: "u11", hydrationMs: "1500" },
          { replicaId: "u1", objectId: "u12", hydrationMs: "1500" },
        ],
        new Date(Date.UTC(2026, 9, 20, 0, 1)),
      ),
    );
    const { result } = await renderStretches(objects);

    await waitFor(() =>
      expect(result.current.data).toEqual(
        new Map([
          [
            "u1",
            [
              {
                objectId: "u12",
                createdAtMs: CREATED_AT.u12.getTime(),
                hydrationMs: 1500,
              },
            ],
          ],
        ]),
      ),
    );
  });

  it("stays idle without objects", async () => {
    const { result } = await renderStretches([]);

    expect(result.current.fetchStatus).toEqual("idle");
    expect(result.current.data).toBeUndefined();
  });
});

describe("useReplicaUtilizationHistory past the indexed views", () => {
  const THIRTY_DAYS_MINUTES = 30 * 24 * 60;
  const BUCKET_START_MS = Date.UTC(2026, 9, 1);
  const withVersion = (version: string) => ({
    ...healthyEnvironment,
    status: {
      ...healthyEnvironment.status,
      version: parseDbVersion(`v${version} (0000000000)`),
    },
  });
  let utilizationSql: string[];

  // Answers like the database: the swap columns come back only when the query
  // selects them.
  beforeEach(() => {
    vi.stubGlobal("WebSocket", PendingWebSocket);
    utilizationSql = [];
    server.use(
      http.post("*/api/sql", async ({ request }) => {
        const queryKey = JSON.parse(
          new URL(request.url).searchParams.get("query_key") ?? "[]",
        );
        if (queryKey[1]?.scope !== "replicaUtilizationHistory")
          return undefined;
        if (queryKey[2] === "deploymentLineage") {
          return HttpResponse.json({
            results: [
              mapKyselyToTabular({
                rows: [],
                columns: buildColumns([
                  "clusterId",
                  "currentDeploymentClusterId",
                  "clusterName",
                ]),
              }),
            ],
          });
        }
        const body = (await request.json()) as {
          queries: Array<{ query: string }>;
        };
        const sqlText = body.queries[0].query;
        utilizationSql.push(sqlText);
        const at = String(BUCKET_START_MS);
        return HttpResponse.json({
          results: [
            mapKyselyToTabular({
              rows: [
                {
                  bucketStart: at,
                  bucketEnd: String(BUCKET_START_MS + 3_600_000),
                  replicaId: "u1",
                  clusterId: "u9",
                  name: "r1",
                  size: "100cc",
                  maxHeapPercent: "0.6",
                  maxHeapAt: at,
                  maxMemoryAt: at,
                  maxDiskAt: at,
                  maxCpuAt: at,
                  maxMemoryAndDiskAt: at,
                  offlineEvents: null,
                  ...(sqlText.includes('as "swapOfRamPercent"')
                    ? { swapOfRamPercent: "0.25", heapLimitPercent: "1.25" }
                    : {}),
                },
              ],
              columns: [
                ...[
                  "bucketStart",
                  "bucketEnd",
                  "maxHeapAt",
                  "maxMemoryAt",
                  "maxDiskAt",
                  "maxCpuAt",
                  "maxMemoryAndDiskAt",
                ].map((name) =>
                  buildColumn({ name, type_oid: MzDataType.timestamptz }),
                ),
                ...[
                  "maxHeapPercent",
                  "swapOfRamPercent",
                  "heapLimitPercent",
                ].map((name) =>
                  buildColumn({ name, type_oid: MzDataType.float8 }),
                ),
              ],
            }),
          ],
        });
      }),
    );
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  const renderThirtyDays = async ({
    version,
    includeMemoryBreakdown,
  }: {
    version: string;
    includeMemoryBreakdown?: boolean;
  }) => {
    const environment = withVersion(version);
    const ProviderWrapper = await createProviderWrapper({
      initializeState: ({ set }) =>
        setFakeEnvironment(set, defaultRegionId, environment),
    });
    const { result } = renderHook(
      () =>
        useReplicaUtilizationHistory({
          clusterIds: ["u9"],
          replicaId: undefined,
          timePeriodMinutes: THIRTY_DAYS_MINUTES,
          bucketSizeMs: 3_600_000,
          includeMemoryBreakdown,
        }),
      { wrapper: ProviderWrapper },
    );
    await waitFor(() => expect(result.current.data).toBeDefined());
    expect(utilizationSql).toHaveLength(1);
    expect(utilizationSql[0]).toContain("mz_cluster_replica_metrics_history");
    return result.current.data?.graphData[0].data[0];
  };

  it("charts the memory split from the ad-hoc query", async () => {
    const point = await renderThirtyDays({
      version: "26.44.0",
      includeMemoryBreakdown: true,
    });

    expect(point?.swapPercent).toBeCloseTo(20);
    expect(point?.ramLimitPercent).toBeCloseTo(80);
  });

  it("reads no swap column for callers that don't ask for the split", async () => {
    const point = await renderThirtyDays({ version: "26.44.0" });

    expect(utilizationSql[0]).not.toContain("swap");
    expect(point?.ramLimitPercent).toBeNull();
  });

  it("reads no swap column on an environment that predates it", async () => {
    await renderThirtyDays({
      version: "26.43.0",
      includeMemoryBreakdown: true,
    });

    expect(utilizationSql[0]).not.toContain("swap");
  });
});

/**
 * Answers the cluster source exports query with `exportIds`, or fails it, and
 * counts how often it was asked.
 */
function mockClusterSourceExports(exportIds: string[] | "error") {
  const calls = { count: 0 };
  server.use(
    http.post("*/api/sql", ({ request }) => {
      const queryKey = JSON.parse(
        new URL(request.url).searchParams.get("query_key") ?? "[]",
      );
      if (queryKey[1]?.scope !== "clusterSourceExports") return undefined;
      calls.count += 1;
      return HttpResponse.json({
        results: [
          exportIds === "error"
            ? {
                error: {
                  message: "Something went wrong",
                  code: ErrorCode.INTERNAL_ERROR,
                },
                notices: [],
              }
            : mapKyselyToTabular({
                rows: exportIds.map((exportId) => ({ exportId })),
                columns: buildColumns(["exportId"]),
              }),
        ],
      });
    }),
  );
  return calls;
}

async function renderFreshnessObjects(
  initial: DatabaseObject[],
  exportIds: string[] | "error" = [],
  { snapshotComplete = true } = {},
) {
  const exportCalls = mockClusterSourceExports(exportIds);
  const store = createStore();
  store.set(allObjects, {
    data: initial,
    error: undefined,
    snapshotComplete,
  });
  const ProviderWrapper = await createProviderWrapper({ store });
  // Counted so a test can wait for the re-render the subscribe causes. Without
  // that wait an identity assertion passes before anything has happened.
  const renders = { count: 0 };
  const values: ReturnType<typeof useFreshnessObjects>[] = [];
  const view = renderHook(
    () => {
      renders.count += 1;
      const value = useFreshnessObjects("u1");
      values.push(value);
      return value;
    },
    { wrapper: ProviderWrapper },
  );
  return { ...view, store, renders, values, exportCalls };
}

describe("useFreshnessObjects", () => {
  it("keeps its identity when an object on another cluster changes", async () => {
    // `useAllObjects` emits a new array for any change in the environment. A
    // new array here rebuilds the stats and rows chain behind it, so a cluster
    // nobody is looking at would re-render the page.
    const mine = buildDatabaseObject({ id: "u10" });
    const { result, store, renders } = await renderFreshnessObjects([
      mine,
      buildDatabaseObject({ id: "u20", clusterId: "u2", name: "elsewhere" }),
    ]);

    const first = result.current;
    expect(first).toHaveLength(1);
    const before = renders.count;

    store.set(allObjects, {
      data: [
        mine,
        buildDatabaseObject({ id: "u20", clusterId: "u2", name: "renamed" }),
      ],
      error: undefined,
      snapshotComplete: true,
    });

    // The subscribe emitted, so the hook ran again. What it returns must be
    // the array it returned last time.
    await waitFor(() => expect(renders.count).toBeGreaterThan(before));
    expect(result.current).toBe(first);
  });

  it("changes identity when one of its own objects is renamed", async () => {
    const { result, store } = await renderFreshnessObjects([
      buildDatabaseObject({ id: "u10", name: "before" }),
    ]);

    const first = result.current;
    store.set(allObjects, {
      data: [buildDatabaseObject({ id: "u10", name: "after" })],
      error: undefined,
      snapshotComplete: true,
    });

    await waitFor(() => expect(result.current?.[0].objectName).toBe("after"));
    expect(result.current).not.toBe(first);
  });

  it("adds the subsources and tables the cluster's sources ingest into", async () => {
    const { result } = await renderFreshnessObjects(
      [
        buildDatabaseObject({
          id: "u30",
          objectType: "source",
          sourceType: "postgres",
        }),
        buildDatabaseObject({
          id: "u31",
          objectType: "source",
          sourceType: "subsource",
          clusterId: null,
        }),
        buildDatabaseObject({
          id: "u32",
          objectType: "table",
          clusterId: null,
        }),
        buildDatabaseObject({
          id: "u33",
          objectType: "source",
          sourceType: "progress",
          clusterId: null,
        }),
        buildDatabaseObject({
          id: "u34",
          objectType: "table",
          clusterId: null,
        }),
      ],
      ["u31", "u32"],
    );

    // u33 is the source's progress collection and u34 a table written with
    // INSERT, so neither belongs to the cluster.
    await waitFor(() =>
      expect(result.current?.map((o) => o.objectId)).toEqual([
        "u30",
        "u31",
        "u32",
      ]),
    );
  });

  it("reports nothing until the exports of the cluster's sources are known", async () => {
    const { result } = await renderFreshnessObjects(
      [
        buildDatabaseObject({
          id: "u30",
          objectType: "source",
          sourceType: "postgres",
        }),
        buildDatabaseObject({
          id: "u31",
          objectType: "source",
          sourceType: "subsource",
          clusterId: null,
        }),
      ],
      ["u31"],
    );

    expect(result.current).toBeUndefined();
    await waitFor(() =>
      expect(result.current?.map((o) => o.objectId)).toEqual(["u30", "u31"]),
    );
  });

  it("reports nothing until the all-objects snapshot is in", async () => {
    const index = buildDatabaseObject({ id: "u10" });
    const { result, store } = await renderFreshnessObjects([index], [], {
      snapshotComplete: false,
    });
    expect(result.current).toBeUndefined();

    store.set(allObjects, {
      data: [index],
      error: undefined,
      snapshotComplete: true,
    });
    await waitFor(() =>
      expect(result.current?.map((o) => o.objectId)).toEqual(["u10"]),
    );
  });

  it("reports the cluster's own objects when the export lookup fails", async () => {
    const { result } = await renderFreshnessObjects(
      [
        buildDatabaseObject({
          id: "u30",
          objectType: "source",
          sourceType: "postgres",
        }),
        buildDatabaseObject({
          id: "u31",
          objectType: "source",
          sourceType: "subsource",
          clusterId: null,
        }),
      ],
      "error",
    );

    await waitFor(() =>
      expect(result.current?.map((o) => o.objectId)).toEqual(["u30"]),
    );
  });

  it("looks up the exports again when a table is created", async () => {
    const source = buildDatabaseObject({
      id: "u30",
      objectType: "source",
      sourceType: "postgres",
    });
    const { result, store, values, exportCalls } = await renderFreshnessObjects(
      [source],
      [],
    );
    await waitFor(() => expect(exportCalls.count).toBe(1));
    await waitFor(() => expect(result.current).toBeDefined());
    const answered = values.length;

    mockClusterSourceExports(["u35"]);
    store.set(allObjects, {
      data: [
        source,
        buildDatabaseObject({
          id: "u35",
          objectType: "table",
          clusterId: null,
        }),
      ],
      error: undefined,
      snapshotComplete: true,
    });

    await waitFor(() =>
      expect(result.current?.map((o) => o.objectId)).toEqual(["u30", "u35"]),
    );
    // The card keeps the previous list while the new one is looked up.
    expect(values.slice(answered)).not.toContain(undefined);
  });

  it("looks up the exports again when a source moves onto the cluster", async () => {
    const subsource = buildDatabaseObject({
      id: "u31",
      objectType: "source",
      sourceType: "subsource",
      clusterId: null,
    });
    const { result, store, exportCalls } = await renderFreshnessObjects(
      [
        buildDatabaseObject({
          id: "u30",
          objectType: "source",
          sourceType: "postgres",
          clusterId: "u2",
        }),
        subsource,
      ],
      ["u31"],
    );
    expect(result.current).toEqual([]);

    store.set(allObjects, {
      data: [
        buildDatabaseObject({
          id: "u30",
          objectType: "source",
          sourceType: "postgres",
        }),
        subsource,
      ],
      error: undefined,
      snapshotComplete: true,
    });

    await waitFor(() =>
      expect(result.current?.map((o) => o.objectId)).toEqual(["u30", "u31"]),
    );
    expect(exportCalls.count).toBe(1);
  });

  it("does not look up exports for a cluster with no sources", async () => {
    const { result, store, renders, exportCalls } =
      await renderFreshnessObjects([buildDatabaseObject({ id: "u10" })]);
    const before = renders.count;

    store.set(allObjects, {
      data: [
        buildDatabaseObject({ id: "u10" }),
        buildDatabaseObject({
          id: "u11",
          objectType: "table",
          clusterId: null,
        }),
      ],
      error: undefined,
      snapshotComplete: true,
    });

    await waitFor(() => expect(renders.count).toBeGreaterThan(before));
    expect(result.current?.map((o) => o.objectId)).toEqual(["u10"]);
    expect(exportCalls.count).toBe(0);
  });

  it("keeps a name that contains control characters intact", async () => {
    const { result } = await renderFreshnessObjects([
      buildDatabaseObject({ id: "u10", name: "orders\u0001u2" }),
      buildDatabaseObject({ id: "u11", name: "\u0002" }),
    ]);

    expect(result.current?.map((o) => [o.objectId, o.objectName])).toEqual([
      ["u10", "orders\u0001u2"],
      ["u11", "\u0002"],
    ]);
  });
});

describe("buildFreshnessData", () => {
  // Newest first, so a builder that trusts row order fails. Three bins a
  // minute apart, with the newest reading the healthiest.
  const readings = [
    { bucketStart: new Date(180_000), objectId: "u1", lag: parse("00:00:01") },
    { bucketStart: new Date(120_000), objectId: "u1", lag: parse("00:00:05") },
    { bucketStart: new Date(60_000), objectId: "u1", lag: parse("00:10:00") },
  ];
  const objects = [
    {
      objectId: "u1",
      objectName: "orders_mv",
      schemaName: "public",
      databaseName: "materialize",
      schemaId: "u3",
      clusterId: "u1",
      clusterName: "quickstart",
      objectType: "materialized-view",
    },
  ];

  it("runs time forwards whatever order the rows arrive in", () => {
    const data = buildFreshnessData(readings, objects);

    expect(data.startTime).toBe(60_000);
    expect(data.endTime).toBe(180_000);
    expect(data.historicalData.map((d) => d.timestamp)).toEqual([
      60_000, 120_000, 180_000,
    ]);
  });

  it("reports each object's newest bin as its current lag", () => {
    const data = buildFreshnessData(readings, objects);

    expect(data.currentData[0].lag).toEqual(parse("00:00:01"));
  });
});

describe("useRollingWindowStartMs", () => {
  const NOW = Date.parse("2026-10-16T12:00:00Z");
  const HOUR = 60 * millisecondsInMinute;

  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("has its start on the first render and keeps it through mounting", () => {
    // Each read of the clock is a millisecond later, so a second start taken
    // while mounting would differ from the first.
    let reads = 0;
    const now = vi.spyOn(Date, "now").mockImplementation(() => NOW + reads++);
    const renderedStarts: Array<number | undefined> = [];
    renderHook(() => {
      const startMs = useRollingWindowStartMs(180);
      renderedStarts.push(startMs);
      return startMs;
    });
    now.mockRestore();

    expect(renderedStarts[0]).toBeCloseTo(NOW - 3 * HOUR, -3);
    expect(new Set(renderedStarts).size).toBe(1);
  });

  it("starts one window back and moves forward once per window", () => {
    const { result } = renderHook(() => useRollingWindowStartMs(180));
    expect(result.current).toEqual(NOW - 3 * HOUR);

    act(() => vi.advanceTimersByTime(3 * HOUR - 1));
    expect(result.current).toEqual(NOW - 3 * HOUR);

    act(() => vi.advanceTimersByTime(1));
    expect(result.current).toEqual(NOW);
  });

  it("starts the new window back when the window changes, never the old start", () => {
    const renderedStarts: Array<number | undefined> = [];
    const { rerender } = renderHook(
      (windowMinutes: number | undefined) => {
        const startMs = useRollingWindowStartMs(windowMinutes);
        renderedStarts.push(startMs);
        return startMs;
      },
      { initialProps: 180 },
    );
    act(() => vi.advanceTimersByTime(HOUR));
    renderedStarts.length = 0;

    rerender(1440);

    const dayStart = NOW + HOUR - 24 * HOUR;
    expect(renderedStarts.at(-1)).toEqual(dayStart);
    expect(renderedStarts.filter((startMs) => startMs !== undefined)).toEqual(
      renderedStarts.filter((startMs) => startMs === dayStart),
    );
  });

  it("takes a fresh start when a window comes back", () => {
    const { result, rerender } = renderHook(
      (windowMinutes: number | undefined) =>
        useRollingWindowStartMs(windowMinutes),
      { initialProps: 180 },
    );
    rerender(undefined);
    act(() => vi.advanceTimersByTime(HOUR));

    rerender(180);

    expect(result.current).toEqual(NOW + HOUR - 3 * HOUR);
  });

  it("has no start without a window", () => {
    const { result, rerender } = renderHook(
      (windowMinutes: number | undefined) =>
        useRollingWindowStartMs(windowMinutes),
      { initialProps: 180 },
    );

    rerender(undefined);

    expect(result.current).toBeUndefined();
  });
});
