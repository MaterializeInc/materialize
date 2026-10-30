// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { renderHook, waitFor } from "@testing-library/react";

import { DatabaseObject } from "~/api/materialize/objects";
import { MzDataType } from "~/api/materialize/types";
import { buildColumn } from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { getStore } from "~/jotai";
import { allObjectsCollection } from "~/store/allObjectsCollection";
import {
  MockSubscribeConnection,
  mockSubscribeSocket,
} from "~/test/mockSubscribeSocket";
import {
  createProviderWrapper,
  healthyEnvironment,
  setFakeEnvironment,
} from "~/test/utils";
import { parseDbVersion } from "~/version/api";

import {
  useLiveHydrationProgress,
  useLiveObjectHydrations,
} from "./useLiveHydration";

const HYDRATION_TIMES_ENVIRONMENT = {
  ...healthyEnvironment,
  status: {
    ...healthyEnvironment.status,
    version: parseDbVersion("v26.46.0 (0000000000)"),
  },
};

const PRE_HYDRATION_TIMES_ENVIRONMENT = {
  ...healthyEnvironment,
  status: {
    ...healthyEnvironment.status,
    version: parseDbVersion("v26.38.0 (0000000000)"),
  },
};

const catalogObject = (id: string, name: string): DatabaseObject => ({
  databaseName: "materialize",
  databaseId: "u1",
  name,
  schemaName: "public",
  schemaId: "u2",
  id,
  objectType: "index",
  sourceType: null,
  isWebhookTable: null,
  clusterId: "u9",
  clusterName: "analytics",
});

describe("useLiveHydrationProgress", () => {
  it("counts each current replica's hydrated objects from the stream", async () => {
    const sessions: Record<string, unknown>[] = [];
    server.use(
      mockSubscribeSocket((sql, params, connection) => {
        sessions.push(connection.options);
        return sql.includes("mz_compute_hydration_times") &&
          params.includes("u31")
          ? {
              columns: [
                buildColumn({ name: "replicaId" }),
                buildColumn({ name: "objectId" }),
                buildColumn({
                  name: "isHydrated",
                  type_oid: MzDataType.bool,
                }),
              ],
              rows: [
                ["u31", "u5", true],
                ["u31", "u6", false],
                ["u31", "u7", false],
              ],
            }
          : undefined;
      }),
    );

    const { result } = renderHook(
      () =>
        useLiveHydrationProgress({
          clusterId: "u90",
          replicaIds: ["u32", "u31"],
        }),
      { wrapper: await createProviderWrapper() },
    );

    await waitFor(() => expect(result.current.snapshotComplete).toBe(true));
    // Not pinned to a replica, so it runs on the catalog server.
    expect(sessions).toContainEqual(
      expect.objectContaining({ cluster: "mz_catalog_server" }),
    );
    expect(Object.fromEntries(result.current.progressByReplicaId)).toEqual({
      u31: {
        objectCount: 3,
        hydratedCount: 1,
        unhydratedObjectIds: expect.arrayContaining(["u6", "u7"]),
      },
      // A replica that reports nothing has nothing to hydrate yet.
      u32: { objectCount: 0, hydratedCount: 0, unhydratedObjectIds: [] },
    });
  });

  it("is complete at once for a cluster with no replicas", async () => {
    const { result } = renderHook(
      () => useLiveHydrationProgress({ clusterId: "u91", replicaIds: [] }),
      { wrapper: await createProviderWrapper() },
    );

    expect(result.current.snapshotComplete).toBe(true);
    expect(result.current.progressByReplicaId.size).toBe(0);
  });
});

describe("useLiveObjectHydrations", () => {
  beforeEach(async () => {
    await setFakeEnvironment(
      getStore().set,
      "aws/us-east-1",
      HYDRATION_TIMES_ENVIRONMENT,
    );
    allObjectsCollection.applySnapshot({
      data: [catalogObject("u10", "orders_idx")],
      snapshotComplete: true,
      error: undefined,
    });
  });

  it("reads each object's state on the replica it pins the session to", async () => {
    const sessions: Record<string, unknown>[] = [];
    server.use(
      mockSubscribeSocket((sql, _params, connection) => {
        sessions.push(connection.options);
        return sql.includes("mz_compute_hydration_times_per_worker")
          ? {
              columns: [
                buildColumn({ name: "globalId" }),
                buildColumn({ name: "objectId" }),
                buildColumn({
                  name: "installedAt",
                  type_oid: MzDataType.timestamptz,
                }),
                buildColumn({
                  name: "startedAt",
                  type_oid: MzDataType.timestamptz,
                }),
                buildColumn({
                  name: "hydratedAt",
                  type_oid: MzDataType.timestamptz,
                }),
              ],
              rows: [
                ["u20", "u10", "1792540800000", "1792540800000", null],
                ["u21", null, "1792540801000", null, null],
                [
                  "u22",
                  null,
                  "1792540802000",
                  "1792540802000",
                  "1792540810000",
                ],
              ],
            }
          : undefined;
      }),
    );

    const { result } = renderHook(
      () =>
        useLiveObjectHydrations({
          clusterName: "analytics",
          replicaId: "u41",
          replicaName: "r2",
        }),
      { wrapper: await createProviderWrapper() },
    );

    await waitFor(() => expect(result.current.snapshotComplete).toBe(true));
    expect(sessions).toContainEqual(
      expect.objectContaining({ cluster: "analytics", cluster_replica: "r2" }),
    );
    expect(
      result.current.objects.map(({ name, state }) => [name, state]),
    ).toEqual([
      ["orders_idx", "working"],
      // Not in the catalog, so a dropped object's dataflow, named by its id.
      ["u21", "queued"],
      ["u22", "hydrated"],
    ]);
  });

  it("follows the replica's new process after its subscribe fails over a restart", async () => {
    const connections: MockSubscribeConnection[] = [];
    // Before the restart u20 is working. After it, the new process is waiting
    // for a hydration slot.
    const rowsByRun = [
      [["u20", "u10", "1792540800000", "1792540800000", null]],
      [["u20", "u10", "1792540900000", null, null]],
    ];
    server.use(
      mockSubscribeSocket((sql, _params, connection) => {
        if (!sql.includes("mz_compute_hydration_times_per_worker")) {
          return undefined;
        }
        connections.push(connection);
        return {
          columns: [
            buildColumn({ name: "globalId" }),
            buildColumn({ name: "objectId" }),
            buildColumn({
              name: "installedAt",
              type_oid: MzDataType.timestamptz,
            }),
            buildColumn({
              name: "startedAt",
              type_oid: MzDataType.timestamptz,
            }),
            buildColumn({
              name: "hydratedAt",
              type_oid: MzDataType.timestamptz,
            }),
          ],
          rows: rowsByRun[Math.min(connections.length - 1, 1)],
        };
      }),
    );
    const { result } = renderHook(
      () =>
        useLiveObjectHydrations({
          clusterName: "analytics",
          replicaId: "u43",
          replicaName: "r3",
        }),
      { wrapper: await createProviderWrapper() },
    );
    await waitFor(() =>
      expect(result.current.objects.map(({ state }) => state)).toEqual([
        "working",
      ]),
    );

    connections[0].endWithError("target replica failed or was dropped");

    await waitFor(() =>
      expect(result.current.objects.map(({ state }) => state)).toEqual([
        "queued",
      ]),
    );
    expect(result.current.isError).toBe(false);
  });

  it("doesn't subscribe before the environment records per-worker hydration times", async () => {
    await setFakeEnvironment(
      getStore().set,
      "aws/us-east-1",
      PRE_HYDRATION_TIMES_ENVIRONMENT,
    );
    let requests = 0;
    server.use(
      mockSubscribeSocket((sql) => {
        if (sql.includes("mz_compute_hydration_times_per_worker")) {
          requests += 1;
        }
        return undefined;
      }),
    );

    const { result } = renderHook(
      () =>
        useLiveObjectHydrations({
          clusterName: "analytics",
          replicaId: "u42",
          replicaName: "r1",
        }),
      { wrapper: await createProviderWrapper() },
    );

    // Give a subscribe time to start.
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(requests).toEqual(0);
    expect(result.current.objects).toEqual([]);
  });
});
