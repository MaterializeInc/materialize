// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { renderHook } from "@testing-library/react";

import useLatestOfflineReplica, {
  shouldSurfaceOom,
} from "./useLatestOfflineReplica";

// The hook reduces the subscribe's per-process rows into one entry per replica.
// Feeding rows directly is the only way to reach that reducer.
const { subscribeRows } = vi.hoisted(() => ({
  subscribeRows: [] as {
    data: {
      replicaId: string | null;
      processId: string;
      clusterId: string;
      lastOfflineAt: Date | null;
      isOom: boolean;
    };
  }[],
}));

vi.mock("../useSubscribe", () => ({
  useSubscribe: () => ({
    data: subscribeRows,
    snapshotComplete: true,
    resubscribing: false,
    isError: false,
    error: undefined,
    disconnect: vi.fn(),
    reset: vi.fn(),
  }),
}));

const offlineRow = (
  replicaId: string | null,
  processId: string,
  lastOfflineAt: Date | null,
  isOom = true,
) => ({
  data: { replicaId, processId, clusterId: "u1", lastOfflineAt, isOom },
});

const renderMap = () =>
  renderHook(() => useLatestOfflineReplica()).result.current.data;

describe("useLatestOfflineReplica", () => {
  beforeEach(() => {
    subscribeRows.length = 0;
  });

  it("keys entries by replica, not by cluster", () => {
    const now = new Date();
    subscribeRows.push(
      offlineRow("u10", "0", now),
      offlineRow("u11", "0", now),
    );

    const map = renderMap();

    // Both replicas belong to cluster u1. Keying by cluster would collapse
    // them into a single entry and lose which replica was affected.
    expect([...map.keys()].sort()).toEqual(["u10", "u11"]);
  });

  it("keeps the newest outage when a replica has several processes", () => {
    const older = new Date("2024-03-01T08:00:00.000Z");
    const newest = new Date("2024-03-07T09:00:00.000Z");
    // Deliberately out of order: the subscribe does not sort its rows.
    subscribeRows.push(
      offlineRow("u10", "0", older),
      offlineRow("u10", "2", newest),
      offlineRow("u10", "1", new Date("2024-03-04T12:00:00.000Z")),
    );

    const map = renderMap();

    expect(map.size).toBe(1);
    expect(map.get("u10")?.lastOfflineAt).toEqual(newest);
  });

  it("skips rows with no replica id or no timestamp", () => {
    const now = new Date();
    subscribeRows.push(
      offlineRow(null, "0", now),
      offlineRow("u12", "0", null),
    );

    expect(renderMap().size).toBe(0);
  });
});

describe("shouldSurfaceOom", () => {
  it("should return true when lastOomAt is within 15 minutes", () => {
    const currentTime = new Date("2023-01-01T12:00:00");
    const lastOomAt = new Date("2023-01-01T11:47:00");

    const result = shouldSurfaceOom(currentTime, lastOomAt);

    expect(result).toBe(true);
  });

  it("should return false when lastOomAt is outside 15 minutes", () => {
    const currentTime = new Date("2023-01-01T12:00:00");
    const lastOomAt = new Date("2023-01-01T11:30:00");

    const result = shouldSurfaceOom(currentTime, lastOomAt);

    expect(result).toBe(false);
  });

  it("should return false when lastOomAt is exactly at the end of 15 minutes", () => {
    const currentTime = new Date("2023-01-01T12:00:00");
    const lastOomAt = new Date("2023-01-01T11:45:00");

    const result = shouldSurfaceOom(currentTime, lastOomAt);

    expect(result).toBe(false);
  });
});
