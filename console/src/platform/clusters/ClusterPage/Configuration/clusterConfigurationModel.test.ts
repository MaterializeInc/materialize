// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  formatLinger,
  parseAutoScaling,
  replicaStatus,
  resetAutoScalingStatement,
  resizeNote,
  setAutoScalingStatement,
} from "./clusterConfigurationModel";

const status = (state: string, reason: string | null = null) => ({
  replica_id: "u3",
  process_id: "0",
  reason,
  status: state,
  updated_at: "2026-07-23T22:26:00.000Z",
});

describe("parseAutoScaling", () => {
  it("reads the hydration size, the linger and an in-flight burst", () => {
    expect(
      parseAutoScaling({
        strategy: {
          on_hydration: {
            hydration_size: "800cc",
            linger_duration: { secs: 15, nanos: 500_000_000 },
          },
        },
        state: {
          burst: {
            burst_size: "800cc",
            linger_duration: { secs: 15, nanos: 0 },
            steady_ready_at: null,
          },
        },
      }),
    ).toEqual({
      policy: { hydrationSize: "800cc", lingerMs: 15_500 },
      burst: { size: "800cc", lingerMs: 15_000 },
    });
  });

  it("leaves the linger to the default when the strategy doesn't set it", () => {
    expect(
      parseAutoScaling({
        strategy: { on_hydration: { hydration_size: "800cc" } },
        state: null,
      }),
    ).toEqual({
      policy: { hydrationSize: "800cc", lingerMs: undefined },
      burst: undefined,
    });
  });

  it("reads no policy without a row or an on-hydration sub-policy", () => {
    expect(parseAutoScaling(null)).toEqual({ policy: null, burst: undefined });
    expect(parseAutoScaling({ strategy: {}, state: null }).policy).toBeNull();
  });

  it("keeps a burst that outlasts a removed policy", () => {
    expect(
      parseAutoScaling({
        strategy: null,
        state: {
          burst: {
            burst_size: "800cc",
            linger_duration: { secs: 15, nanos: 0 },
          },
        },
      }),
    ).toEqual({ policy: null, burst: { size: "800cc", lingerMs: 15_000 } });
  });
});

describe("replicaStatus", () => {
  it("calls a replica online only when every process is", () => {
    expect(replicaStatus([status("online"), status("online")])).toEqual({
      kind: "online",
    });
    expect(
      replicaStatus([status("online"), status("offline", "oom-killed")]),
    ).toEqual({ kind: "offline", reason: "oom-killed" });
    expect(replicaStatus([])).toEqual({ kind: "unknown" });
  });
});

describe("auto-scaling statements", () => {
  it("quotes the cluster and writes the linger in seconds", () => {
    expect(setAutoScalingStatement("my cluster", "800cc", 120_000)).toBe(
      [
        `ALTER CLUSTER "my cluster" SET (`,
        "  AUTO SCALING STRATEGY = (",
        "    ON HYDRATION (HYDRATION SIZE = '800cc', LINGER DURATION = '120s')",
        "  )",
        ");",
      ].join("\n"),
    );
    expect(resetAutoScalingStatement("prod")).toBe(
      `ALTER CLUSTER "prod" RESET (AUTO SCALING STRATEGY);`,
    );
  });

  it("escapes a quote in the size", () => {
    expect(setAutoScalingStatement("prod", "it's", undefined)).toContain(
      "HYDRATION SIZE = 'it''s')",
    );
  });

  it("keeps a zero or sub-second linger as it is", () => {
    const linger = (lingerMs: number) =>
      setAutoScalingStatement("prod", "800cc", lingerMs).match(
        /LINGER DURATION = ('[^']*')/,
      )?.[1];
    expect(linger(0)).toBe("'0s'");
    expect(linger(1_500)).toBe("'1500ms'");
    expect(linger(250)).toBe("'250ms'");
    expect(linger(15_000)).toBe("'15s'");
    // `{ secs: 1, nanos: 32_053_000 }`, which parses to 1032.0529999999999.
    expect(linger(1_000 + 32_053_000 / 1e6)).toBe("'1032.053ms'");
  });
});

describe("formatLinger", () => {
  it("reads a zero linger in seconds", () => {
    expect(formatLinger(0)).toBe("0s");
    expect(formatLinger(1_500)).toBe("1.5s");
  });
});

describe("resizeNote", () => {
  const CLUSTER = { size: "100cc", replicationFactor: 1 };

  it("names the target size, and the target replication factor when it changes too", () => {
    expect(resizeNote(CLUSTER, { size: "800cc", replicationFactor: 1 })).toBe(
      "A resize to 800cc is in progress. Size shows the current size until it finishes.",
    );
    expect(resizeNote(CLUSTER, { size: "800cc", replicationFactor: 2 })).toBe(
      "A resize to 800cc with replication factor 2 is in progress. Size and replication factor show the current values until it finishes.",
    );
    expect(resizeNote(CLUSTER, { size: "100cc", replicationFactor: 2 })).toBe(
      "A change to replication factor 2 is in progress. Replication factor shows the current factor until it finishes.",
    );
  });

  it("says nothing when the size and replication factor stay", () => {
    expect(
      resizeNote(CLUSTER, { size: "100cc", replicationFactor: 1 }),
    ).toBeUndefined();
    expect(
      resizeNote(CLUSTER, { size: null, replicationFactor: null }),
    ).toBeUndefined();
  });
});
