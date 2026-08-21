// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { diagnoseSourceHealth, IngestionSample } from "./sourceHealthDiagnosis";

const NOW = 1_000_000_000;
const MINUTE = 60_000;

const sample = (
  minutesAgo: number,
  offsetKnown: number,
  offsetCommitted: number,
): IngestionSample => ({
  atMs: NOW - minutesAgo * MINUTE,
  offsetKnown,
  offsetCommitted,
});

describe("diagnoseSourceHealth", () => {
  it("reports healthy when lag is small, even without rate samples", () => {
    const result = diagnoseSourceHealth({
      samples: [],
      lagMs: 1_000,
      nowMs: NOW,
    });
    expect(result.verdict).toBe("healthy");
    expect(result.backlog).toBeNull();
  });

  it("reports measuring when behind but only one sample exists", () => {
    const result = diagnoseSourceHealth({
      samples: [sample(0, 1_000_000, 900_000)],
      lagMs: 4 * 60 * MINUTE,
      nowMs: NOW,
    });
    expect(result.verdict).toBe("measuring");
    expect(result.backlog).toBe(100_000);
    expect(result.stalledSinceMs).toBe(NOW - 4 * 60 * MINUTE);
  });

  it("reports measuring when samples are too close together for a rate", () => {
    const result = diagnoseSourceHealth({
      samples: [
        { atMs: NOW - 5_000, offsetKnown: 100, offsetCommitted: 50 },
        { atMs: NOW, offsetKnown: 110, offsetCommitted: 55 },
      ],
      lagMs: 60 * MINUTE,
      nowMs: NOW,
    });
    expect(result.verdict).toBe("measuring");
  });

  it("reports stalled when upstream produces but nothing commits", () => {
    const result = diagnoseSourceHealth({
      samples: [sample(5, 1_000_000, 500_000), sample(0, 1_040_000, 500_010)],
      lagMs: 60 * MINUTE,
      nowMs: NOW,
    });
    expect(result.verdict).toBe("stalled");
    expect(result.inflowPerSec).toBeCloseTo(40_000 / 300);
  });

  it("reports fallingBehind when drain is below inflow", () => {
    const result = diagnoseSourceHealth({
      samples: [sample(5, 1_000_000, 500_000), sample(0, 1_040_000, 505_000)],
      lagMs: 60 * MINUTE,
      nowMs: NOW,
    });
    expect(result.verdict).toBe("fallingBehind");
    expect(result.drainPerSec).toBeCloseTo(5_000 / 300);
    expect(result.etaMs).toBeNull();
  });

  it("reports catchingUp with an ETA when drain exceeds inflow", () => {
    const result = diagnoseSourceHealth({
      samples: [sample(5, 1_000_000, 500_000), sample(0, 1_003_000, 803_000)],
      lagMs: 60 * MINUTE,
      nowMs: NOW,
    });
    expect(result.verdict).toBe("catchingUp");
    expect(result.backlog).toBe(200_000);
    // net drain = (303_000 - 3_000) / 300s = 1_000/s -> 200s to clear.
    expect(result.etaMs).toBeCloseTo(200_000);
  });

  it("uses the widest sample span so bursts do not flip the verdict", () => {
    const result = diagnoseSourceHealth({
      samples: [
        sample(10, 1_000_000, 500_000),
        sample(5, 1_030_000, 502_000),
        sample(0, 1_060_000, 504_000),
      ],
      lagMs: 60 * MINUTE,
      nowMs: NOW,
    });
    expect(result.verdict).toBe("fallingBehind");
    expect(result.inflowPerSec).toBeCloseTo(60_000 / 600);
  });

  it("reports healthy on an empty backlog even when the lag input is stale", () => {
    // The panel's lag is a windowed maximum that can stay high for hours
    // after a source recovers.
    const result = diagnoseSourceHealth({
      samples: [
        sample(5, 1_000_000, 1_000_000),
        sample(0, 1_050_000, 1_050_000),
      ],
      lagMs: 5 * 60 * 60 * 1000,
      nowMs: NOW,
    });
    expect(result.verdict).toBe("healthy");
  });

  it("does not report healthy on frozen watermarks, even with zero backlog", () => {
    // A paused source (or stale statistics) reports an unmoving backlog of
    // zero; the caught-up rule requires watermark movement.
    const result = diagnoseSourceHealth({
      samples: [
        sample(5, 1_000_000, 1_000_000),
        sample(0, 1_000_000, 1_000_000),
      ],
      lagMs: 5 * 60 * 60 * 1000,
      nowMs: NOW,
    });
    expect(result.verdict).not.toBe("healthy");
  });

  it("clamps a backwards-moving watermark to a zero rate", () => {
    const result = diagnoseSourceHealth({
      samples: [sample(5, 1_000_000, 500_000), sample(0, 990_000, 499_000)],
      lagMs: 60 * MINUTE,
      nowMs: NOW,
    });
    expect(result.inflowPerSec).toBe(0);
    expect(result.drainPerSec).toBe(0);
  });
});
