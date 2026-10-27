// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { addMinutes, addSeconds } from "date-fns";
import parsePostgresInterval from "postgres-interval";

import { SourceStatisticsDataPoint } from "~/api/materialize/source/sourceStatistics";
import { SubscribeRow } from "~/api/materialize/SubscribeManager";
import { bucketAggregator } from "~/platform/sources/SourceOverview/utils";

import {
  aggregateBucketData,
  bucketPoints,
  normalizeStatisticsData,
} from "./graph";

const SNAPSHOT_TIME = new Date("2024-01-01T12:00:00Z");
const DATA_INTERVAL = 60_000;

type Nullable<T> = {
  [P in keyof T]: T[P] | null;
};

function buildProgressMessage(
  date: Date,
): SubscribeRow<Nullable<SourceStatisticsDataPoint>> {
  return {
    mzState: null,
    mzTimestamp: date.getTime(),
    mzProgressed: true,
    data: {
      id: null,
      bytesReceived: null,
      messagesReceived: null,
      offsetDelta: null,
      updatesCommitted: null,
      updatesStaged: null,
      snapshotRecordsKnown: null,
      snapshotRecordsStaged: null,
      rehydrationLatency: null,
      replicaId: null,
      replicaName: null,
    },
  };
}
function buildPoint(
  date: Date,
  data?: Partial<SourceStatisticsDataPoint>,
): SubscribeRow<SourceStatisticsDataPoint> {
  return {
    mzState: "upsert" as const,
    mzTimestamp: date.getTime(),
    mzProgressed: false,
    data: {
      id: "",
      bytesReceived: 0,
      messagesReceived: 0,
      offsetDelta: 0,
      updatesCommitted: 0,
      updatesStaged: 0,
      snapshotRecordsKnown: 0,
      snapshotRecordsStaged: 0,
      rehydrationLatency: null,
      replicaId: "",
      replicaName: "",
      ...data,
    },
  };
}

describe("normalizeStatisticsData", () => {
  it("does nothing for empty data", () => {
    expect(normalizeStatisticsData([], DATA_INTERVAL)).toEqual([]);
  });

  it("correctly inserts points after the snapshot when there is no data in the period", () => {
    const data = [
      buildPoint(SNAPSHOT_TIME),
      buildProgressMessage(addMinutes(SNAPSHOT_TIME, 3)),
    ];
    const result = normalizeStatisticsData(data, DATA_INTERVAL);
    expect(result.map((r) => new Date(r.mzTimestamp))).toEqual([
      new Date("2024-01-01T12:01:00.000Z"),
      new Date("2024-01-01T12:02:00.000Z"),
    ]);
  });

  it("synthesized points have the same values as the previous point", () => {
    const pointData: SourceStatisticsDataPoint = {
      id: "1",
      replicaId: "u1",
      replicaName: "r1",
      bytesReceived: 2,
      messagesReceived: 3,
      offsetDelta: 1,
      updatesCommitted: 6,
      updatesStaged: 7,
      snapshotRecordsKnown: 8,
      snapshotRecordsStaged: 9,
      rehydrationLatency: parsePostgresInterval("00:01:33.676"),
    };
    const data = [
      buildPoint(SNAPSHOT_TIME, pointData),
      buildProgressMessage(addMinutes(SNAPSHOT_TIME, 2)),
    ];
    const result = normalizeStatisticsData(data, DATA_INTERVAL);
    expect(result.map((r) => r.data)).toEqual([pointData]);
  });

  it("correctly inserts points after the snapshot when there is a gap", () => {
    const data = [
      buildPoint(SNAPSHOT_TIME),
      buildPoint(addMinutes(SNAPSHOT_TIME, 2)),
      buildProgressMessage(addMinutes(SNAPSHOT_TIME, 2)),
    ];
    const result = normalizeStatisticsData(data, DATA_INTERVAL);
    expect(result.map((r) => new Date(r.mzTimestamp))).toEqual([
      new Date("2024-01-01T12:01:00.000Z"),
      new Date("2024-01-01T12:02:00.000Z"),
    ]);
  });

  it("correctly inserts inital points when the timestamps are slightly off", () => {
    const data = [
      buildPoint(SNAPSHOT_TIME),
      buildPoint(addSeconds(SNAPSHOT_TIME, 119)),
      buildProgressMessage(addMinutes(SNAPSHOT_TIME, 2)),
    ];
    const result = normalizeStatisticsData(data, DATA_INTERVAL);
    expect(result.map((r) => new Date(r.mzTimestamp))).toEqual([
      new Date("2024-01-01T12:00:59.000Z"),
      new Date("2024-01-01T12:01:59.000Z"),
    ]);
  });

  it("correctly inserts points in the middle", () => {
    const data = [
      buildPoint(SNAPSHOT_TIME),
      buildPoint(addSeconds(SNAPSHOT_TIME, 60)),
      buildPoint(addSeconds(SNAPSHOT_TIME, 180)),
      buildProgressMessage(addMinutes(SNAPSHOT_TIME, 3)),
    ];
    const result = normalizeStatisticsData(data, DATA_INTERVAL);
    expect(result.map((r) => new Date(r.mzTimestamp))).toEqual([
      new Date("2024-01-01T12:01:00.000Z"),
      new Date("2024-01-01T12:02:00.000Z"),
      new Date("2024-01-01T12:03:00.000Z"),
    ]);
  });

  it("correctly inserts points in the middle when the timestamps are slightly off", () => {
    const data = [
      buildPoint(SNAPSHOT_TIME),
      buildPoint(addSeconds(SNAPSHOT_TIME, 60)),
      buildPoint(addSeconds(SNAPSHOT_TIME, 179)),
      buildPoint(addSeconds(SNAPSHOT_TIME, 301)),
      buildProgressMessage(addMinutes(SNAPSHOT_TIME, 6)),
    ];
    const result = normalizeStatisticsData(data, DATA_INTERVAL);
    expect(result.map((r) => new Date(r.mzTimestamp))).toEqual([
      new Date("2024-01-01T12:01:00.000Z"),
      new Date("2024-01-01T12:02:00.000Z"),
      new Date("2024-01-01T12:02:59.000Z"),
      new Date("2024-01-01T12:03:59.000Z"),
      new Date("2024-01-01T12:05:01.000Z"),
    ]);
  });

  it("correctly inserts points at the end", () => {
    const data = [
      buildPoint(SNAPSHOT_TIME),
      buildPoint(addSeconds(SNAPSHOT_TIME, 60)),
      buildProgressMessage(addMinutes(SNAPSHOT_TIME, 3)),
    ];
    const result = normalizeStatisticsData(data, DATA_INTERVAL);
    expect(result.map((r) => new Date(r.mzTimestamp))).toEqual([
      new Date("2024-01-01T12:01:00.000Z"),
      new Date("2024-01-01T12:02:00.000Z"),
    ]);
  });
});

describe("bucketPoints", () => {
  it("does nothing for empty data", () => {
    expect(bucketPoints([], [], DATA_INTERVAL)).toEqual([]);
  });

  it("groups points into buckets", () => {
    const a = buildPoint(addMinutes(SNAPSHOT_TIME, 1));
    const b = buildPoint(addMinutes(SNAPSHOT_TIME, 2));
    const c = buildPoint(addMinutes(SNAPSHOT_TIME, 3));
    const d = buildPoint(addMinutes(SNAPSHOT_TIME, 4));

    expect(
      bucketPoints([a, b, c, d], [b.mzTimestamp, d.mzTimestamp], DATA_INTERVAL),
    ).toEqual([
      {
        timestamp: b.mzTimestamp,
        dataPoints: [a, b],
      },
      {
        timestamp: d.mzTimestamp,
        dataPoints: [c, d],
      },
    ]);
  });

  it("handles empty buckets correctly", () => {
    const firstEmptyTimestamp = addMinutes(SNAPSHOT_TIME, 1).getTime();
    const second = buildPoint(addMinutes(SNAPSHOT_TIME, 2));
    const anotherEmptyTimestamp = addMinutes(SNAPSHOT_TIME, 3).getTime();
    const lastEmptyTimestamp = addMinutes(SNAPSHOT_TIME, 4).getTime();

    expect(
      bucketPoints(
        [second],
        [
          firstEmptyTimestamp,
          second.mzTimestamp,
          anotherEmptyTimestamp,
          lastEmptyTimestamp,
        ],
        DATA_INTERVAL,
      ),
    ).toEqual([
      {
        timestamp: firstEmptyTimestamp,
        dataPoints: [],
      },
      {
        timestamp: second.mzTimestamp,
        dataPoints: [second],
      },
      {
        timestamp: anotherEmptyTimestamp,
        dataPoints: [],
      },
      {
        timestamp: lastEmptyTimestamp,
        dataPoints: [],
      },
    ]);
  });
});

describe("aggregateBucketData", () => {
  it("correctly calculates a rates and max");

  it("rates are based on the first and last point in the bucket", () => {
    const first = addMinutes(SNAPSHOT_TIME, 1);
    const second = addMinutes(SNAPSHOT_TIME, 1.5);
    const end = addMinutes(SNAPSHOT_TIME, 2);
    expect(
      aggregateBucketData(
        [
          {
            timestamp: end.getTime(),
            dataPoints: [
              buildPoint(first),
              buildPoint(second, {
                bytesReceived: 1,
                messagesReceived: 2,
                updatesCommitted: 3,
                offsetDelta: 10,
                updatesStaged: 4,
              }),
              buildPoint(end, {
                bytesReceived: 60,
                messagesReceived: 30,
                offsetDelta: 100,
                updatesCommitted: 180,
                updatesStaged: 240,
              }),
            ],
          },
        ],
        bucketAggregator,
      ),
    ).toEqual([
      {
        timestamp: end.getTime(),
        // rates
        bytesReceivedPerSecond: 1,
        messagesReceivedPerSecond: 0.5,
        updatesCommittedPerSecond: 3,
        updatesStagedPerSecond: 4,
        // max
        offsetDelta: 100,
      },
    ]);
  });

  it("does not create rates for buckets with 0 or 1 points", () => {
    const point = buildPoint(addMinutes(SNAPSHOT_TIME, 2));
    expect(
      aggregateBucketData(
        [
          {
            timestamp: SNAPSHOT_TIME.getTime(),
            dataPoints: [],
          },
          {
            timestamp: point.mzTimestamp,
            dataPoints: [point],
          },
        ],
        bucketAggregator,
      ),
    ).toEqual([
      {
        timestamp: SNAPSHOT_TIME.getTime(),
        bytesReceivedPerSecond: null,
        messagesReceivedPerSecond: null,
        updatesCommittedPerSecond: null,
        updatesStagedPerSecond: null,
        offsetDelta: null,
      },
      {
        timestamp: point.mzTimestamp,
        // rates
        bytesReceivedPerSecond: null,
        messagesReceivedPerSecond: null,
        updatesCommittedPerSecond: null,
        updatesStagedPerSecond: null,
        // max
        offsetDelta: 0,
      },
    ]);
  });

  it("for buckets with a single point, look back to previous buckets to calculate rates", () => {
    const first = addMinutes(SNAPSHOT_TIME, 1);
    const end = addMinutes(SNAPSHOT_TIME, 2);
    expect(
      aggregateBucketData(
        [
          {
            timestamp: first.getTime(),
            dataPoints: [buildPoint(first)],
          },
          {
            timestamp: end.getTime(),
            dataPoints: [
              buildPoint(end, {
                bytesReceived: 60,
                messagesReceived: 30,
                offsetDelta: 100,
                updatesCommitted: 180,
                updatesStaged: 240,
              }),
            ],
          },
        ],
        bucketAggregator,
      ),
    ).toEqual([
      {
        timestamp: first.getTime(),
        // rates
        bytesReceivedPerSecond: null,
        messagesReceivedPerSecond: null,
        updatesCommittedPerSecond: null,
        updatesStagedPerSecond: null,
        // max
        offsetDelta: 0,
      },
      {
        timestamp: end.getTime(),
        // rates
        bytesReceivedPerSecond: 1,
        messagesReceivedPerSecond: 0.5,
        updatesCommittedPerSecond: 3,
        updatesStagedPerSecond: 4,
        // max
        offsetDelta: 100,
      },
    ]);
  });
});
