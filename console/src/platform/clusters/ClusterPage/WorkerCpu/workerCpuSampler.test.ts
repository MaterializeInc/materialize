// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { createSubscribeCollection } from "~/api/materialize/subscribeCollection";
import { MzDataType } from "~/api/materialize/types";
import { buildColumn } from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import {
  MockSubscribeConnection,
  mockSubscribeSocket,
} from "~/test/mockSubscribeSocket";

import {
  ADHOC_ROW_ID,
  DataflowExport,
  DataflowWorkerCpu,
  hasCounterReset,
  Measurement,
  ObjectCpuRead,
  workerBusyBetween,
} from "./workerCpuModel";
import {
  createWorkerCpuSampler,
  SAMPLING_LIMIT_MS,
  StoredContinuation,
  StoredWorkerReading,
} from "./workerCpuSampler";

const START_MS = Date.UTC(2026, 9, 16, 12);
const SECOND_NS = 1e9;
const SESSION_VARIABLES = { cluster: "prod", cluster_replica: "r1" };
// Past the longest test's clock.
const REPORT_HORIZON_S = 15 * 60;

const COLUMNS = [
  buildColumn({ name: "kind" }),
  buildColumn({ name: "dataflowId" }),
  buildColumn({ name: "workerId", type_oid: MzDataType.int4 }),
  buildColumn({ name: "elapsedNs", type_oid: MzDataType.float8 }),
  buildColumn({ name: "objectId" }),
  buildColumn({ name: "connectionId", type_oid: MzDataType.uint4 }),
  buildColumn({ name: "dataflowName" }),
];

// The subscribe's upsert key: kind, dataflow, worker and object.
const rowKey = (row: unknown[]) => [0, 1, 2, 4].map((i) => row[i]).join("|");

interface Session {
  connection: MockSubscribeConnection;
  /** What the subscribe has sent, by upsert key. */
  sent: Map<string, unknown[]>;
  query: string;
  /** The dataflow the session's subscribe runs. */
  dataflowId: string;
  startedAtMs: number;
  closedAtMs?: number;
}

/**
 * Serves the sampler's subscribe from mutable counters, reporting them each
 * second as a replica's introspection does. Each session's subscribe runs a
 * dataflow that spends `ownNsPerSecond` on each worker, as the Workers tab's
 * own does, and that keeps running for `lingerMs` after its session closes,
 * untagged. Create it before the sampler, so its report at a given instant
 * lands before a reading due then.
 */
function createFakeReplica({
  ownNsPerSecond = 0,
  lingerMs = 0,
  failing = false,
}: {
  ownNsPerSecond?: number;
  lingerMs?: number;
  /** Fails the subscribe, as a version without its relations. */
  failing?: boolean;
} = {}) {
  const sessions: Session[] = [];
  const replica = {
    /** Every dataflow but the sessions' own, on two workers. */
    dataflows: [
      { dataflowId: "1", workerId: 0, elapsedNs: 10 * SECOND_NS },
      { dataflowId: "1", workerId: 1, elapsedNs: 10 * SECOND_NS },
    ] as DataflowWorkerCpu[],
    exports: [] as DataflowExport[],
    /** How far the counters trail the clock. */
    lagMs: 0,
    /** Stops the counters from reporting, as on a stuck replica. */
    isStalled: false,
    /** Holds back each subscribe's snapshot, as on a replica that can't serve it. */
    holdsSnapshots: false,
    /**
     * Reports the counters every this many seconds, as on a replica whose
     * workers are too busy to advance its introspection each second.
     */
    reportEverySeconds: 1,
    /** Changes the counters before each report. */
    onSecond: undefined as (() => void) | undefined,
    sessions,
    open: () => sessions.filter(({ connection }) => !connection.isClosed),
    /** Adds `seconds` to a dataflow's counter on each worker. */
    grow: (dataflowId: string, seconds: number[]) => {
      replica.dataflows = replica.dataflows.map((row) =>
        row.dataflowId === dataflowId
          ? {
              ...row,
              elapsedNs: row.elapsedNs + seconds[row.workerId] * SECOND_NS,
            }
          : row,
      );
    },
    rows: () => {
      const nowMs = Date.now();
      for (const session of sessions) {
        if (session.connection.isClosed) session.closedAtMs ??= nowMs;
      }
      const running = sessions.filter(
        ({ closedAtMs }) =>
          closedAtMs === undefined || nowMs < closedAtMs + lingerMs,
      );
      const own = running.flatMap(({ dataflowId, startedAtMs, closedAtMs }) =>
        [0, 1].map((workerId) => ({
          dataflowId,
          workerId,
          elapsedNs:
            (ownNsPerSecond * ((closedAtMs ?? nowMs) - startedAtMs)) / 1000,
        })),
      );
      return [
        ...[...replica.dataflows, ...own].map(
          ({ dataflowId, workerId, elapsedNs }) => [
            "elapsed",
            dataflowId,
            String(workerId),
            String(elapsedNs),
            null,
            null,
            null,
          ],
        ),
        ...replica.exports.map(({ objectId, dataflowId, dataflowName }) => [
          "export",
          dataflowId,
          null,
          null,
          objectId,
          null,
          dataflowName ?? null,
        ]),
        // A closed session's tag leaves before its dataflow does.
        ...running.map(({ connection, dataflowId, closedAtMs }) => [
          "export",
          dataflowId,
          null,
          null,
          `t${dataflowId}`,
          closedAtMs === undefined ? String(connection.connectionId) : null,
          `Dataflow: subscribe-t${dataflowId}`,
        ]),
      ];
    },
  };
  // Scheduled before the sampler's timers, so a report lands before a reading
  // due at the same instant.
  for (let second = 1; second <= REPORT_HORIZON_S; second++) {
    setTimeout(() => {
      if (replica.isStalled) return;
      replica.onSecond?.();
      if (second % replica.reportEverySeconds !== 0) return;
      for (const session of replica.open()) {
        const rows = replica.rows();
        const sent = new Map(rows.map((row) => [rowKey(row), row]));
        const deletes = [...session.sent]
          .filter(([key]) => !sent.has(key))
          .map(([, row]) => row);
        session.sent = sent;
        session.connection.advance(Date.now() - replica.lagMs, rows, deletes);
      }
    }, second * 1000);
  }
  server.use(
    mockSubscribeSocket((query, _params, connection) => {
      if (!query.startsWith("SUBSCRIBE")) {
        throw new Error(`Unexpected query: ${query}`);
      }
      if (failing) {
        connection.error("unknown catalog item 'mz_scheduling_elapsed_raw'");
        return undefined;
      }
      const rows = replica.rows();
      sessions.push({
        connection,
        sent: new Map(rows.map((row) => [rowKey(row), row])),
        query,
        dataflowId: String(1000 + sessions.length),
        startedAtMs: Date.now(),
      });
      if (replica.holdsSnapshots) return undefined;
      return {
        columns: COLUMNS,
        rows,
        timestamp: Date.now() - replica.lagMs - 1,
      };
    }),
  );
  return replica;
}

let storeCount = 0;
const createStores = () => {
  storeCount += 1;
  return {
    readingsStore: createSubscribeCollection<StoredWorkerReading>({
      id: `worker-cpu-sampler-test-readings-${storeCount}`,
      getKey: (reading) => String(reading.atMs),
    }),
    objectsStore: createSubscribeCollection<ObjectCpuRead>({
      id: `worker-cpu-sampler-test-objects-${storeCount}`,
      getKey: (read) => String(read.atMs),
    }),
    continuationStore: createSubscribeCollection<StoredContinuation>({
      id: `worker-cpu-sampler-test-continuation-${storeCount}`,
      getKey: () => "last",
    }),
    measurementStore: createSubscribeCollection<Measurement>({
      id: `worker-cpu-sampler-test-measurement-${storeCount}`,
      getKey: () => "last",
    }),
  };
};

type Stores = ReturnType<typeof createStores>;

const samplers: Array<ReturnType<typeof createWorkerCpuSampler>> = [];

const createSampler = (stores: Stores, workerCount = 2) => {
  const sampler = createWorkerCpuSampler({
    httpAddress: "replica.test",
    sessionVariables: SESSION_VARIABLES,
    workerCount,
    ...stores,
  });
  samplers.push(sampler);
  return sampler;
};

const startSampler = (stores = createStores()) => {
  const sampler = createSampler(stores);
  sampler.start();
  return sampler;
};

/** A replica where u1's dataflow takes half of worker 0 and a quarter of 1. */
const createSteadyReplica = () => {
  const replica = createFakeReplica();
  replica.exports = [{ objectId: "u1", dataflowId: "1" }];
  replica.onSecond = () => replica.grow("1", [0.5, 0.25]);
  return replica;
};

/** Moves the clock, delivering what the sockets send along the way. */
const advance = (ms: number) => vi.advanceTimersByTimeAsync(ms);

const sinceStart = (readings: Array<{ atMs: number }>) =>
  readings.map(({ atMs }) => atMs - START_MS);

const spanSinceStart = (measurement: Measurement | undefined) =>
  measurement && [measurement.startMs - START_MS, measurement.endMs - START_MS];

/** Each worker's growth between two readings, in seconds. */
const growthS = (
  before: { elapsedNsByWorker: Map<number, number> } | undefined,
  after: { elapsedNsByWorker: Map<number, number> } | undefined,
) =>
  [0, 1].map((workerId) =>
    Number(
      (
        ((after?.elapsedNsByWorker.get(workerId) ?? NaN) -
          (before?.elapsedNsByWorker.get(workerId) ?? NaN)) /
        SECOND_NS
      ).toFixed(2),
    ),
  );

describe("createWorkerCpuSampler", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(START_MS);
  });

  afterEach(() => {
    for (const sampler of samplers.splice(0)) sampler.stop();
    vi.useRealTimers();
  });

  it("subscribes once, on one session pinned to the replica, up to the sampling limit", async () => {
    const replica = createFakeReplica();
    startSampler();
    await advance(0);

    expect(replica.sessions).toHaveLength(1);
    expect(replica.sessions[0].connection.options).toMatchObject(
      SESSION_VARIABLES,
    );
    expect(replica.sessions[0].query).toContain(
      `UP TO ${START_MS + SAMPLING_LIMIT_MS}`,
    );
  });

  it("reads every worker's counters once the snapshot lands, then every 5 seconds", async () => {
    createFakeReplica();
    const sampler = startSampler();

    await advance(30_000);

    expect(sinceStart(sampler.getState().readings)).toEqual([
      0, 5_000, 10_000, 15_000, 20_000, 25_000, 30_000,
    ]);
  });

  it("reads each worker's busy share from its dataflows' growth", async () => {
    const replica = createFakeReplica();
    replica.dataflows.push({ dataflowId: "2", workerId: 0, elapsedNs: 0 });
    replica.onSecond = () => {
      replica.grow("1", [0.5, 0.1]);
      replica.grow("2", [0.4]);
    };
    const sampler = startSampler();

    await advance(30_000);

    const { readings } = sampler.getState();
    expect(
      workerBusyBetween(readings[0], readings[readings.length - 1]).map(
        ({ workerId, busyFraction }) => [
          workerId,
          Number(busyFraction.toFixed(2)),
        ],
      ),
    ).toEqual([
      [0, 0.9],
      [1, 0.1],
    ]);
  });

  it("doesn't fall when a dataflow is dropped between readings", async () => {
    const replica = createFakeReplica();
    replica.dataflows.push({ dataflowId: "2", workerId: 0, elapsedNs: 0 });
    replica.onSecond = () => replica.grow("2", [0.5]);
    const sampler = startSampler();
    await advance(10_000);
    const beforeDrop = sampler.getState().readings.at(-1);

    replica.dataflows = replica.dataflows.filter(
      ({ dataflowId }) => dataflowId !== "2",
    );
    await advance(5_000);

    expect(growthS(beforeDrop, sampler.getState().readings.at(-1))).toEqual([
      0, 0,
    ]);
  });

  it("times each reading by the replica's counters, and says how far they trail the clock", async () => {
    const replica = createFakeReplica();
    replica.lagMs = 20_000;
    const sampler = startSampler();

    await advance(10_000);

    expect(sinceStart(sampler.getState().readings)).toEqual([
      -20_000, -15_000, -10_000,
    ]);
    expect(sampler.getState().lagMs).toBe(20_000);
  });

  it("takes no reading while the replica's counters stand still", async () => {
    const replica = createFakeReplica();
    const sampler = startSampler();
    await advance(5_000);

    replica.isStalled = true;
    await advance(20_000);

    expect(sinceStart(sampler.getState().readings)).toEqual([0, 5_000]);
    expect(sampler.getState().lagMs).toBe(20_000);
  });

  it("says how long it has waited for a snapshot that hasn't landed", async () => {
    const replica = createFakeReplica();
    replica.holdsSnapshots = true;
    const sampler = startSampler();
    await advance(0);
    expect(sampler.getState()).toMatchObject({
      status: "reading",
      waitingMs: 0,
    });

    await advance(10_000);

    expect(sampler.getState()).toMatchObject({
      status: "reading",
      waitingMs: 10_000,
      readings: [],
    });
  });

  it("reads per-dataflow counters every 30 seconds", async () => {
    const replica = createFakeReplica();
    replica.dataflows = [
      { dataflowId: "7", workerId: 0, elapsedNs: SECOND_NS },
    ];
    replica.onSecond = () => replica.grow("7", [1]);
    const sampler = startSampler();

    await advance(59_000);

    const { objectReads } = sampler.getState();
    expect(sinceStart(objectReads)).toEqual([0, 30_000]);
    // Dataflow 7 exports nothing, so it counts toward no row.
    expect(objectReads.at(-1)).toEqual({
      atMs: START_MS + 30_000,
      rows: [{ dataflowId: "7", workerId: 0, elapsedNs: 31 * SECOND_NS }],
      exports: [],
      rowTotals: [],
      grownDataflows: [],
    });
  });

  it("reads per-dataflow counters every 30 seconds of the counters' time while they trail the clock", async () => {
    const replica = createFakeReplica();
    replica.lagMs = 40_000;
    const sampler = startSampler();

    await advance(25_000);
    expect(sampler.getState().objectReads.at(-1)?.atMs).toBe(START_MS - 40_000);

    await advance(5_000);
    expect(sampler.getState().objectReads.at(-1)?.atMs).toBe(START_MS - 10_000);
  });

  it("leaves its own subscribe out of the readings and the objects", async () => {
    const replica = createFakeReplica({ ownNsPerSecond: 0.5 * SECOND_NS });
    replica.exports = [{ objectId: "u1", dataflowId: "1" }];
    const sampler = startSampler();

    await advance(30_000);

    const { readings, objectReads } = sampler.getState();
    const latestRead = objectReads.at(-1);
    expect(growthS(readings[0], readings.at(-1))).toEqual([0, 0]);
    expect([
      ...new Set(latestRead?.rows.map(({ dataflowId }) => dataflowId)),
    ]).toEqual(["1"]);
    expect(latestRead?.exports).toEqual([{ objectId: "u1", dataflowId: "1" }]);
  });

  it("leaves an earlier session's dataflow out while it still runs after a quick resume", async () => {
    createFakeReplica({
      ownNsPerSecond: 0.5 * SECOND_NS,
      lingerMs: 3_000,
    });
    const sampler = startSampler();
    await advance(10_000);
    const beforePause = sampler.getState().readings.at(-1);

    sampler.pause();
    await advance(1_000);
    sampler.resume();
    await advance(0);

    const { readings } = sampler.getState();
    expect(readings.at(-1)).toMatchObject({
      atMs: START_MS + 11_000,
      afterGap: true,
    });
    expect(growthS(beforePause, readings.at(-1))).toEqual([0, 0]);
  });

  it("doesn't read a pause as a counter reset, though its own subscribe's time leaves the counters", async () => {
    const replica = createFakeReplica({ ownNsPerSecond: 0.5 * SECOND_NS });
    replica.onSecond = () => replica.grow("1", [0.01, 0]);
    const sampler = startSampler();
    await advance(10_000);
    const beforePause = sampler.getState().readings.at(-1);

    sampler.pause();
    await advance(5_000);
    sampler.resume();
    await advance(5_000);

    const afterPause = sampler.getState().readings.at(-1);
    expect(afterPause?.atMs).toBe(START_MS + 20_000);
    expect(
      hasCounterReset(
        beforePause?.elapsedNsByWorker ?? new Map(),
        afterPause?.elapsedNsByWorker ?? new Map(),
      ),
    ).toBe(false);
  });

  it("closes its session while paused, and marks the first reading after", async () => {
    const replica = createFakeReplica();
    const sampler = startSampler();
    await advance(5_000);

    sampler.pause();
    await advance(0);
    expect(replica.open()).toEqual([]);
    expect(sampler.getState()).toMatchObject({
      status: "paused",
      pauseReason: "user",
    });
    await advance(5_000);
    sampler.resume();
    await advance(0);

    expect(replica.open()).toHaveLength(1);
    expect(
      sampler
        .getState()
        .readings.map(({ atMs, afterGap }) => [atMs - START_MS, afterGap]),
    ).toEqual([
      [0, false],
      [5_000, false],
      [10_000, true],
    ]);
  });

  it("pauses itself after 10 minutes of reading and closes its session", async () => {
    const replica = createFakeReplica();
    const sampler = startSampler();

    await advance(SAMPLING_LIMIT_MS - 1_000);
    expect(sampler.getState().status).toBe("reading");
    await advance(1_000);

    expect(sampler.getState()).toMatchObject({
      status: "paused",
      pauseReason: "limit",
    });
    expect(replica.open()).toEqual([]);
    const readingCount = sampler.getState().readings.length;
    await advance(60_000);
    expect(sampler.getState().readings).toHaveLength(readingCount);
  });

  it("pauses for the limit when the subscribe reaches its bound first, without subscribing again", async () => {
    const replica = createFakeReplica();
    const sampler = startSampler();
    await advance(60_000);

    // As when this clock runs behind the replica's.
    replica.sessions[0].connection.complete();
    await advance(0);

    expect(sampler.getState()).toMatchObject({
      status: "paused",
      pauseReason: "limit",
    });
    expect(replica.open()).toEqual([]);
    await advance(10_000);
    expect(replica.sessions).toHaveLength(1);
  });

  it("starts a new bound when resumed", async () => {
    const replica = createFakeReplica();
    const sampler = startSampler();
    await advance(5_000);
    sampler.pause();
    await advance(60_000);

    sampler.resume();
    await advance(0);

    expect(replica.sessions[1].query).toContain(
      `UP TO ${START_MS + 65_000 + SAMPLING_LIMIT_MS}`,
    );
  });

  it("pauses while the browser tab is hidden, and resumes when it's shown", async () => {
    const replica = createFakeReplica();
    const sampler = startSampler();
    await advance(5_000);

    sampler.setHidden(true);
    await advance(0);
    expect(sampler.getState()).toMatchObject({
      status: "paused",
      pauseReason: "hidden",
    });
    expect(replica.open()).toEqual([]);

    await advance(30_000);
    sampler.setHidden(false);
    await advance(0);

    expect(sampler.getState().status).toBe("reading");
    expect(replica.open()).toHaveLength(1);
    expect(sampler.getState().readings.at(-1)).toMatchObject({
      atMs: START_MS + 35_000,
      afterGap: true,
    });
  });

  it("keeps a pause it didn't cause when the tab is shown again", async () => {
    const replica = createFakeReplica();
    const sampler = startSampler();
    await advance(5_000);
    sampler.pause();

    sampler.setHidden(true);
    sampler.setHidden(false);
    await advance(0);

    expect(sampler.getState()).toMatchObject({
      status: "paused",
      pauseReason: "user",
    });
    expect(replica.open()).toEqual([]);
  });

  it("stays idle until started, even when the tab is hidden and shown", async () => {
    const replica = createFakeReplica();
    const sampler = createSampler(createStores());
    sampler.setHidden(true);
    sampler.setHidden(false);
    await advance(10_000);

    expect(sampler.getState().status).toBe("idle");
    expect(replica.sessions).toEqual([]);

    sampler.start();
    sampler.start();
    await advance(0);

    expect(sampler.getState().status).toBe("reading");
    expect(replica.sessions).toHaveLength(1);
  });

  it("doesn't subscribe when it starts in a hidden tab", async () => {
    const replica = createFakeReplica();
    const sampler = createSampler(createStores());
    sampler.setHidden(true);
    sampler.start();
    await advance(10_000);

    expect(replica.sessions).toEqual([]);
    expect(sampler.getState()).toMatchObject({
      status: "paused",
      pauseReason: "hidden",
    });
  });

  it("keeps its readings when its session drops, and reads on a new session when resumed", async () => {
    const replica = createFakeReplica();
    const sampler = startSampler();
    await advance(5_000);

    replica.sessions[0].connection.close();
    await advance(0);
    expect(sampler.getState()).toMatchObject({
      status: "error",
      error: expect.stringMatching(/closed/i),
    });
    expect(sinceStart(sampler.getState().readings)).toEqual([0, 5_000]);

    await advance(1_000);
    sampler.resume();
    await advance(0);

    expect(replica.open()).toHaveLength(1);
    expect(sampler.getState()).toMatchObject({
      status: "reading",
      error: undefined,
    });
    expect(sinceStart(sampler.getState().readings)).toEqual([0, 5_000, 6_000]);
  });

  it("shows the error its subscribe fails with", async () => {
    createFakeReplica({ failing: true });
    const sampler = startSampler();
    await advance(0);

    expect(sampler.getState()).toMatchObject({
      status: "error",
      error: "unknown catalog item 'mz_scheduling_elapsed_raw'",
    });
  });

  it("keeps the per-dataflow reads of the last 3 minutes, oldest first, and stores them by time", async () => {
    createFakeReplica();
    const stores = createStores();
    const sampler = startSampler(stores);

    await advance(300_000);

    const { objectReads } = sampler.getState();
    expect(sinceStart(objectReads)).toEqual([
      120_000, 150_000, 180_000, 210_000, 240_000, 270_000, 300_000,
    ]);
    expect(stores.objectsStore.getRows()).toEqual(objectReads);
    expect(createSampler(stores).getState().objectReads).toEqual(objectReads);
  });

  it("doesn't measure across a dataflow's counter falling, and measures from the first read after it", async () => {
    const replica = createFakeReplica();
    replica.dataflows = [
      { dataflowId: "7", workerId: 0, elapsedNs: 100 * SECOND_NS },
    ];
    replica.exports = [{ objectId: "u7", dataflowId: "7" }];
    const sampler = startSampler();
    await advance(10_000);

    // As after a restart.
    replica.dataflows = [
      { dataflowId: "7", workerId: 0, elapsedNs: 40 * SECOND_NS },
    ];
    await advance(50_000);
    expect(sampler.getState()).toMatchObject({
      measurement: undefined,
      isMeasuring: true,
    });

    await advance(30_000);
    expect(spanSinceStart(sampler.getState().measurement)).toEqual([
      30_000, 90_000,
    ]);
  });

  it("measures the first minute of reads once there is one, and not before", async () => {
    createSteadyReplica();
    const stores = createStores();
    const sampler = startSampler(stores);
    await advance(59_000);
    expect(sampler.getState()).toMatchObject({
      measurement: undefined,
      isMeasuring: true,
    });

    await advance(1_000);

    const { measurement, isMeasuring } = sampler.getState();
    expect(isMeasuring).toBe(false);
    expect(spanSinceStart(measurement)).toEqual([0, 60_000]);
    expect(measurement?.workers).toEqual([
      { workerId: 0, busyFraction: 0.5 },
      { workerId: 1, busyFraction: 0.25 },
    ]);
    expect(
      measurement?.rows.map(({ objectId, perWorkerNs }) => [
        objectId,
        perWorkerNs.map((ns) => ns / SECOND_NS),
      ]),
    ).toEqual([["u1", [30, 15]]]);
    expect(stores.measurementStore.getRows()).toEqual([measurement]);
  });

  it("keeps a finished ad-hoc query's time in the queries row, up to the last reading that saw it", async () => {
    const replica = createFakeReplica();
    replica.dataflows = [
      { dataflowId: "9", workerId: 0, elapsedNs: 0 },
      { dataflowId: "9", workerId: 1, elapsedNs: 0 },
    ];
    replica.exports = [
      {
        objectId: "t9",
        dataflowId: "9",
        dataflowName: "Dataflow: oneshot-select-t9",
      },
    ];
    // The query's dataflow runs on worker 0 until it ends after 42 s, between
    // the readings at 40 and 45 s.
    let second = 0;
    replica.onSecond = () => {
      second += 1;
      if (second < 42) {
        replica.grow("9", [1, 0]);
      } else {
        replica.dataflows = [];
        replica.exports = [];
      }
    };
    const sampler = startSampler();

    await advance(60_000);

    const { measurement } = sampler.getState();
    expect(spanSinceStart(measurement)).toEqual([0, 60_000]);
    expect(
      measurement?.rows.map(({ objectId, dataflowCount, perWorkerNs }) => [
        objectId,
        dataflowCount,
        perWorkerNs.map((ns) => ns / SECOND_NS),
      ]),
    ).toEqual([[ADHOC_ROW_ID, 1, [40, 0]]]);
    expect(measurement?.workerTotalsNs.map((ns) => ns / SECOND_NS)).toEqual([
      40, 0,
    ]);
  });

  it("counts all of an object's time across a restart of its dataflow inside the span, though its counter ends above where it started", async () => {
    const replica = createFakeReplica();
    replica.dataflows = [
      { dataflowId: "9", workerId: 0, elapsedNs: 5 * SECOND_NS },
      { dataflowId: "9", workerId: 1, elapsedNs: 0 },
    ];
    replica.exports = [{ objectId: "u9", dataflowId: "9" }];
    // u9 keeps worker 0 busy throughout. Its dataflow restarts under the same
    // id after 25 s, so its counter falls from 30 s and ends at 35 s.
    let second = 0;
    replica.onSecond = () => {
      second += 1;
      if (second === 26) {
        replica.dataflows = replica.dataflows.map((row) => ({
          ...row,
          elapsedNs: 0,
        }));
      }
      replica.grow("9", [1, 0]);
    };
    const sampler = startSampler();

    await advance(60_000);

    const { measurement } = sampler.getState();
    expect(spanSinceStart(measurement)).toEqual([0, 60_000]);
    expect(
      measurement?.rows.map(({ objectId, perWorkerNs }) => [
        objectId,
        perWorkerNs.map((ns) => ns / SECOND_NS),
      ]),
    ).toEqual([["u9", [60, 0]]]);
  });

  it("records an annotation on its current measurement, which the next sampler restores", async () => {
    createSteadyReplica();
    const stores = createStores();
    const earlier = startSampler(stores);
    await advance(60_000);
    const endMs = earlier.getState().measurement?.endMs ?? NaN;

    earlier.annotateMeasurement(endMs - 1, { replicaCpu: { fraction: 0.9 } });
    expect(earlier.getState().measurement?.replicaCpu).toBeUndefined();
    earlier.annotateMeasurement(endMs, { replicaCpu: { fraction: 0.4 } });
    expect(earlier.getState().measurement?.replicaCpu).toEqual({
      fraction: 0.4,
    });
    earlier.stop();

    expect(createSampler(stores).getState().measurement).toMatchObject({
      endMs,
      replicaCpu: { fraction: 0.4 },
    });
  });

  it("keeps its measurement while later reads arrive", async () => {
    createSteadyReplica();
    const sampler = startSampler();
    await advance(60_000);
    const measurement = sampler.getState().measurement;
    expect(spanSinceStart(measurement)).toEqual([0, 60_000]);

    await advance(90_000);

    expect(sampler.getState().objectReads.at(-1)?.atMs).toBe(
      START_MS + 150_000,
    );
    expect(sampler.getState().measurement).toBe(measurement);
  });

  it("measures the minute from the next reading on when asked, keeping the current measurement until then", async () => {
    createSteadyReplica();
    const sampler = startSampler();
    await advance(72_000);

    sampler.measure();
    expect(sampler.getState()).toMatchObject({
      isMeasuring: true,
      measurement: { endMs: START_MS + 60_000 },
    });
    await advance(3_000);

    expect(sinceStart(sampler.getState().objectReads)).toEqual([
      0, 30_000, 60_000, 75_000,
    ]);
    expect(sampler.getState()).toMatchObject({
      isMeasuring: true,
      measuringFromMs: START_MS + 75_000,
      measurement: { endMs: START_MS + 60_000 },
    });

    await advance(59_000);
    expect(sampler.getState().measurement?.endMs).toBe(START_MS + 60_000);

    await advance(1_000);
    expect(sampler.getState().isMeasuring).toBe(false);
    expect(sampler.getState().measuringFromMs).toBeUndefined();
    expect(spanSinceStart(sampler.getState().measurement)).toEqual([
      75_000, 135_000,
    ]);
  });

  it("starts an asked-for minute and its countdown over at the first read after a pause", async () => {
    createSteadyReplica();
    const sampler = startSampler();
    await advance(72_000);
    sampler.measure();
    await advance(3_000);
    expect(sampler.getState().measuringFromMs).toBe(START_MS + 75_000);

    await advance(20_000);
    sampler.pause();
    expect(sampler.getState().measuringFromMs).toBeUndefined();
    await advance(30_000);
    sampler.resume();
    await advance(0);

    const resumedFromMs = sampler.getState().objectReads.at(-1)?.atMs;
    expect(resumedFromMs).toBeGreaterThanOrEqual(START_MS + 125_000);
    expect(sampler.getState()).toMatchObject({
      isMeasuring: true,
      measuringFromMs: resumedFromMs,
    });
  });

  it("opens a new session when asked to measure too close to the open one's bound for a minute to fit", async () => {
    const replica = createSteadyReplica();
    const sampler = startSampler();
    await advance(545_000);

    sampler.measure();
    await advance(120_000);

    const { measurement, isMeasuring, status } = sampler.getState();
    expect({ isMeasuring, status }).toEqual({
      isMeasuring: false,
      status: "reading",
    });
    expect(measurement?.startMs).toBeGreaterThanOrEqual(START_MS + 545_000);
    expect(replica.open()).toHaveLength(1);
    expect(replica.sessions).toHaveLength(2);
  });

  it("resumes when asked to measure while paused, and measures once there's a minute of fresh reads", async () => {
    const replica = createSteadyReplica();
    const sampler = startSampler();
    await advance(62_000);
    sampler.pause();
    await advance(300_000);

    sampler.measure();
    await advance(0);
    expect(replica.open()).toHaveLength(1);
    expect(sampler.getState()).toMatchObject({
      status: "reading",
      isMeasuring: true,
      measurement: { endMs: START_MS + 60_000 },
    });
    expect(sinceStart(sampler.getState().objectReads)).toEqual([362_000]);

    await advance(59_000);
    expect(sampler.getState().measurement?.endMs).toBe(START_MS + 60_000);

    await advance(1_000);
    expect(sampler.getState().isMeasuring).toBe(false);
    expect(spanSinceStart(sampler.getState().measurement)).toEqual([
      362_000, 422_000,
    ]);
  });

  it("marks the first reading's time in each stretch of reading", async () => {
    createFakeReplica();
    const sampler = startSampler();
    await advance(10_000);
    expect(sampler.getState().stretchStartMs).toBe(START_MS);

    sampler.pause();
    await advance(5_000);
    sampler.resume();
    expect(sampler.getState().stretchStartMs).toBeUndefined();
    await advance(10_000);

    expect(sampler.getState().stretchStartMs).toBe(START_MS + 15_000);
  });

  it("keeps an earlier sampler's measurement as it was, without measuring again until asked", async () => {
    createSteadyReplica();
    const stores = createStores();
    const earlier = startSampler(stores);
    await advance(62_000);
    const restored = earlier.getState().measurement;
    expect(spanSinceStart(restored)).toEqual([0, 60_000]);
    earlier.stop();
    await advance(600_000);

    const later = createSampler(stores);
    expect(later.getState()).toMatchObject({
      measurement: restored,
      isMeasuring: false,
    });
    later.start();
    await advance(120_000);
    expect(later.getState()).toMatchObject({
      measurement: restored,
      isMeasuring: false,
    });

    later.measure();
    await advance(65_000);
    expect(later.getState().isMeasuring).toBe(false);
    expect(later.getState().measurement).not.toBe(restored);
  });

  it("doesn't restore a measurement that ended over 30 minutes ago", async () => {
    createSteadyReplica();
    const stores = createStores();
    const earlier = startSampler(stores);
    await advance(60_000);
    earlier.stop();

    await advance(30 * 60_000);
    expect(createSampler(stores).getState().measurement?.endMs).toBe(
      START_MS + 60_000,
    );
    await advance(1);

    expect(createSampler(stores).getState()).toMatchObject({
      measurement: undefined,
      isMeasuring: true,
    });
  });

  it("continues from an earlier sampler's readings and marks its first reading as after a gap", async () => {
    createFakeReplica();
    const stores = createStores();
    const earlier = startSampler(stores);
    await advance(10_000);
    earlier.stop();
    await advance(60_000);

    const later = createSampler(stores);
    expect(sinceStart(later.getState().readings)).toEqual([0, 5_000, 10_000]);
    later.start();
    await advance(0);

    expect(later.getState().readings.at(-1)).toMatchObject({
      atMs: START_MS + 70_000,
      afterGap: true,
    });
  });

  it("carries the totals on from where an earlier sampler stopped, through a dataflow dropped in between", async () => {
    const replica = createFakeReplica();
    replica.dataflows.push({ dataflowId: "2", workerId: 0, elapsedNs: 0 });
    replica.onSecond = () => replica.grow("2", [0.5]);
    const stores = createStores();
    const earlier = startSampler(stores);
    await advance(10_000);
    const lastEarlier = earlier.getState().readings.at(-1);
    earlier.stop();

    replica.onSecond = undefined;
    replica.dataflows = replica.dataflows.filter(
      ({ dataflowId }) => dataflowId !== "2",
    );
    await advance(60_000);
    const later = startSampler(stores);
    await advance(0);

    expect(growthS(lastEarlier, later.getState().readings.at(-1))).toEqual([
      0, 0,
    ]);
  });

  it("carries each row's totals on from where an earlier sampler stopped, keeping a finished query's time", async () => {
    const replica = createFakeReplica();
    replica.dataflows = [
      { dataflowId: "9", workerId: 0, elapsedNs: 0 },
      { dataflowId: "9", workerId: 1, elapsedNs: 0 },
    ];
    replica.exports = [
      {
        objectId: "t9",
        dataflowId: "9",
        dataflowName: "Dataflow: oneshot-select-t9",
      },
    ];
    replica.onSecond = () => replica.grow("9", [1, 0]);
    const stores = createStores();
    const earlier = startSampler(stores);
    await advance(10_000);
    earlier.stop();

    // The query ends while no sampler reads.
    replica.onSecond = undefined;
    replica.dataflows = [];
    replica.exports = [];
    await advance(5_000);
    const later = startSampler(stores);
    // To its first object read, 30 s after the earlier sampler's.
    await advance(15_000);

    expect(sinceStart(later.getState().objectReads).at(-1)).toBe(30_000);
    expect(later.getState().objectReads.at(-1)?.rowTotals).toEqual([
      [ADHOC_ROW_ID, [10 * SECOND_NS, 0]],
    ]);
  });

  it("carries nothing on from a sampler of another worker count, as after a resize, even with no object's time to carry", async () => {
    // Dataflow 1 exports nothing, so the earlier sampler keeps no row totals.
    createFakeReplica();
    const stores = createStores();
    const earlier = startSampler(stores);
    await advance(62_000);
    expect(earlier.getState().measurement).toBeDefined();
    earlier.stop();

    expect(createSampler(stores, 4).getState()).toMatchObject({
      readings: [],
      objectReads: [],
      measurement: undefined,
      isMeasuring: true,
    });
  });

  it("starts each row's totals over for a sampler with a different worker count", async () => {
    createSteadyReplica();
    const stores = createStores();
    const earlier = startSampler(stores);
    await advance(10_000);
    earlier.stop();

    const later = createSampler(stores, 4);
    later.start();
    await advance(0);

    // u1's lifetime counters at 10 s, laid out for four workers.
    expect(sinceStart(later.getState().objectReads)).toEqual([10_000]);
    expect(later.getState().objectReads.at(-1)?.rowTotals).toEqual([
      ["u1", [15 * SECOND_NS, 12.5 * SECOND_NS, 0, 0]],
    ]);
  });

  it("measures when the counters advance in steps too long for a read a minute back", async () => {
    const replica = createSteadyReplica();
    replica.reportEverySeconds = 26;
    const sampler = startSampler();

    await advance(110_000);

    expect(spanSinceStart(sampler.getState().measurement)).toEqual([
      0, 104_000,
    ]);
  });

  it("ignores updates after stop and closes its session", async () => {
    const replica = createFakeReplica();
    const stores = createStores();
    const sampler = startSampler(stores);
    await advance(0);
    const stateAtStop = sampler.getState();

    sampler.stop();
    await advance(60_000);

    expect(sampler.getState()).toBe(stateAtStop);
    expect(sinceStart(stores.readingsStore.getRows())).toEqual([0]);
    expect(replica.open()).toEqual([]);
  });
});
