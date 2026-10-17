// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { CompiledQuery } from "kysely";

import {
  buildDataflowExportsQuery,
  buildDataflowWorkerCpuQuery,
  buildWorkerCpuQuery,
} from "~/api/materialize/cluster/workerCpu";
import { createSubscribeCollection } from "~/api/materialize/subscribeCollection";
import { WebsocketQueryClient } from "~/api/materialize/WebsocketQueryClient";

import { DataflowExport, DataflowWorkerCpu } from "./workerCpuModel";
import {
  createWorkerCpuSampler,
  SAMPLING_LIMIT_MS,
  StoredObjectRead,
  StoredWorkerReading,
} from "./workerCpuSampler";

const START_MS = Date.UTC(2026, 9, 16, 12);
const SECOND_NS = 1e9;

type QueryKind = "workers" | "dataflows" | "exports";

interface Gate {
  release: () => void;
}

/**
 * Answers the sampler's three queries from mutable counters. A query is
 * answered with the counters as they are when it completes, so a test can move
 * them while a held query is in flight.
 */
function createFakeReplica() {
  const kindBySql = new Map<string, QueryKind>([
    [buildWorkerCpuQuery().compile().sql, "workers"],
    [buildDataflowWorkerCpuQuery().compile().sql, "dataflows"],
    [buildDataflowExportsQuery().compile().sql, "exports"],
  ]);
  const gates = new Map<QueryKind, Promise<void>>();
  const replica = {
    /** Each worker's cumulative dataflow time, indexed by worker id. */
    workerNs: [10 * SECOND_NS, 10 * SECOND_NS],
    dataflows: [] as DataflowWorkerCpu[],
    exports: [] as DataflowExport[],
    queries: [] as QueryKind[],
    clients: [] as Array<{ closed: boolean; dropped: boolean }>,
    /** Holds the next query of `kind` until the returned gate is released. */
    holdNext: (kind: QueryKind): Gate => {
      let release = () => {};
      gates.set(
        kind,
        new Promise<void>((resolve) => {
          release = resolve;
        }),
      );
      return { release: () => release() };
    },
    /** Every open session fails its later queries, like a socket that closed. */
    dropConnections: () => {
      for (const client of replica.clients) client.dropped = true;
    },
    createClient: (): WebsocketQueryClient => {
      const client = { closed: false, dropped: false };
      replica.clients.push(client);
      return {
        query: async <T>(query: CompiledQuery<T>): Promise<T[]> => {
          const kind = kindBySql.get(query.sql);
          if (!kind) throw new Error(`Unexpected query: ${query.sql}`);
          replica.queries.push(kind);
          if (client.closed) throw new Error("Client closed");
          if (client.dropped) throw new Error("Connection closed");
          const gate = gates.get(kind);
          gates.delete(kind);
          await gate;
          const rows =
            kind === "workers"
              ? replica.workerNs.map((elapsedNs, workerId) => ({
                  workerId,
                  elapsedNs,
                }))
              : kind === "dataflows"
                ? replica.dataflows
                : replica.exports;
          return rows as T[];
        },
        close: () => {
          client.closed = true;
        },
      };
    },
  };
  return replica;
}

type FakeReplica = ReturnType<typeof createFakeReplica>;

let storeCount = 0;
const createStores = () => {
  storeCount += 1;
  return {
    readingsStore: createSubscribeCollection<StoredWorkerReading>({
      id: `worker-cpu-sampler-test-readings-${storeCount}`,
      getKey: (reading) => String(reading.atMs),
    }),
    objectsStore: createSubscribeCollection<StoredObjectRead>({
      id: `worker-cpu-sampler-test-objects-${storeCount}`,
      getKey: (read) => read.role,
    }),
  };
};

type Stores = ReturnType<typeof createStores>;

const samplers: Array<ReturnType<typeof createWorkerCpuSampler>> = [];

const createSampler = (replica: FakeReplica, stores: Stores) => {
  const sampler = createWorkerCpuSampler({
    createClient: replica.createClient,
    ...stores,
    now: () => Date.now(),
  });
  samplers.push(sampler);
  return sampler;
};

const startSampler = (replica: FakeReplica, stores = createStores()) => {
  const sampler = createSampler(replica, stores);
  sampler.start();
  return sampler;
};

/** Moves the clock, running due reads and every read's queries. */
const advance = (ms: number) => vi.advanceTimersByTimeAsync(ms);

const countQueries = (replica: FakeReplica, kind: QueryKind) =>
  replica.queries.filter((queried) => queried === kind).length;

const sinceStart = (readings: Array<{ atMs: number }>) =>
  readings.map(({ atMs }) => atMs - START_MS);

describe("createWorkerCpuSampler", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(START_MS);
  });

  afterEach(() => {
    for (const sampler of samplers.splice(0)) sampler.stop();
  });

  it("reads every worker's counters every 5 seconds", async () => {
    const sampler = startSampler(createFakeReplica());

    await advance(30_000);

    expect(sinceStart(sampler.getState().readings)).toEqual([
      0, 5_000, 10_000, 15_000, 20_000, 25_000, 30_000,
    ]);
  });

  it("reads per-dataflow counters at most every 30 seconds", async () => {
    const replica = createFakeReplica();
    const sampler = startSampler(replica);

    await advance(59_000);

    expect(countQueries(replica, "dataflows")).toBe(2);
    expect(sampler.getState().latestObjects?.atMs).toBe(START_MS + 30_000);
  });

  it("waits ten times as long as a slow read took before reading again", async () => {
    const replica = createFakeReplica();
    const slowRead = replica.holdNext("workers");
    const sampler = startSampler(replica);
    await advance(2_000);
    slowRead.release();
    await advance(0);

    expect(sampler.getState().intervalMs).toBe(20_000);
    await advance(19_999);
    expect(sampler.getState().readings).toHaveLength(1);
    await advance(1);
    expect(sampler.getState().readings).toHaveLength(2);
  });

  it("keeps the 5 second cadence after a read under half a second", async () => {
    const replica = createFakeReplica();
    const read = replica.holdNext("workers");
    const sampler = startSampler(replica);
    await advance(300);
    read.release();
    await advance(0);

    expect(sampler.getState().intervalMs).toBe(5_000);
  });

  it("pauses itself after 10 minutes of reading and closes its session", async () => {
    const replica = createFakeReplica();
    const sampler = startSampler(replica);

    await advance(SAMPLING_LIMIT_MS - 1);
    expect(sampler.getState().status).toBe("reading");
    await advance(1);

    expect(sampler.getState()).toMatchObject({
      status: "paused",
      pausedForLimit: true,
    });
    expect(replica.clients.map(({ closed }) => closed)).toEqual([true]);
    const readingCount = sampler.getState().readings.length;
    await advance(60_000);
    expect(sampler.getState().readings).toHaveLength(readingCount);
  });

  it("keeps one read cadence when paused and resumed while a read is in flight", async () => {
    const replica = createFakeReplica();
    const sampler = startSampler(replica);
    await advance(0);
    const inFlight = replica.holdNext("workers");
    await advance(5_000);

    sampler.pause();
    sampler.resume();
    inFlight.release();
    const workerReadsBefore = countQueries(replica, "workers");
    await advance(30_000);

    expect(countQueries(replica, "workers") - workerReadsBefore).toBe(6);
  });

  it("does not mark a read in flight across a pause and resume as after a gap", async () => {
    const replica = createFakeReplica();
    const sampler = startSampler(replica);
    await advance(0);
    const inFlight = replica.holdNext("workers");
    await advance(5_000);

    sampler.pause();
    sampler.resume();
    inFlight.release();
    await advance(0);

    expect(sampler.getState().readings.at(-1)).toMatchObject({
      atMs: START_MS + 5_000,
      afterGap: false,
    });

    await advance(5_000);
    expect(sampler.getState().readings.at(-1)).toMatchObject({
      atMs: START_MS + 10_000,
      afterGap: false,
    });
  });

  it("reads again on a new session when resumed after a read failed", async () => {
    const replica = createFakeReplica();
    const sampler = startSampler(replica);
    await advance(0);
    replica.dropConnections();
    await advance(5_000);
    expect(sampler.getState()).toMatchObject({
      status: "error",
      error: "Connection closed",
    });

    sampler.resume();
    await advance(0);

    expect(replica.clients).toHaveLength(2);
    expect(sampler.getState()).toMatchObject({
      status: "reading",
      error: undefined,
    });
    expect(sinceStart(sampler.getState().readings)).toEqual([0, 5_000]);
  });

  it("marks the reading after a worker's counter fell as after a reset", async () => {
    const replica = createFakeReplica();
    replica.workerNs = [100 * SECOND_NS, 100 * SECOND_NS];
    const sampler = startSampler(replica);
    await advance(0);

    replica.workerNs = [40 * SECOND_NS, 101 * SECOND_NS];
    await advance(5_000);
    replica.workerNs = [41 * SECOND_NS, 102 * SECOND_NS];
    await advance(5_000);

    expect(
      sampler.getState().readings.map(({ afterReset }) => afterReset),
    ).toEqual([false, true, false]);
  });

  it("keeps the first per-dataflow read as the baseline while counters grow", async () => {
    const replica = createFakeReplica();
    replica.dataflows = [
      { dataflowId: "7", workerId: 0, elapsedNs: 100 * SECOND_NS },
    ];
    const stores = createStores();
    const sampler = startSampler(replica, stores);
    await advance(0);

    replica.dataflows = [
      { dataflowId: "7", workerId: 0, elapsedNs: 130 * SECOND_NS },
    ];
    await advance(30_000);

    expect(sampler.getState().firstObjects?.atMs).toBe(START_MS);
    expect(
      stores.objectsStore.getRows().find(({ role }) => role === "first")?.atMs,
    ).toBe(START_MS);
  });

  it("rebases the first per-dataflow read when a dataflow's counter falls", async () => {
    const replica = createFakeReplica();
    replica.dataflows = [
      { dataflowId: "7", workerId: 0, elapsedNs: 100 * SECOND_NS },
    ];
    const stores = createStores();
    const sampler = startSampler(replica, stores);
    await advance(0);

    const afterRestart = [
      { dataflowId: "7", workerId: 0, elapsedNs: 40 * SECOND_NS },
    ];
    replica.dataflows = afterRestart;
    await advance(30_000);

    expect(sampler.getState().firstObjects).toEqual({
      atMs: START_MS + 30_000,
      rows: afterRestart,
    });
    expect(
      stores.objectsStore.getRows().find(({ role }) => role === "first")?.atMs,
    ).toBe(START_MS + 30_000);
  });

  it("continues from an earlier sampler's readings and marks its first reading as after a gap", async () => {
    const replica = createFakeReplica();
    const stores = createStores();
    const earlier = startSampler(replica, stores);
    await advance(10_000);
    earlier.stop();
    await advance(60_000);

    const later = createSampler(replica, stores);
    expect(sinceStart(later.getState().readings)).toEqual([0, 5_000, 10_000]);
    later.start();
    await advance(0);

    expect(later.getState().readings.at(-1)).toMatchObject({
      atMs: START_MS + 70_000,
      afterGap: true,
    });
  });

  it("ignores a read that lands after stop and closes its session", async () => {
    const replica = createFakeReplica();
    const stores = createStores();
    const sampler = startSampler(replica, stores);
    await advance(0);
    const late = replica.holdNext("workers");
    await advance(5_000);
    const stateAtStop = sampler.getState();

    sampler.stop();
    late.release();
    await advance(60_000);

    expect(sampler.getState()).toBe(stateAtStop);
    expect(sinceStart(stores.readingsStore.getRows())).toEqual([0]);
    expect(replica.clients.map(({ closed }) => closed)).toEqual([true]);
  });
});
