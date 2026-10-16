// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { millisecondsInMinute } from "date-fns/constants";
import { useAtomValue } from "jotai";
import React from "react";

import {
  buildDataflowExportsQuery,
  buildDataflowWorkerCpuQuery,
  buildWorkerCpuQuery,
} from "~/api/materialize/cluster/workerCpu";
import {
  createScopedSubscribeCollections,
  SubscribeCollection,
} from "~/api/materialize/subscribeCollection";
import {
  createWebsocketQueryClient,
  WebsocketQueryClient,
} from "~/api/materialize/WebsocketQueryClient";
import { useCurrentEnvironmentHttpAddress } from "~/store/environments";
import { syncEngineCacheScopeLoadableAtom } from "~/store/syncEngineCache";

import {
  DataflowExport,
  DataflowWorkerCpu,
  hasCounterReset,
  hasDataflowCounterReset,
  WorkerCpuReading,
} from "./workerCpuModel";

export const READ_INTERVAL_MS = 5_000;
const OBJECT_READ_INTERVAL_MS = 30_000;
export const SAMPLING_LIMIT_MS = 10 * millisecondsInMinute;
// The next read waits at least this many times as long as the last one took.
// A replica that answers slowly is usually a busy one, and these reads run on
// it.
const READ_BACKOFF_FACTOR = 10;
const READ_TIMEOUT = "10s";
/** How long readings stay, so reopening the drawer continues from them. */
const READINGS_RETENTION_MS = 30 * millisecondsInMinute;

/** A reading as the collection holds it, in JSON-friendly form. */
export interface StoredWorkerReading {
  atMs: number;
  afterGap: boolean;
  afterReset: boolean;
  elapsedNs: Array<[number, number]>;
}

export interface StoredObjectRead {
  role: "first" | "latest";
  atMs: number;
  rows: DataflowWorkerCpu[];
  exports: DataflowExport[];
}

// Per tenant scope and replica, in memory only: an organization or region
// switch drops them.
const readingsByReplica = createScopedSubscribeCollections<StoredWorkerReading>(
  { id: "worker-cpu-readings", getKey: (reading) => String(reading.atMs) },
);
const objectReadsByReplica = createScopedSubscribeCollections<StoredObjectRead>(
  {
    id: "worker-cpu-object-reads",
    getKey: (read) => read.role,
  },
);

const toReading = ({
  atMs,
  afterGap,
  afterReset,
  elapsedNs,
}: StoredWorkerReading): WorkerCpuReading => ({
  atMs,
  afterGap,
  afterReset,
  elapsedNsByWorker: new Map(elapsedNs),
});

const toStored = ({
  atMs,
  afterGap,
  afterReset,
  elapsedNsByWorker,
}: WorkerCpuReading): StoredWorkerReading => ({
  atMs,
  afterGap: Boolean(afterGap),
  afterReset: Boolean(afterReset),
  elapsedNs: [...elapsedNsByWorker],
});

const applyRows = <T extends object>(
  collection: SubscribeCollection<T>,
  data: T[],
) =>
  collection.applySnapshot({ data, snapshotComplete: true, error: undefined });

interface ObjectCpuRead {
  atMs: number;
  rows: DataflowWorkerCpu[];
}

export interface WorkerCpuState {
  status: "reading" | "paused" | "error";
  /** Paused by `SAMPLING_LIMIT_MS` rather than by the user. */
  pausedForLimit: boolean;
  readings: WorkerCpuReading[];
  /** The per-object read when sampling started, and the latest one. */
  firstObjects?: ObjectCpuRead;
  latestObjects?: ObjectCpuRead;
  exports: DataflowExport[];
  /** How long the next read waits. */
  intervalMs: number;
  error?: string;
}

const INITIAL_WORKER_CPU_STATE: WorkerCpuState = {
  status: "reading",
  pausedForLimit: false,
  readings: [],
  exports: [],
  intervalMs: READ_INTERVAL_MS,
};

interface WorkerCpuSampler {
  getState: () => WorkerCpuState;
  subscribe: (listener: () => void) => () => void;
  start: () => void;
  pause: () => void;
  resume: () => void;
  stop: () => void;
}

/**
 * Reads a replica's per-worker CPU counters on a timer, and its per-object
 * counters on a slower one, until paused or stopped. It starts from, and keeps
 * writing to, the readings `readingsStore` and `objectsStore` hold from an
 * earlier session. It holds a session from `createClient` only while reading.
 */
export function createWorkerCpuSampler({
  createClient,
  readingsStore,
  objectsStore,
  now = Date.now,
}: {
  createClient: () => WebsocketQueryClient;
  readingsStore: SubscribeCollection<StoredWorkerReading>;
  objectsStore: SubscribeCollection<StoredObjectRead>;
  now?: () => number;
}): WorkerCpuSampler {
  const isRetained = (atMs: number) => now() - atMs <= READINGS_RETENTION_MS;
  const priorReadings = readingsStore
    .getRows()
    .filter(({ atMs }) => isRetained(atMs))
    .sort((a, b) => a.atMs - b.atMs)
    .map(toReading);
  const priorObjects = objectsStore
    .getRows()
    .filter(({ atMs }) => isRetained(atMs));
  const priorFirst = priorObjects.find(({ role }) => role === "first");
  const priorLatest = priorObjects.find(({ role }) => role === "latest");
  let state: WorkerCpuState = {
    ...INITIAL_WORKER_CPU_STATE,
    readings: priorReadings,
    firstObjects: priorFirst && {
      atMs: priorFirst.atMs,
      rows: priorFirst.rows,
    },
    latestObjects: priorLatest && {
      atMs: priorLatest.atMs,
      rows: priorLatest.rows,
    },
    exports: priorLatest?.exports ?? [],
  };
  const listeners = new Set<() => void>();
  let client: WebsocketQueryClient | undefined;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let stopped = false;
  // At most one read runs, so resuming during a read doesn't start a second
  // timer chain.
  let isReading = false;
  let limitStartMs = 0;
  let afterGap = priorReadings.length > 0;

  const setState = (update: Partial<WorkerCpuState>) => {
    state = { ...state, ...update };
    for (const listener of listeners) listener();
  };

  // A closed session can't recover, so each stretch of reading opens its own.
  const releaseClient = () => {
    client?.close();
    client = undefined;
  };

  const readObjects = async (
    session: WebsocketQueryClient,
  ): Promise<Partial<WorkerCpuState>> => {
    const [rows, exports] = await Promise.all([
      session.query(buildDataflowWorkerCpuQuery().compile()),
      session.query(buildDataflowExportsQuery().compile()),
    ]);
    if (stopped) return {};
    const read = { atMs: now(), rows };
    const first =
      state.firstObjects &&
      !hasDataflowCounterReset(state.firstObjects.rows, rows)
        ? state.firstObjects
        : read;
    applyRows<StoredObjectRead>(objectsStore, [
      { role: "first", ...first, exports },
      { role: "latest", ...read, exports },
    ]);
    return { firstObjects: first, latestObjects: read, exports };
  };

  const addReading = (reading: WorkerCpuReading) => {
    const readings = [...state.readings, reading].filter(({ atMs }) =>
      isRetained(atMs),
    );
    applyRows(readingsStore, readings.map(toStored));
    return readings;
  };

  const tick = async () => {
    timer = undefined;
    if (stopped || isReading || state.status !== "reading") return;
    isReading = true;
    const isAfterGap = afterGap;
    afterGap = false;
    // Applied with the worker reading, so one tick renders once.
    let objects: Partial<WorkerCpuState> = {};
    try {
      const session = (client ??= createClient());
      const startedMs = now();
      if (
        !state.latestObjects ||
        startedMs - state.latestObjects.atMs >= OBJECT_READ_INTERVAL_MS
      ) {
        objects = await readObjects(session);
      }
      if (stopped) return;
      const workers = await session.query(buildWorkerCpuQuery().compile());
      if (stopped) return;
      const atMs = now();
      const previous = state.readings.at(-1);
      const elapsedNsByWorker = new Map(
        workers.map(({ workerId, elapsedNs }) => [workerId, elapsedNs]),
      );
      const reading: WorkerCpuReading = {
        atMs,
        elapsedNsByWorker,
        afterGap: isAfterGap,
        afterReset:
          previous !== undefined &&
          hasCounterReset(previous.elapsedNsByWorker, elapsedNsByWorker),
      };
      const intervalMs = Math.max(
        READ_INTERVAL_MS,
        READ_BACKOFF_FACTOR * (atMs - startedMs),
      );
      const readings = addReading(reading);
      if (state.status !== "reading") {
        // Paused while this read ran.
        setState({ ...objects, readings, intervalMs });
        releaseClient();
        return;
      }
      if (atMs - limitStartMs >= SAMPLING_LIMIT_MS) {
        afterGap = true;
        setState({
          ...objects,
          readings,
          intervalMs,
          status: "paused",
          pausedForLimit: true,
        });
        releaseClient();
        return;
      }
      setState({ ...objects, readings, intervalMs });
      timer = setTimeout(() => void tick(), intervalMs);
    } catch (error) {
      if (stopped) return;
      afterGap = true;
      releaseClient();
      setState({
        ...objects,
        status: "error",
        error: error instanceof Error ? error.message : String(error),
      });
    } finally {
      isReading = false;
    }
  };

  return {
    getState: () => state,
    subscribe: (listener) => {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    start: () => {
      limitStartMs = now();
      void tick();
    },
    pause: () => {
      if (state.status !== "reading") return;
      clearTimeout(timer);
      timer = undefined;
      afterGap = true;
      setState({ status: "paused", pausedForLimit: false });
      // A read in flight releases the session when it lands.
      if (!isReading) releaseClient();
    },
    resume: () => {
      if (stopped || state.status === "reading") return;
      // A read still in flight carries on the chain, so nothing was missed.
      if (isReading) afterGap = false;
      limitStartMs = now();
      setState({ status: "reading", pausedForLimit: false, error: undefined });
      void tick();
    },
    stop: () => {
      stopped = true;
      clearTimeout(timer);
      releaseClient();
    },
  };
}

const noopSubscribe = () => () => {};

/**
 * Samples one replica's worker CPU while the calling component is mounted,
 * continuing from the readings of an earlier session in the last
 * `READINGS_RETENTION_MS`.
 */
export function useWorkerCpuSampler({
  clusterName,
  replicaId,
  replicaName,
}: {
  clusterName: string;
  replicaId: string;
  replicaName: string;
}) {
  const httpAddress = useCurrentEnvironmentHttpAddress();
  const scope = useAtomValue(syncEngineCacheScopeLoadableAtom);
  const scopeKey =
    scope.state === "hasData" && scope.data ? scope.data : undefined;
  const [sampler, setSampler] = React.useState<WorkerCpuSampler>();
  React.useEffect(() => {
    if (!httpAddress || !scopeKey) return;
    const next = createWorkerCpuSampler({
      createClient: () =>
        createWebsocketQueryClient({
          httpAddress,
          sessionVariables: {
            cluster: clusterName,
            cluster_replica: replicaName,
            statement_timeout: READ_TIMEOUT,
          },
        }),
      readingsStore: readingsByReplica.get(scopeKey, replicaId),
      objectsStore: objectReadsByReplica.get(scopeKey, replicaId),
    });
    setSampler(next);
    next.start();
    return () => {
      next.stop();
      setSampler(undefined);
    };
  }, [httpAddress, scopeKey, clusterName, replicaId, replicaName]);
  const state = React.useSyncExternalStore(
    sampler?.subscribe ?? noopSubscribe,
    () => sampler?.getState() ?? INITIAL_WORKER_CPU_STATE,
  );
  return {
    state:
      scope.state === "hasError"
        ? { ...state, status: "error" as const, error: "Couldn't load." }
        : state,
    pause: sampler?.pause,
    resume: sampler?.resume,
  };
}
