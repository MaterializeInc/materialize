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

import { queryBuilder } from "~/api/materialize";
import {
  buildWorkerCpuSubscribe,
  splitWorkerCpuRows,
  WorkerCpuRow,
  workerCpuRowKey,
} from "~/api/materialize/cluster/workerCpu";
import {
  createScopedSubscribeCollections,
  SubscribeCollection,
} from "~/api/materialize/subscribeCollection";
import { SubscribeManager } from "~/api/materialize/SubscribeManager";
import { SessionVariables } from "~/api/materialize/types";
import { useCurrentEnvironmentHttpAddress } from "~/store/environments";
import { syncEngineCacheScopeLoadableAtom } from "~/store/syncEngineCache";

import {
  accumulateRowElapsedNs,
  accumulateWorkerElapsedNs,
  DataflowCounters,
  dataflowCounters,
  DataflowExport,
  DataflowRowIds,
  dataflowRowIds,
  MAX_MEASUREMENT_MS,
  measureLatest,
  Measurement,
  MEASUREMENT_MS,
  ObjectCpuRead,
  ownDataflows,
  OwnExport,
  RowTotals,
  WorkerCpuReading,
} from "./workerCpuModel";

export const READ_INTERVAL_MS = 5_000;
const OBJECT_READ_INTERVAL_MS = 30_000;
// A measurement needs a read at most `MAX_MEASUREMENT_MS` before the last.
const OBJECT_READS_RETENTION_MS = MAX_MEASUREMENT_MS;
export const SAMPLING_LIMIT_MS = 10 * millisecondsInMinute;
/** Counters trailing the clock by more than this are called out. */
export const LAG_NOTE_MS = 10_000;
/** How long readings stay, so returning to the tab continues from them. */
const READINGS_RETENTION_MS = 30 * millisecondsInMinute;

/** A reading as the collection holds it, in JSON-friendly form. */
export interface StoredWorkerReading {
  atMs: number;
  afterGap: boolean;
  elapsedNs: Array<[number, number]>;
}

/** Where a stopped sampler left off, so the next one carries on from it. */
export interface StoredContinuation {
  /** The last reading's time. */
  atMs: number;
  counters: Array<[string, number]>;
  ownExports: OwnExport[];
  rowTotals: Array<[string, number[]]>;
  rowIds: Array<[string, string]>;
  /** The dataflows that grew since the last object read, by row. */
  grownSinceRead: Array<[string, string[]]>;
}

// Per tenant scope and replica, in memory only: an organization or region
// switch drops them.
const readingsByReplica = createScopedSubscribeCollections<StoredWorkerReading>(
  { id: "worker-cpu-readings", getKey: (reading) => String(reading.atMs) },
);
const objectReadsByReplica = createScopedSubscribeCollections<ObjectCpuRead>({
  id: "worker-cpu-object-reads",
  getKey: (read) => String(read.atMs),
});
const measurementByReplica = createScopedSubscribeCollections<Measurement>({
  id: "worker-cpu-measurement",
  getKey: () => "last",
});
const continuationByReplica =
  createScopedSubscribeCollections<StoredContinuation>({
    id: "worker-cpu-continuation",
    getKey: () => "last",
  });

const toReading = ({
  atMs,
  afterGap,
  elapsedNs,
}: StoredWorkerReading): WorkerCpuReading => ({
  atMs,
  afterGap,
  elapsedNsByWorker: new Map(elapsedNs),
});

const toStored = ({
  atMs,
  afterGap,
  elapsedNsByWorker,
}: WorkerCpuReading): StoredWorkerReading => ({
  atMs,
  afterGap: Boolean(afterGap),
  elapsedNs: [...elapsedNsByWorker],
});

const applyRows = <T extends object>(
  collection: SubscribeCollection<T>,
  data: T[],
) =>
  collection.applySnapshot({ data, snapshotComplete: true, error: undefined });

/** Why reading paused: a click, `SAMPLING_LIMIT_MS`, or a hidden browser tab. */
export type PauseReason = "user" | "limit" | "hidden";

export interface WorkerCpuState {
  status: "reading" | "paused" | "error";
  pauseReason?: PauseReason;
  readings: WorkerCpuReading[];
  /** Per-object reads, oldest first, back `OBJECT_READS_RETENTION_MS`. */
  objectReads: ObjectCpuRead[];
  /** The span the report shows, until the next is taken. */
  measurement?: Measurement;
  /** Whether a measurement is due: the first, or one asked for. */
  isMeasuring: boolean;
  /** When an asked-for measurement started, once its first read is taken. */
  measuringFromMs?: number;
  /** The first reading's time in this stretch of reading. */
  stretchStartMs?: number;
  /** How far the replica's latest counters trail the clock. */
  lagMs?: number;
  /**
   * How long this stretch of reading has waited for the replica's first
   * counters, until they arrive.
   */
  waitingMs?: number;
  error?: string;
}

const INITIAL_WORKER_CPU_STATE: WorkerCpuState = {
  status: "reading",
  readings: [],
  objectReads: [],
  isMeasuring: true,
};

interface WorkerCpuSampler {
  getState: () => WorkerCpuState;
  subscribe: (listener: () => void) => () => void;
  start: () => void;
  pause: () => void;
  resume: () => void;
  /**
   * Measures the minute from the next reading on, resuming first if paused.
   * The current measurement shows until the new one is taken.
   */
  measure: () => void;
  /**
   * Records what the report shows beside the measurement ending at `endMs`,
   * if it's still the current one. A restored measurement keeps it.
   */
  annotateMeasurement: (
    endMs: number,
    annotation: Pick<Measurement, "replicaCpu" | "lag">,
  ) => void;
  /** Pauses while `hidden`, and resumes a pause it caused once shown. */
  setHidden: (hidden: boolean) => void;
  stop: () => void;
}

interface SessionTarget {
  httpAddress: string;
  sessionVariables: SessionVariables;
}

/** One stretch of reading, on one session. */
interface LiveRead {
  manager: SubscribeManager<WorkerCpuRow, WorkerCpuRow>;
  openedAtMs: number;
  /** The subscribe's `UP TO`. */
  upToMs: number;
  close: () => void;
}

/** The last reading, with the counters the next one grows from. */
interface Previous {
  atMs: number;
  elapsedNsByWorker: Map<number, number>;
  counters: DataflowCounters;
  rowTotals: RowTotals;
  rowIds: DataflowRowIds;
}

const toGrownMap = (grown: Array<[string, string[]]>) =>
  new Map(grown.map(([rowId, ids]) => [rowId, new Set(ids)]));

const fromGrownMap = (grown: Map<string, Set<string>>) =>
  [...grown].map(([rowId, ids]): [string, string[]] => [rowId, [...ids]]);

const openLiveRead = (
  { httpAddress, sessionVariables }: SessionTarget,
  openedAtMs: number,
  onChange: (read: LiveRead) => void,
): LiveRead => {
  const upToMs = openedAtMs + SAMPLING_LIMIT_MS;
  const { sql, parameters } =
    buildWorkerCpuSubscribe(upToMs).compile(queryBuilder);
  const manager = new SubscribeManager<WorkerCpuRow, WorkerCpuRow>({
    httpAddress,
    request: { queries: [{ query: sql, params: parameters as string[] }] },
    upsert: { key: (row) => workerCpuRowKey(row.data) },
    select: (row) => row.data,
    // Without it, the manager sends the subscribe again once it ends at its
    // `UP TO`, and an `AS OF` past that bound fails it
    // (`AbsurdSubscribeBounds` in `src/adapter`).
    closeSocketOnComplete: true,
  });
  // NOTE: connected directly, not through `createSubscribeSession`. That
  // builds its WebsocketConnectionManager without `getSessionVariables`, so
  // its reconnects would run this on `mz_catalog_server`. Nothing reconnects
  // this session: a dropped one reads as an error until resumed.
  manager.connect(undefined, httpAddress, sessionVariables);
  let unsubscribe = () => {};
  const read: LiveRead = {
    manager,
    openedAtMs,
    upToMs,
    close: () => {
      unsubscribe();
      manager.disconnect();
    },
  };
  unsubscribe = manager.onChange(() => onChange(read));
  return read;
};

/**
 * Samples a replica's per-worker CPU on a timer, and its per-object CPU on a
 * slower one, from one subscribe it holds open while reading, until paused or
 * stopped. Its own subscribes' dataflows are left out of both. It measures a
 * span once there's a minute of readings, and again when asked. It starts
 * from, and keeps writing to, what the stores hold from an earlier sampler.
 */
export function createWorkerCpuSampler({
  httpAddress,
  sessionVariables,
  workerCount,
  readingsStore,
  objectsStore,
  continuationStore,
  measurementStore,
  now = Date.now,
}: SessionTarget & {
  workerCount: number;
  readingsStore: SubscribeCollection<StoredWorkerReading>;
  objectsStore: SubscribeCollection<ObjectCpuRead>;
  continuationStore: SubscribeCollection<StoredContinuation>;
  measurementStore: SubscribeCollection<Measurement>;
  now?: () => number;
}): WorkerCpuSampler {
  const isRetained = (atMs: number) => now() - atMs <= READINGS_RETENTION_MS;
  const storedReadings = readingsStore
    .getRows()
    .filter(({ atMs }) => isRetained(atMs))
    .sort((a, b) => a.atMs - b.atMs)
    .map(toReading);
  // Every dataflow runs on every worker, so a reading holds each worker of
  // the size it was taken on. One from a size with other workers, as before a
  // resize, would mix their counters into these readings, so nothing an
  // earlier sampler kept carries on then.
  const isThisSize = storedReadings.every(
    ({ elapsedNsByWorker }) =>
      elapsedNsByWorker.size === workerCount &&
      [...elapsedNsByWorker.keys()].every((workerId) => workerId < workerCount),
  );
  const priorReadings = isThisSize ? storedReadings : [];
  const priorObjects = isThisSize
    ? objectsStore
        .getRows()
        .filter(({ atMs }) => isRetained(atMs))
        .sort((a, b) => a.atMs - b.atMs)
    : [];
  const priorMeasurement = isThisSize
    ? measurementStore
        .getRows()
        .find(
          ({ endMs, workerTotalsNs }) =>
            isRetained(endMs) && workerTotalsNs.length === workerCount,
        )
    : undefined;
  const continuation = continuationStore.getRows().at(0);
  const lastPrior = priorReadings.at(-1);
  let state: WorkerCpuState = {
    ...INITIAL_WORKER_CPU_STATE,
    readings: priorReadings,
    objectReads: priorObjects,
    // Coming back to the tab shows the last report as it was, until Measure
    // again.
    measurement: priorMeasurement,
    isMeasuring: !priorMeasurement,
  };
  // Carried on only from the reading it was left at. Otherwise readings start
  // over from the running dataflows' lifetimes.
  const continues =
    continuation && lastPrior && continuation.atMs === lastPrior.atMs;
  let previous: Previous | undefined = continues
    ? {
        atMs: lastPrior.atMs,
        elapsedNsByWorker: lastPrior.elapsedNsByWorker,
        counters: new Map(continuation.counters),
        rowTotals: new Map(continuation.rowTotals),
        rowIds: new Map(continuation.rowIds),
      }
    : undefined;
  let grownSinceRead = continues
    ? toGrownMap(continuation.grownSinceRead)
    : new Map<string, Set<string>>();
  let ownExports = continuation?.ownExports ?? [];
  const listeners = new Set<() => void>();
  let live: LiveRead | undefined;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let stopped = false;
  let isHidden = false;
  let afterGap = priorReadings.length > 0;
  // The time a measurement that's due must start after.
  let measureAfterMs: number | undefined;
  let isObjectReadDue = false;
  let isStretchStarted = false;

  const setState = (update: Partial<WorkerCpuState>) => {
    state = { ...state, ...update };
    for (const listener of listeners) listener();
  };

  const close = () => {
    clearTimeout(timer);
    timer = undefined;
    live?.close();
    live = undefined;
  };

  const pauseFor = (
    pauseReason: PauseReason,
    update: Partial<WorkerCpuState> = {},
  ) => {
    afterGap = true;
    close();
    setState({
      ...update,
      status: "paused",
      pauseReason,
      waitingMs: undefined,
    });
  };

  const fail = (message: string) => {
    afterGap = true;
    close();
    setState({ status: "error", error: message, waitingMs: undefined });
  };

  const readObjects = (
    read: ObjectCpuRead,
    readings: WorkerCpuReading[],
  ): Partial<WorkerCpuState> => {
    const startsMeasurement = isObjectReadDue;
    isObjectReadDue = false;
    const objectReads = [...state.objectReads, read].filter(
      ({ atMs }) => read.atMs - atMs <= OBJECT_READS_RETENTION_MS,
    );
    applyRows(objectsStore, objectReads);
    const update: Partial<WorkerCpuState> = {
      objectReads,
      ...(startsMeasurement && { measuringFromMs: read.atMs }),
    };
    if (state.measurement && measureAfterMs === undefined) return update;
    const measurement = measureLatest({
      reads: objectReads,
      readings,
      workerCount,
      startsFromMs: measureAfterMs === undefined ? 0 : measureAfterMs + 1,
    });
    if (!measurement) return update;
    measureAfterMs = undefined;
    applyRows(measurementStore, [measurement]);
    return {
      ...update,
      measurement,
      isMeasuring: false,
      measuringFromMs: undefined,
    };
  };

  const addReading = (reading: WorkerCpuReading) => {
    const readings = [...state.readings, reading].filter(({ atMs }) =>
      isRetained(atMs),
    );
    applyRows(readingsStore, readings.map(toStored));
    return readings;
  };

  /** A reading at `atMs` of what `read` holds, without its own dataflows. */
  const takeReading = (read: LiveRead, atMs: number) => {
    const { dataflows, exports } = splitWorkerCpuRows(
      read.manager.getSnapshot().data,
    );
    const own = ownDataflows({
      dataflows,
      exports,
      connectionId: read.manager.getConnectionId(),
      remembered: ownExports,
    });
    ownExports = own.remembered;
    const rows = dataflows.filter(
      ({ dataflowId }) => !own.dataflowIds.has(dataflowId),
    );
    const theirExports = exports.flatMap(
      ({ objectId, dataflowId, dataflowName }): DataflowExport[] =>
        own.dataflowIds.has(dataflowId)
          ? []
          : [
              {
                objectId,
                dataflowId,
                ...(dataflowName !== null && { dataflowName }),
              },
            ],
    );
    const rowIds = dataflowRowIds(rows, theirExports, previous?.rowIds);
    const elapsedNsByWorker = accumulateWorkerElapsedNs(rows, previous);
    const { rowTotals, grown } = accumulateRowElapsedNs({
      rows,
      rowIds,
      workerCount,
      previous,
    });
    for (const [rowId, ids] of grown) {
      const sinceRead = grownSinceRead.get(rowId) ?? new Set<string>();
      for (const id of ids) sinceRead.add(id);
      grownSinceRead.set(rowId, sinceRead);
    }
    previous = {
      atMs,
      elapsedNsByWorker,
      counters: dataflowCounters(rows),
      rowTotals,
      rowIds,
    };
    const readings = addReading({ atMs, elapsedNsByWorker, afterGap });
    let update: Partial<WorkerCpuState> = { readings };
    afterGap = false;
    if (!isStretchStarted) {
      isStretchStarted = true;
      update.stretchStartMs = atMs;
    }
    const lastRead = state.objectReads.at(-1);
    // Against the upper, not the clock, or counters 30 s behind the clock
    // would be read every time.
    if (
      isObjectReadDue ||
      !lastRead ||
      atMs - lastRead.atMs >= OBJECT_READ_INTERVAL_MS
    ) {
      const objectRead: ObjectCpuRead = {
        atMs,
        rows,
        exports: theirExports,
        rowTotals: [...rowTotals],
        grownDataflows: fromGrownMap(grownSinceRead),
      };
      grownSinceRead = new Map();
      update = { ...update, ...readObjects(objectRead, readings) };
    }
    return update;
  };

  const tick = () => {
    timer = undefined;
    if (stopped || state.status !== "reading" || !live) return;
    const read = live;
    const nowMs = now();
    // Timed by the subscribe's upper. The counters trail the clock, by more
    // on a busy replica, so the clock would misstate the span a delta covers.
    const atMs = read.manager.getUpper();
    let update: Partial<WorkerCpuState> =
      atMs === undefined
        ? { waitingMs: nowMs - read.openedAtMs }
        : { waitingMs: undefined, lagMs: Math.max(0, nowMs - atMs) };
    if (atMs !== undefined && atMs > (state.readings.at(-1)?.atMs ?? 0)) {
      update = { ...update, ...takeReading(read, atMs) };
    }
    // The subscribe ends at its `UP TO` on its own, which can come first when
    // this clock runs behind the replica's.
    if (read.manager.getSnapshot().isComplete || nowMs >= read.upToMs) {
      pauseFor("limit", update);
      return;
    }
    if (Object.keys(update).length > 0) setState(update);
    timer = setTimeout(tick, READ_INTERVAL_MS);
  };

  const open = () => {
    let awaitingSnapshot = true;
    isStretchStarted = false;
    live = openLiveRead({ httpAddress, sessionVariables }, now(), (read) => {
      if (read !== live) return;
      const { error, snapshotComplete, isComplete } =
        read.manager.getSnapshot();
      if (error) {
        fail(error.message);
        return;
      }
      if (isComplete || (awaitingSnapshot && snapshotComplete)) {
        // A reading is due once the snapshot lands, and a last one when the
        // subscribe ends. A timer keeps `tick` out of the manager's flush,
        // which calls this.
        awaitingSnapshot = false;
        clearTimeout(timer);
        timer = setTimeout(tick, 0);
      }
    });
    timer = setTimeout(tick, READ_INTERVAL_MS);
  };

  const resume = () => {
    if (stopped || isHidden || state.status === "reading") return;
    setState({
      status: "reading",
      pauseReason: undefined,
      error: undefined,
      lagMs: undefined,
      waitingMs: 0,
      stretchStartMs: undefined,
    });
    open();
  };

  return {
    getState: () => state,
    subscribe: (listener) => {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    start: () => {
      if (isHidden) {
        setState({ status: "paused", pauseReason: "hidden" });
        return;
      }
      setState({ waitingMs: 0 });
      open();
    },
    pause: () => {
      if (state.status === "reading") pauseFor("user");
    },
    resume,
    measure: () => {
      if (stopped) return;
      measureAfterMs = state.readings.at(-1)?.atMs ?? 0;
      isObjectReadDue = true;
      setState({ isMeasuring: true });
      // The minute has to land before the open session reaches its `UP TO`,
      // or the session ends first and the measurement is never taken.
      const neededMs =
        MEASUREMENT_MS + OBJECT_READ_INTERVAL_MS + (state.lagMs ?? 0);
      if (live && now() + neededMs > live.upToMs) {
        afterGap = true;
        close();
        setState({ waitingMs: 0, stretchStartMs: undefined });
        open();
        return;
      }
      resume();
    },
    annotateMeasurement: (endMs, annotation) => {
      const { measurement } = state;
      if (!measurement || measurement.endMs !== endMs) return;
      const annotated = { ...measurement, ...annotation };
      applyRows(measurementStore, [annotated]);
      setState({ measurement: annotated });
    },
    setHidden: (hidden) => {
      isHidden = hidden;
      if (hidden && state.status === "reading") pauseFor("hidden");
      if (!hidden && state.pauseReason === "hidden") resume();
    },
    stop: () => {
      stopped = true;
      close();
      if (previous) {
        applyRows(continuationStore, [
          {
            atMs: previous.atMs,
            counters: [...previous.counters],
            ownExports,
            rowTotals: [...previous.rowTotals],
            rowIds: [...previous.rowIds],
            grownSinceRead: fromGrownMap(grownSinceRead),
          },
        ]);
      }
    },
  };
}

const noopSubscribe = () => () => {};

/** Keeps `sampler` paused while the browser tab is hidden. */
const registerVisibilityPause = (sampler: WorkerCpuSampler) => {
  const apply = () => sampler.setHidden(document.visibilityState === "hidden");
  apply();
  document.addEventListener("visibilitychange", apply);
  return () => document.removeEventListener("visibilitychange", apply);
};

/**
 * Samples one replica's worker CPU while the calling component is mounted,
 * continuing from the readings of an earlier session in the last
 * `READINGS_RETENTION_MS`.
 */
export function useWorkerCpuSampler({
  clusterName,
  replicaId,
  replicaName,
  workerCount,
}: {
  clusterName: string;
  replicaId: string;
  replicaName: string;
  workerCount: number;
}) {
  const httpAddress = useCurrentEnvironmentHttpAddress();
  const scope = useAtomValue(syncEngineCacheScopeLoadableAtom);
  const scopeKey =
    scope.state === "hasData" && scope.data ? scope.data : undefined;
  const [sampler, setSampler] = React.useState<WorkerCpuSampler>();
  React.useEffect(() => {
    if (!httpAddress || !scopeKey) return;
    const next = createWorkerCpuSampler({
      httpAddress,
      sessionVariables: { cluster: clusterName, cluster_replica: replicaName },
      workerCount,
      readingsStore: readingsByReplica.get(scopeKey, replicaId),
      objectsStore: objectReadsByReplica.get(scopeKey, replicaId),
      continuationStore: continuationByReplica.get(scopeKey, replicaId),
      measurementStore: measurementByReplica.get(scopeKey, replicaId),
    });
    setSampler(next);
    const unregisterVisibility = registerVisibilityPause(next);
    next.start();
    return () => {
      unregisterVisibility();
      next.stop();
      setSampler(undefined);
    };
  }, [httpAddress, scopeKey, clusterName, replicaId, replicaName, workerCount]);
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
    measure: sampler?.measure,
    annotateMeasurement: sampler?.annotateMeasurement,
  };
}
