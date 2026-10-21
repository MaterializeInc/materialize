// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { greatest, min } from "d3";

import { OOM_REASON } from "~/api/materialize/cluster/replicaStatusTimeline";
import { AlertVariant } from "~/components/Alert";
import {
  formatDate,
  FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
  TIME_FORMAT_NO_SECONDS,
} from "~/utils/dateFormat";
import { formatDurationExact } from "~/utils/format";

import {
  Hydration,
  HydrationReplica,
  latestBoot,
  latestFullHydration,
  UnfinishedHydration,
} from "./hydrationModel";
import { ReplicaHydrationProgress } from "./useLiveHydration";

/** A finished hydration or one a replica restarted before finishing. */
export type HydrationRow =
  | ({ type: "hydration" } & Hydration)
  | ({ type: "unfinished" } & UnfinishedHydration);

/** When a row happened: a hydration's start, or an unfinished one's end. */
export const rowTime = (row: HydrationRow) =>
  row.type === "hydration" ? row.startedAt : row.endedAt;

/** Hydrations and unfinished ones, newest first. */
export const toHydrationRows = (
  hydrations: Hydration[],
  unfinished: UnfinishedHydration[],
): HydrationRow[] =>
  [
    ...hydrations.map((row) => ({ ...row, type: "hydration" as const })),
    ...unfinished.map((row) => ({ ...row, type: "unfinished" as const })),
  ].sort((a, b) => rowTime(b).getTime() - rowTime(a).getTime());

/** `restarting` is hydrating again after restarting since it last finished. */
export type ReplicaHydrationState = "hydrated" | "hydrating" | "restarting";

export interface ReplicaNow {
  replica: HydrationReplica;
  state: ReplicaHydrationState;
  progress: ReplicaHydrationProgress;
  /** When its current hydration began, if that's known. */
  startedAt: Date | undefined;
  lastFull: Hydration | undefined;
  /** Times it restarted without finishing, since it last finished. */
  restarts: UnfinishedHydration[];
}

/**
 * When a replica's current hydration began. If every object it hasn't hydrated
 * was created after its latest boot, it's hydrating new objects and began with
 * the first of them. Otherwise it's rehydrating since that boot. Asking whether
 * a hydration finished since the boot fails after a zero-downtime upgrade,
 * which records the boot after the replica already hydrated.
 */
function currentHydrationStart(
  bootedAt: Date | undefined,
  unhydratedObjectIds: string[],
  createdAtMsById: Map<string, number> | undefined,
) {
  if (!createdAtMsById) return undefined;
  const bootMs = bootedAt?.getTime() ?? -Infinity;
  const createdMs = unhydratedObjectIds.map((id) => createdAtMsById.get(id));
  const isNewObjects =
    createdMs.length > 0 &&
    createdMs.every((ms) => ms !== undefined && ms > bootMs);
  if (!isNewObjects) return bootedAt;
  const firstMs = min(createdMs, (ms) => ms);
  return firstMs === undefined ? undefined : new Date(firstMs);
}

/** Where each current replica's hydration stands. */
export function summarizeReplicasNow({
  replicas,
  progressByReplicaId,
  bootTimes,
  hydrations,
  unfinished,
  createdObjects,
}: {
  replicas: HydrationReplica[];
  progressByReplicaId: Map<string, ReplicaHydrationProgress>;
  bootTimes: Map<string, number[]>;
  hydrations: Hydration[];
  unfinished: UnfinishedHydration[];
  /** Objects created on the cluster in the window, undefined until known. */
  createdObjects: Array<{ id: string | null; occurredAt: Date }> | undefined;
}): ReplicaNow[] {
  const createdAtMsById =
    createdObjects &&
    new Map(
      createdObjects.flatMap(({ id, occurredAt }) =>
        id ? [[id, occurredAt.getTime()]] : [],
      ),
    );
  return replicas
    .filter(({ isCurrent }) => isCurrent)
    .map((replica) => {
      const progress = progressByReplicaId.get(replica.id) ?? {
        objectCount: 0,
        hydratedCount: 0,
        unhydratedObjectIds: [],
      };
      const lastFinishedMs =
        greatest(
          hydrations.filter(({ replicaId }) => replicaId === replica.id),
          ({ finishedAt }) => finishedAt.getTime(),
        )?.finishedAt.getTime() ?? -Infinity;
      const restarts = unfinished.filter(
        ({ replicaId, endedAt }) =>
          replicaId === replica.id && endedAt.getTime() > lastFinishedMs,
      );
      const isHydrating = progress.hydratedCount < progress.objectCount;
      return {
        replica,
        state: !isHydrating
          ? "hydrated"
          : restarts.length > 0
            ? "restarting"
            : "hydrating",
        progress,
        startedAt: isHydrating
          ? currentHydrationStart(
              latestBoot(bootTimes, replica.id),
              progress.unhydratedObjectIds,
              createdAtMsById,
            )
          : undefined,
        lastFull: latestFullHydration(hydrations, replica.id),
        restarts,
      };
    });
}

export interface HydrationVerdict {
  variant: AlertVariant;
  label: string;
  message: string | undefined;
  /** Whether to point at the hydration troubleshooting guides. */
  isFailing: boolean;
}

const LIST_FORMAT = new Intl.ListFormat("en", { type: "conjunction" });

const restartsSentence = (restarts: UnfinishedHydration[], subject: string) => {
  const times = restarts.length === 1 ? "once" : `${restarts.length} times`;
  const isOom = restarts.every(({ reason }) => reason === OOM_REASON);
  if (!isOom) return `${subject} restarted ${times}.`;
  return restarts.length === 1
    ? `${subject} restarted once, after running out of memory.`
    : `${subject} restarted ${times}, each after running out of memory.`;
};

/** One sentence on the cluster's hydration, with the numbers behind it. */
export function hydrationVerdict(
  replicasNow: ReplicaNow[],
  nowMs: number,
): HydrationVerdict {
  if (replicasNow.length === 0) {
    return {
      variant: "info",
      label: "This cluster has no replicas, so nothing on it is hydrated.",
      message: undefined,
      isFailing: false,
    };
  }

  const restarting = replicasNow.filter(({ state }) => state === "restarting");
  if (restarting.length > 0) {
    const restarts = restarting.flatMap(({ restarts: each }) => each);
    // Since the first attempt that never finished began, when that's recorded.
    const sinceMs =
      min(restarts, ({ startedAt, endedAt }) =>
        (startedAt ?? endedAt).getTime(),
      ) ?? nowMs;
    const names = LIST_FORMAT.format(
      restarting.map(({ replica }) => replica.name),
    );
    return {
      variant: "error",
      label: `${names} ${restarting.length === 1 ? "hasn't" : "haven't"} finished hydrating since ${formatDate(sinceMs, TIME_FORMAT_NO_SECONDS)}.`,
      message: restartsSentence(
        restarts,
        restarting.length === 1 ? "It" : "Between them, they",
      ),
      isFailing: true,
    };
  }

  const hydrating = replicasNow.filter(({ state }) => state === "hydrating");
  if (hydrating.length === 1) {
    const [{ replica, progress, startedAt, lastFull }] = hydrating;
    const elapsed = startedAt
      ? `, ${formatDurationExact(nowMs - startedAt.getTime())} so far`
      : "";
    return {
      variant: "warning",
      label: `${replica.name} is hydrating: ${progress.hydratedCount} of ${progress.objectCount} objects${elapsed}.`,
      message: lastFull
        ? `Its last full hydration took ${formatDurationExact(lastFull.durationMs)}.`
        : undefined,
      isFailing: false,
    };
  }
  if (hydrating.length > 1) {
    return {
      variant: "warning",
      label: `${LIST_FORMAT.format(hydrating.map(({ replica }) => replica.name))} are hydrating.`,
      message: undefined,
      isFailing: false,
    };
  }

  const lastFull = greatest(
    replicasNow.flatMap(({ lastFull: each }) => (each ? [each] : [])),
    ({ finishedAt }) => finishedAt.getTime(),
  );
  const replicaName = replicasNow.find(
    ({ replica }) => replica.id === lastFull?.replicaId,
  )?.replica.name;
  return {
    variant: "success",
    label:
      replicasNow.length === 1
        ? `${replicasNow[0].replica.name} is hydrated.`
        : "Every replica is hydrated.",
    message: lastFull
      ? `The last full hydration finished ${formatDate(lastFull.finishedAt, FRIENDLY_DATETIME_FORMAT_NO_SECONDS)} and took ${formatDurationExact(lastFull.durationMs)} on ${replicaName}.`
      : "No full hydration finished in this period.",
    isFailing: false,
  };
}

/** What the tab's terms mean, shared by the chart, detail and table. */
export const HYDRATION_HELP = {
  full: "Inferred: began as the replica started, or covered every object on the cluster, as after an upgrade.",
  newObjects:
    "Inferred: began while the replica was running, such as for a new index.",
  unfinished:
    "Inferred from replica status: it went offline or ran out of memory before finishing.",
  inProgress: "Still hydrating.",
  typical: "Median of recent full hydrations on this size.",
  hydrationSlot: "A replica hydrates a few objects at a time, 4 by default.",
  duration: "From the first dataflow installed to the last object hydrated.",
  objects: "Indexes and materialized views in the hydration.",
  peakMemory:
    "Most memory and disk one process used since it started, so it can predate this hydration.",
  peakDisk: "Most scratch disk or swap one process used since it started.",
  cpu: "Highest of the once-a-minute CPU samples during the hydration.",
  createdSince: "These hydrate on the next restart, which can take longer.",
  lastFullMemory:
    "Most memory and disk one process used at the last full hydration.",
  restarting: "Inferred from replica status: it restarted before finishing.",
};
