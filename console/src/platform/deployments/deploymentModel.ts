// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { millisecondsInHour, millisecondsInMinute } from "date-fns/constants";

import { formatDurationExact } from "~/utils/format";

/**
 * `mz-deploy wait`'s default `--allowed-lag`. A hydrated cluster whose objects
 * lag by more than this is "lagging", not "ready".
 */
export const DEFAULT_ALLOWED_LAG_MS = 5 * millisecondsInMinute;

/**
 * mz-deploy's OOM-loop rule: a replica with this many `oom-killed` status
 * events in the last 24 hours is "problematic".
 */
export const OOM_LOOP_THRESHOLD = 3;

/** How long a sink's frontier may sit still before we call it stuck. */
export const SINK_STUCK_AFTER_MS = millisecondsInMinute;

export interface DeploymentRow {
  deployId: string;
  deployedAt: Date;
  promotedAt: Date | null;
  database: string;
  schema: string;
  deployedBy: string;
  commit: string | null;
  kind: string;
  mode: string;
}

export interface DeploymentSchema {
  database: string;
  schema: string;
  /** `objects` schemas are staged and swapped; `sinks` schemas are not. */
  kind: string;
}

export interface Deployment {
  deployId: string;
  deployedAt: Date;
  promotedAt: Date | null;
  deployedBy: string;
  commit: string | null;
  mode: string;
  schemas: DeploymentSchema[];
}

/**
 * Folds mz-deploy's one-row-per-schema `deployments` table into one entry per
 * deploy, newest first.
 */
export function groupDeployments(rows: DeploymentRow[]): Deployment[] {
  const byId = new Map<string, Deployment>();
  for (const row of rows) {
    let deployment = byId.get(row.deployId);
    if (!deployment) {
      deployment = {
        deployId: row.deployId,
        deployedAt: row.deployedAt,
        promotedAt: row.promotedAt,
        deployedBy: row.deployedBy,
        commit: row.commit,
        mode: row.mode,
        schemas: [],
      };
      byId.set(row.deployId, deployment);
    }
    deployment.schemas.push({
      database: row.database,
      schema: row.schema,
      kind: row.kind,
    });
  }
  for (const deployment of byId.values()) {
    deployment.schemas.sort((a, b) =>
      `${a.database}.${a.schema}`.localeCompare(`${b.database}.${b.schema}`),
    );
  }
  return [...byId.values()].sort(
    (a, b) => b.deployedAt.getTime() - a.deployedAt.getTime(),
  );
}

/** Schemas that `stage` copies under a suffix and `promote` swaps. */
export function swappedSchemas(deployment: Deployment) {
  return deployment.schemas.filter((s) => s.kind === "objects");
}

/**
 * mz-deploy names a staged cluster or schema `<live name>_<deploy id>`.
 * `promote` swaps the two, so after promotion the suffixed name holds the old
 * production objects until they are dropped.
 */
export function stagingName(liveName: string, deployId: string) {
  return `${liveName}_${deployId}`;
}

export interface ClusterHealth {
  id: string;
  name: string;
  size: string | null;
  replicationFactor: number | null;
  replicas: number;
  oomLoopingReplicas: number;
  /**
   * Some replica has reported hydration for any object, system ones included.
   * Until then `hydrated` and `total` are both 0.
   */
  hydrationReported: boolean;
  /** User objects hydrated on the replica that has hydrated the most. */
  hydrated: number;
  total: number;
  memoryPercent: number | null;
  maxLagMs: number | null;
}

export type ClusterStatus = "ready" | "hydrating" | "lagging" | "failing";

export interface ClusterVerdict {
  status: ClusterStatus;
  detail: string;
}

/**
 * Ports the status rule `mz-deploy wait` applies to each staged cluster, so the
 * Console and the CLI agree on when a deploy can be promoted.
 *
 * NOTE: mz-deploy counts every object on the replica, including system
 * introspection objects, so its "33/37" reads "4 of 5" here. Introspection
 * objects hydrate within seconds, so the verdict matches in practice.
 */
export function clusterVerdict(
  health: ClusterHealth | undefined,
  allowedLagMs = DEFAULT_ALLOWED_LAG_MS,
): ClusterVerdict {
  if (!health) {
    return { status: "failing", detail: "New cluster was dropped" };
  }
  if (health.replicas === 0) {
    return { status: "failing", detail: "No replicas" };
  }
  if (health.oomLoopingReplicas >= health.replicas) {
    return {
      status: "failing",
      detail: `Out of memory ${OOM_LOOP_THRESHOLD}+ times in 24 h on every replica`,
    };
  }
  if (!health.hydrationReported) {
    return { status: "hydrating", detail: "Waiting for replicas to report" };
  }
  if (health.hydrated < health.total) {
    return {
      status: "hydrating",
      detail: `${health.hydrated} of ${health.total} objects hydrated`,
    };
  }
  if (health.maxLagMs !== null && health.maxLagMs > allowedLagMs) {
    return {
      status: "lagging",
      detail: `Hydrated, but ${formatDuration(health.maxLagMs)} behind`,
    };
  }
  return { status: "ready", detail: "Hydrated and caught up" };
}

export interface SinkCutoverRow {
  sinkId: string;
  sinkDatabase: string;
  sinkSchema: string;
  sinkName: string;
  status: string | null;
  error: string | null;
  dependencySchema: string;
  dependencyName: string;
  /** The staged replacement for the sink's input exists. */
  hasReplacement: boolean;
  sinkWriteFrontierMs: number | null;
  replacementReadFrontierMs: number | null;
  sinkLagMs: number | null;
}

export type CutoverState =
  "ready" | "behind" | "stuck" | "no-replacement" | "unknown";

export interface SinkCutover {
  state: CutoverState;
  /** How far the sink still has to write before the cutover can happen. */
  gapMs: number;
}

/**
 * Whether `ALTER SINK ... SET FROM` would complete now.
 *
 * The cutover waits until the sink has written up to the oldest time the
 * replacement can be read at (its read frontier). A sink that is behind but
 * advancing delays the promote; one that is not advancing hangs it until the
 * statement times out.
 *
 * @param stalledForMs - how long the sink's frontier has been unchanged, as
 * observed by the caller across polls.
 */
export function sinkCutover(
  row: SinkCutoverRow,
  stalledForMs: number,
): SinkCutover {
  if (!row.hasReplacement || row.replacementReadFrontierMs === null) {
    return { state: "no-replacement", gapMs: 0 };
  }
  if (row.sinkWriteFrontierMs === null) {
    return { state: "unknown", gapMs: 0 };
  }
  const gapMs = Math.max(
    0,
    row.replacementReadFrontierMs - row.sinkWriteFrontierMs,
  );
  if (gapMs === 0) return { state: "ready", gapMs };
  if (stalledForMs >= SINK_STUCK_AFTER_MS) return { state: "stuck", gapMs };
  return { state: "behind", gapMs };
}

export interface DeployObjectRow {
  deployId: string;
  database: string;
  schema: string;
  object: string;
  hash: string;
}

export type ObjectChange = "added" | "changed" | "unchanged" | "removed";

export interface ObjectDiffEntry {
  database: string;
  schema: string;
  object: string;
  change: ObjectChange;
}

const objectKey = (o: { database: string; schema: string; object: string }) =>
  `${o.database}.${o.schema}.${o.object}`;

/**
 * Compares a deploy's object hashes with the deploy each schema was last
 * promoted from. `unchanged` objects are still redeployed, because mz-deploy
 * stages and swaps whole schemas.
 *
 * @param baseline - per `database.schema`, the deploy it is compared against.
 * Schemas with no baseline are new, so all their objects are `added`.
 */
export function diffObjects(
  deployId: string,
  rows: DeployObjectRow[],
  baseline: Map<string, string>,
): ObjectDiffEntry[] {
  const current = rows.filter((r) => r.deployId === deployId);
  const schemas = new Set(current.map((r) => `${r.database}.${r.schema}`));
  const previous = new Map<string, DeployObjectRow>();
  for (const row of rows) {
    const schemaKey = `${row.database}.${row.schema}`;
    if (schemas.has(schemaKey) && baseline.get(schemaKey) === row.deployId) {
      previous.set(objectKey(row), row);
    }
  }

  const entries: ObjectDiffEntry[] = current.map((row) => {
    const before = previous.get(objectKey(row));
    const change: ObjectChange = !before
      ? "added"
      : before.hash === row.hash
        ? "unchanged"
        : "changed";
    return {
      database: row.database,
      schema: row.schema,
      object: row.object,
      change,
    };
  });
  const currentKeys = new Set(current.map(objectKey));
  for (const [key, row] of previous) {
    if (!currentKeys.has(key)) {
      entries.push({
        database: row.database,
        schema: row.schema,
        object: row.object,
        change: "removed",
      });
    }
  }
  return entries.sort((a, b) => objectKey(a).localeCompare(objectKey(b)));
}

/**
 * For each schema of `deployment`, the deploy it replaces: the one live for
 * that schema when it was promoted, or, while it is in progress, the one live
 * now.
 */
export function baselineDeploys(
  deployment: Deployment,
  all: Deployment[],
): Map<string, string> {
  const cutoff = deployment.promotedAt?.getTime() ?? Infinity;
  const baseline = new Map<string, string>();
  for (const { database, schema } of deployment.schemas) {
    const key = `${database}.${schema}`;
    let best: Deployment | undefined;
    for (const candidate of all) {
      if (
        candidate.deployId === deployment.deployId ||
        candidate.promotedAt === null ||
        candidate.promotedAt.getTime() >= cutoff ||
        !candidate.schemas.some(
          (s) => s.database === database && s.schema === schema,
        )
      ) {
        continue;
      }
      if (
        !best ||
        (best.promotedAt?.getTime() ?? 0) < candidate.promotedAt.getTime()
      ) {
        best = candidate;
      }
    }
    if (best) baseline.set(key, best.deployId);
  }
  return baseline;
}

/** Deploys that are the most recent promote for at least one of their schemas. */
export function liveDeployIds(all: Deployment[]): Set<string> {
  const latest = new Map<string, Deployment>();
  for (const deployment of all) {
    const promotedAt = deployment.promotedAt?.getTime();
    if (promotedAt === undefined) continue;
    for (const { database, schema } of deployment.schemas) {
      const key = `${database}.${schema}`;
      const current = latest.get(key)?.promotedAt?.getTime() ?? -Infinity;
      if (promotedAt > current) latest.set(key, deployment);
    }
  }
  return new Set([...latest.values()].map((d) => d.deployId));
}

export interface Blocker {
  kind: "cluster" | "sink";
  name: string;
  message: string;
}

/** Everything that would stop or stall `mz-deploy promote` right now. */
export function promoteBlockers(
  clusters: {
    liveName: string;
    stagingName: string;
    verdict: ClusterVerdict;
  }[],
  sinks: { row: SinkCutoverRow; cutover: SinkCutover }[],
): Blocker[] {
  const blockers: Blocker[] = [];
  for (const cluster of clusters) {
    if (cluster.verdict.status !== "ready") {
      blockers.push({
        kind: "cluster",
        name: cluster.stagingName,
        message: cluster.verdict.detail,
      });
    }
  }
  for (const { row, cutover } of sinks) {
    const name = `${row.sinkSchema}.${row.sinkName}`;
    switch (cutover.state) {
      case "behind":
        blockers.push({
          kind: "sink",
          name,
          message: `Promote will wait about ${formatDuration(cutover.gapMs)} for this sink to catch up`,
        });
        break;
      case "stuck":
        blockers.push({
          kind: "sink",
          name,
          message: `Sink is ${formatDuration(cutover.gapMs)} behind and not advancing, so the cutover would hang`,
        });
        break;
      case "no-replacement":
        blockers.push({
          kind: "sink",
          name,
          message: `Reads ${row.dependencySchema}.${row.dependencyName}, which this deploy does not stage, so the repoint fails`,
        });
        break;
      case "unknown":
        blockers.push({
          kind: "sink",
          name,
          message:
            "Sink has not reported a write frontier, so the cutover can't be checked",
        });
        break;
      case "ready":
        break;
    }
  }
  return blockers;
}

/** "12 min ago" from a past date. */
export function timeAgo(date: Date, now = Date.now()) {
  return `${formatDuration(Math.max(0, now - date.getTime()))} ago`;
}

/**
 * `formatDurationExact` at whole seconds, with days past 48 hours, since deploys
 * can sit staged for a week.
 */
export function formatDuration(ms: number): string {
  if (ms >= 48 * millisecondsInHour) {
    return `${(ms / (24 * millisecondsInHour)).toFixed(1)} days`;
  }
  return formatDurationExact(ms < 1000 ? ms : Math.round(ms / 1000) * 1000);
}
