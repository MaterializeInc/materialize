// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { millisecondsInHour, millisecondsInMinute } from "date-fns/constants";

import { quoteIdentifier } from "~/api/materialize";
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

/**
 * How long a sink's frontier may sit still before we call it stuck. A sink that
 * commits on an interval (Iceberg) gets its interval on top.
 */
export const SINK_STUCK_AFTER_MS = millisecondsInMinute;

/** A schema or cluster a deploy staged, from the catalog or the audit log. */
export interface StagedObjectRow {
  kind: "schema" | "cluster";
  id: string;
  /** Null for clusters. */
  database: string | null;
  liveName: string;
  stagedName: string;
  suffix: string;
  /** Null once the audit log no longer has the create event. */
  stagedAt: Date | null;
  stagedBy: string | null;
}

export interface DeployEventRow extends StagedObjectRow {
  state: "promoted" | "aborted";
  endedAt: Date;
  endedBy: string | null;
}

export interface StagedPair {
  id: string;
  database: string | null;
  liveName: string;
  stagedName: string;
}

export type DeployState = "in-progress" | "promoted" | "aborted";

export type DeployTool = "mz-deploy" | "dbt" | "unknown";

export interface Deployment {
  /** Unique on the page, unlike the suffix, which dbt reuses every deploy. */
  key: string;
  suffix: string;
  state: DeployState;
  tool: DeployTool;
  stagedAt: Date | null;
  stagedBy: string | null;
  /** When it was promoted or aborted. */
  endedAt: Date | null;
  endedBy: string | null;
  schemas: StagedPair[];
  clusters: StagedPair[];
}

/** dbt-materialize's `deploy_init` stages `X` as `X_dbt_deploy`. */
export const DBT_DEPLOY_SUFFIX = "dbt_deploy";

/**
 * mz-deploy's default deploy ID: a git SHA prefix or random hex, 7 characters.
 * A custom `--deploy-id` is not recognized, so its deploy shows no tool.
 */
const MZ_DEPLOY_ID = /^[0-9a-f]{7}$/;

/** Aborted objects dropped further apart than this are separate deploys. */
const ABORT_GROUP_GAP_MS = 10 * millisecondsInMinute;

type GroupedRow = StagedObjectRow & {
  endedAt?: Date;
  endedBy?: string | null;
};

/**
 * Folds staged objects and audit-log events into deploys: in progress first,
 * then the rest newest first. One promote is one swap statement, so its
 * renames share a timestamp.
 */
export function groupDeployments(
  staged: StagedObjectRow[],
  history: DeployEventRow[],
  hasMzDeploy: boolean,
): Deployment[] {
  const groups = new Map<string, { state: DeployState; rows: GroupedRow[] }>();
  const add = (key: string, state: DeployState, row: GroupedRow) => {
    const group = groups.get(key) ?? { state, rows: [] };
    group.rows.push(row);
    groups.set(key, group);
  };

  for (const row of staged) add(row.suffix, "in-progress", row);
  for (const row of history) {
    if (row.state === "promoted") {
      add(`${row.suffix}-${row.endedAt.getTime()}`, "promoted", row);
    }
  }
  const aborted = history
    .filter((row) => row.state === "aborted")
    .sort(
      (a, b) =>
        a.suffix.localeCompare(b.suffix) ||
        a.endedAt.getTime() - b.endedAt.getTime(),
    );
  let abortKey = "";
  let previous: DeployEventRow | undefined;
  for (const row of aborted) {
    if (
      !previous ||
      previous.suffix !== row.suffix ||
      row.endedAt.getTime() - previous.endedAt.getTime() > ABORT_GROUP_GAP_MS
    ) {
      abortKey = `${row.suffix}-${row.endedAt.getTime()}`;
    }
    add(abortKey, "aborted", row);
    previous = row;
  }

  const tool = (suffix: string): DeployTool =>
    suffix === DBT_DEPLOY_SUFFIX
      ? "dbt"
      : hasMzDeploy && MZ_DEPLOY_ID.test(suffix)
        ? "mz-deploy"
        : "unknown";
  const recency = (d: Deployment) =>
    d.state === "in-progress"
      ? Infinity
      : (d.endedAt ?? d.stagedAt ?? new Date(0)).getTime();
  return [...groups]
    .map(([key, { state, rows }]) => toDeployment(key, state, rows, tool))
    .sort(
      (a, b) =>
        recency(b) - recency(a) ||
        (b.stagedAt?.getTime() ?? 0) - (a.stagedAt?.getTime() ?? 0),
    );
}

function toDeployment(
  key: string,
  state: DeployState,
  rows: GroupedRow[],
  tool: (suffix: string) => DeployTool,
): Deployment {
  const earliest = rows.reduce<GroupedRow | undefined>(
    (best, row) =>
      row.stagedAt && (!best?.stagedAt || row.stagedAt < best.stagedAt)
        ? row
        : best,
    undefined,
  );
  const latest = rows.reduce<GroupedRow | undefined>(
    (best, row) =>
      row.endedAt && (!best?.endedAt || row.endedAt > best.endedAt)
        ? row
        : best,
    undefined,
  );
  // A staged name can extend two live names (`a_b_1` extends `a` and `a_b`);
  // the longer live name is the one it copies.
  const byId = new Map<string, GroupedRow>();
  for (const row of rows) {
    const seen = byId.get(row.id);
    if (!seen || row.liveName.length > seen.liveName.length) {
      byId.set(row.id, row);
    }
  }
  const pairs = (kind: StagedObjectRow["kind"]) =>
    [...byId.values()]
      .filter((row) => row.kind === kind)
      .map(({ id, database, liveName, stagedName }) => ({
        id,
        database,
        liveName,
        stagedName,
      }))
      .sort((a, b) =>
        `${a.database ?? ""}.${a.liveName}`.localeCompare(
          `${b.database ?? ""}.${b.liveName}`,
        ),
      );
  return {
    key,
    suffix: rows[0].suffix,
    state,
    tool: tool(rows[0].suffix),
    stagedAt: earliest?.stagedAt ?? null,
    stagedBy: earliest?.stagedBy ?? null,
    endedAt: latest?.endedAt ?? null,
    endedBy: latest?.endedBy ?? null,
    schemas: pairs("schema"),
    clusters: pairs("cluster"),
  };
}

/** The commands that finish a deploy, when the tool that staged it is known. */
export function finishCommands(
  deployment: Deployment,
): { promote: string; abort: string } | null {
  switch (deployment.tool) {
    case "mz-deploy":
      return {
        promote: `mz-deploy promote ${deployment.suffix}`,
        abort: `mz-deploy abort ${deployment.suffix}`,
      };
    case "dbt":
      return {
        promote: "dbt run-operation deploy_promote",
        abort: "dbt run-operation deploy_cleanup",
      };
    case "unknown":
      return null;
  }
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
  /** The deploy whose promote would repoint this sink. */
  deployId: string;
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
  /** The sink's `COMMIT INTERVAL`. Its frontier only moves once per interval. */
  commitIntervalMs: number | null;
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
  const stuckAfterMs = SINK_STUCK_AFTER_MS + (row.commitIntervalMs ?? 0);
  if (stalledForMs >= stuckAfterMs) return { state: "stuck", gapMs };
  return { state: "behind", gapMs };
}

export interface DefinitionRow {
  id: string;
  database: string;
  schema: string;
  name: string;
  type: string;
  clusterName: string | null;
  createSql: string | null;
}

export type ObjectChange = "added" | "changed" | "unchanged" | "removed";

export interface ObjectDiffEntry {
  database: string;
  /** The live schema's name, for staged objects too. */
  schema: string;
  object: string;
  type: string;
  change: ObjectChange;
  /** Null for removed objects. */
  stagedId: string | null;
}

/**
 * A `create_sql` as it would read with every staged schema and cluster under
 * its live name, so an unchanged staged object compares equal to its live one.
 * The catalog writes references by ID (`[u5 AS "db"."s"."x"]`, `IN CLUSTER
 * [u7]`) and ends a materialized view with the `AS OF` it was created at, and
 * both always differ between the two copies.
 */
export function normalizeCreateSql(
  createSql: string,
  clusterName: string | null,
  renames: Map<string, string>,
) {
  let normalized = createSql
    .replace(/ AS OF \d+$/, "")
    .replace(/\[[us]\d+ AS ((?:"(?:[^"]|"")*"\.)*"(?:[^"]|"")*")\]/g, "$1")
    .replace(
      /IN CLUSTER \[[us]\d+\]/g,
      clusterName ? `IN CLUSTER ${quoteIdentifier(clusterName)}` : "IN CLUSTER",
    );
  for (const [from, to] of renames) {
    normalized = normalized
      .split(quoteIdentifier(from))
      .join(quoteIdentifier(to));
  }
  return normalized;
}

/**
 * What promoting `deployment` changes, by comparing each staged object's
 * definition with the live object of the same name. `unchanged` objects are
 * still rebuilt, because blue/green deploys swap whole schemas.
 *
 * @param rows - definitions in the deploy's live and staged schemas.
 */
export function diffDefinitions(
  deployment: Deployment,
  rows: DefinitionRow[],
): ObjectDiffEntry[] {
  const renames = new Map(
    [...deployment.schemas, ...deployment.clusters].map((p) => [
      p.stagedName,
      p.liveName,
    ]),
  );
  const liveSchemaOf = new Map(
    deployment.schemas.map((p) => [
      `${p.database}.${p.stagedName}`,
      p.liveName,
    ]),
  );
  const live = new Map<string, DefinitionRow>();
  const staged = new Map<string, { row: DefinitionRow; liveSchema: string }>();
  for (const row of rows) {
    const liveSchema = liveSchemaOf.get(`${row.database}.${row.schema}`);
    if (liveSchema === undefined) {
      live.set(`${row.database}.${row.schema}.${row.name}`, row);
    } else {
      staged.set(`${row.database}.${liveSchema}.${row.name}`, {
        row,
        liveSchema,
      });
    }
  }
  const normalize = (row: DefinitionRow) =>
    row.createSql === null
      ? null
      : normalizeCreateSql(row.createSql, row.clusterName, renames);

  const entries: ObjectDiffEntry[] = [];
  for (const [key, { row, liveSchema }] of staged) {
    const before = live.get(key);
    entries.push({
      database: row.database,
      schema: liveSchema,
      object: row.name,
      type: row.type,
      change: !before
        ? "added"
        : normalize(before) === normalize(row)
          ? "unchanged"
          : "changed",
      stagedId: row.id,
    });
  }
  for (const [key, row] of live) {
    if (!staged.has(key)) {
      entries.push({
        database: row.database,
        schema: row.schema,
        object: row.name,
        type: row.type,
        change: "removed",
        stagedId: null,
      });
    }
  }
  return entries.sort((a, b) =>
    `${a.schema}.${a.object}`.localeCompare(`${b.schema}.${b.object}`),
  );
}

/** Promoted deploys that are the latest promote for one of their schemas. */
export function liveDeployKeys(all: Deployment[]): Set<string> {
  const latest = new Map<string, Deployment>();
  for (const deployment of all) {
    const endedAt = deployment.endedAt?.getTime();
    if (deployment.state !== "promoted" || endedAt === undefined) continue;
    for (const { database, liveName } of deployment.schemas) {
      const key = `${database}.${liveName}`;
      const current = latest.get(key)?.endedAt?.getTime() ?? -Infinity;
      if (endedAt > current) latest.set(key, deployment);
    }
  }
  return new Set([...latest.values()].map((d) => d.key));
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
