// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { QueryKey } from "@tanstack/react-query";
import { sql } from "kysely";

import { escapedLiteral as lit, queryBuilder } from "~/api/materialize";

import { executeSqlV2 } from "../executeSqlV2";

/** An `ON HYDRATION` autoscaling policy. */
export interface AutoScalingPolicyParams {
  hydrationSize: string;
  /** Undefined leaves the linger to the environment's default. */
  lingerMs: number | undefined;
}

export interface AlterClusterSettingsParams {
  clusterName: string;
  /** Undefined leaves the size as it is. */
  size?: string;
  /** Undefined leaves the replication factor as it is. */
  replicas?: number;
  /** A policy to set, null to remove the cluster's, undefined to keep it. */
  autoScalingPolicy?: AutoScalingPolicyParams | null;
}

export interface AlterClusterNameParams {
  clusterName: string;
  name: string;
}

/**
 * A linger as interval text: whole seconds as `Ns`, otherwise `Nms`. Rounded
 * to microseconds, an interval's precision, so float noise from a parsed
 * duration can't reach the literal.
 */
export const lingerInterval = (lingerMs: number) => {
  const micros = Math.round(lingerMs * 1000);
  return micros % 1_000_000 === 0
    ? `${micros / 1_000_000}s`
    : `${micros / 1000}ms`;
};

const autoScalingStrategyOption = (policy: AutoScalingPolicyParams | null) =>
  policy === null
    ? sql`AUTO SCALING STRATEGY = ()`
    : sql`AUTO SCALING STRATEGY = (\n    ON HYDRATION (HYDRATION SIZE = ${lit(policy.hydrationSize)}${
        policy.lingerMs === undefined
          ? sql``
          : sql`, LINGER DURATION = ${lit(lingerInterval(policy.lingerMs))}`
      })\n  )`;

/**
 * The one statement that applies the settings to change, so a failure leaves
 * all of them as they were, or undefined when there are none.
 */
export const alterClusterSettingsStatement = ({
  clusterName,
  size,
  replicas,
  autoScalingPolicy,
}: AlterClusterSettingsParams) => {
  const options = [
    ...(size === undefined ? [] : [sql`SIZE = ${lit(size)}`]),
    ...(replicas === undefined
      ? []
      : [sql`REPLICATION FACTOR = ${sql.raw(replicas.toString())}`]),
  ];
  if (autoScalingPolicy === null && options.length === 0) {
    // NOTE: Unlike a SET naming the strategy, RESET works with autoscaling's
    // feature flag off, so a policy set while it was on can still be removed.
    return sql`ALTER CLUSTER ${sql.id(clusterName)} RESET (AUTO SCALING STRATEGY)`;
  }
  if (autoScalingPolicy !== undefined) {
    options.push(autoScalingStrategyOption(autoScalingPolicy));
  }
  if (options.length === 0) return undefined;
  return sql`ALTER CLUSTER ${sql.id(clusterName)} SET (\n  ${sql.join(options, sql`,\n  `)}\n)`;
};

export const alterClusterNameStatement = (values: AlterClusterNameParams) => {
  return sql`ALTER CLUSTER ${sql.id(values.clusterName)} RENAME TO ${sql.id(values.name)}
`;
};

/**
 * The statements an alter runs, in order, and none when nothing changes. Each
 * commits on its own, so a failed RENAME leaves the settings applied.
 */
export function alterClusterStatements(
  settingsParams: AlterClusterSettingsParams,
  nameParams: AlterClusterNameParams,
) {
  const settings = alterClusterSettingsStatement(settingsParams);
  return [
    ...(settings ? [settings] : []),
    // A RENAME to the same name still writes an audit event.
    ...(nameParams.name === nameParams.clusterName
      ? []
      : [alterClusterNameStatement(nameParams)]),
  ].map((statement) => statement.compile(queryBuilder));
}

export function alterCluster({
  nameParams,
  settingsParams,
  queryKey,
  requestOptions,
}: {
  nameParams: AlterClusterNameParams;
  settingsParams: AlterClusterSettingsParams;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  return executeSqlV2({
    queries: alterClusterStatements(settingsParams, nameParams),
    queryKey: queryKey,
    requestOptions,
  });
}
