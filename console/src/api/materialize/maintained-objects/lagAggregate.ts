// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { sql } from "kysely";

import { IPostgresInterval, queryBuilder } from "~/api/materialize";

export interface LagAggregateParams {
  /** Lookback window in minutes for the temporal filter on lag history. */
  lookbackMinutes: number;
}

export interface LagAggregateRow {
  object_id: string;
  lag: IPostgresInterval | null;
}

/**
 * One row per object: the maximum (pMAX) lag observed in
 * `mz_wallclock_global_lag_recent_history` over the lookback window.
 */
export function buildLagAggregateQuery({
  lookbackMinutes,
}: LagAggregateParams) {
  return queryBuilder
    .selectFrom("mz_wallclock_global_lag_recent_history")
    .select(["object_id", sql<IPostgresInterval | null>`max(lag)`.as("lag")])
    .where(
      (eb) =>
        sql`${eb.ref("occurred_at")} + INTERVAL '${sql.raw(`${lookbackMinutes}`)} MINUTES'`,
      ">=",
      sql<Date>`mz_now()`,
    )
    .groupBy("object_id");
}
