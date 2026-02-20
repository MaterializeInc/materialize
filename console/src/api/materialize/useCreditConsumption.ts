// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { sql } from "kysely";
import React from "react";

import { queryBuilder, useSqlTyped } from "~/api/materialize";

/**
 * Gets the number of compute credits per hour that are being consumed.
 * @param since filters replicas dropped before the given date, or undefined which
 * returns all replicas that are still running.
 */
export function useCreditConsumption(since?: Date) {
  const query = React.useMemo(() => {
    let qb = queryBuilder
      .selectFrom("mz_cluster_replica_history as mcrh")
      .select([
        "mcrh.replica_id as replicaId",
        "mcrh.size",
        "mcrh.created_at as createdAt",
        "mcrh.dropped_at as dropppedAt",
        sql<number>`mcrh.credits_per_hour`.as("creditsPerHour"),
      ]);
    if (since) {
      qb = qb.where((eb) =>
        eb.or([
          eb("mcrh.dropped_at", "is", null),
          eb("mcrh.dropped_at", ">=", since),
        ]),
      );
    } else {
      qb = qb.where("mcrh.dropped_at", "is", null);
    }
    return qb.compile();
  }, [since]);

  const response = useSqlTyped(query);
  let results = null;
  if (response.results) {
    results = response.results.reduce((acc, h) => acc + h.creditsPerHour, 0);
  }
  return { ...response, results };
}
