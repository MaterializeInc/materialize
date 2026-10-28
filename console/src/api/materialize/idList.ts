// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { sql } from "kysely";

import { queryBuilder } from "~/api/materialize";

/**
 * The given ids as a relation `ids(id)`, to join an indexed relation against
 * in place of `id IN (...)`. Past 330 entries a literal list stops planning as
 * an index lookup and scans the whole index, while the join keeps reading by
 * key (observed on v26.45 up to 2000 ids, checked at 400 by the lag SQL tests).
 */
export function buildIdListQuery(ids: string[]) {
  // A repeated id would repeat its rows, which doubles a caller's GROUP BY counts.
  const uniqueIds = [...new Set(ids)];
  return queryBuilder.selectNoFrom(
    sql<string>`unnest(ARRAY[${sql.join(uniqueIds)}]::text[])`.as("id"),
  );
}
