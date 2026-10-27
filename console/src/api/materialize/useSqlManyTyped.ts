// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { CompiledQuery, InferResult } from "kysely";
import React from "react";

import {
  CATALOG_SERVER_CLUSTER,
  mapRowToObject,
  SqlRequest,
  UseSqlApiRequestOptions,
  useSqlMany,
} from "~/api/materialize";

type KeyedQueries = { [K: string]: CompiledQuery };

type ExtractResultTypes<Q extends KeyedQueries> = {
  [K in keyof Q]: InferResult<Q[K]>;
};

/**
 * Executes an array of Kysely queries in a single API call.
 *
 * @param queries - an object where each value is a Kysely CompiledQuery
 * @param options - optional cluster and replica values
 *
 * @returns a `SqlApiResponse` with an extra `results` property, which is an object with
 * the same keys as the input `queries`, and the value is the typed result of that query.
 */
export function useSqlManyTyped<Q extends KeyedQueries>(
  queries: Q | null,
  options: UseSqlApiRequestOptions & {
    cluster?: string;
    replica?: string;
  } = {},
) {
  const { cluster, replica, ...rest } = options;

  const request = React.useMemo((): SqlRequest | undefined => {
    if (!queries) return undefined;
    return {
      queries: Object.values(queries).map((q) => ({
        query: q.sql,
        params: q.parameters as string[],
      })),
      cluster: cluster ?? CATALOG_SERVER_CLUSTER,
      replica: replica,
    };
  }, [queries, cluster, replica]);
  const inner = useSqlMany(
    queries && Object.keys(queries).length > 0 ? request : undefined,
    rest,
  );

  const results = React.useMemo(() => {
    if (!inner.data || !queries) return null;
    const res: Record<string, unknown> = {};

    const keys = Object.keys(queries);
    for (let i = 0; i < inner.data.length; i++) {
      const { columns, rows } = inner.data[i];
      const key = keys[i];
      if (!rows) {
        res[key] = null;
        continue;
      }
      const objects = rows.map((r: unknown[]) => mapRowToObject(r, columns));
      res[key] = objects;
    }
    return res;
  }, [inner.data, queries]);

  return { ...inner, results: results as ExtractResultTypes<Q> };
}
