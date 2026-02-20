// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { CompiledQuery } from "kysely";
import React from "react";

import {
  CATALOG_SERVER_CLUSTER,
  mapRowToObject,
  useSqlMany,
} from "~/api/materialize";

export function useSqlTyped<R, Result = R>(
  query: CompiledQuery<R> | null,
  options: {
    cluster?: string;
    replica?: string;
    queryKey?: string;
    transformRow?: (r: R) => Result;
  } = {},
) {
  const { cluster, replica, transformRow } = options;
  const [transform, setTransform] = React.useState(() => transformRow);

  // So callers don't have to memoize this function
  React.useEffect(() => {
    setTransform(transformRow);
  }, [transformRow]);

  const request = React.useMemo(() => {
    if (!query) return undefined;
    return {
      queries: [
        {
          query: options.queryKey
            ? `--${options.queryKey}\n` + query.sql
            : query.sql,
          params: query.parameters as string[],
        },
      ],
      cluster: cluster ?? CATALOG_SERVER_CLUSTER,
      replica: replica,
    };
  }, [query, options.queryKey, cluster, replica]);
  const inner = useSqlMany(query ? request : undefined);

  const results: Result[] | null = React.useMemo(() => {
    if (!inner.data) return null;

    const { columns, rows } = inner.data[0];
    return rows.map((r: unknown[]) => {
      const o: Record<string, unknown> = mapRowToObject(r, columns);
      if (transform) {
        return transform(o as R);
      }
      return o as Result;
    });
  }, [inner.data, transform]);

  return { ...inner, results };
}

export default useSqlTyped;
