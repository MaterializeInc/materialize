// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useQuery } from "@tanstack/react-query";

import {
  buildQueryKeyPart,
  buildRegionQueryKey,
} from "~/api/buildQueryKeySchema";
import { executeSqlV2, queryBuilder } from "~/api/materialize";

const storageSizeQueryKey = (objectId: string) =>
  [
    ...buildRegionQueryKey("storageUsage"),
    buildQueryKeyPart("objectStorageSize", { objectId }),
  ] as const;

/** Bytes the object's persisted collection occupies, from the most recent
 * storage assessment. Null when no assessment exists for the object. */
export function useObjectStorageSize(objectId: string) {
  return useQuery({
    queryKey: storageSizeQueryKey(objectId),
    queryFn: async ({ queryKey, signal }) => {
      const compiled = queryBuilder
        .selectFrom("mz_recent_storage_usage as u")
        .where("u.object_id", "=", objectId)
        .select("u.size_bytes")
        .compile();
      const result = await executeSqlV2({
        queries: compiled,
        queryKey,
        requestOptions: { signal },
      });
      const raw = result.rows.at(0)?.size_bytes;
      return raw == null ? null : Number(raw);
    },
    refetchInterval: 60_000,
  });
}
