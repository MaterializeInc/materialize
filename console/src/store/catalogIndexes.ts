// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * @module
 * Global live cache of all user-visible indexes in the Materialize catalog.
 *
 * ## What
 * Maintains a Jotai atom containing every index via a single long-lived
 * SUBSCRIBE to `mz_indexes` joined with `mz_show_indexes`, `mz_roles`,
 * `mz_clusters`, and `mz_object_fully_qualified_names`.
 *
 * ## Why
 * Previously, each catalog detail view opened its own per-object WebSocket
 * SUBSCRIBE for indexes on mount, causing a visible delay. By subscribing
 * once at app startup, any component can read index data synchronously.
 *
 * ## How
 * Follows the three-part pattern (atom → initializer → consumer hooks).
 * See `catalogColumns.ts` module doc for details.
 *
 * ## When
 * Started in `AppInitializer.tsx`. Data available after initial snapshot.
 */

import { atom, useAtomValue } from "jotai";
import { sql } from "kysely";
import React from "react";

import { SubscribeState } from "~/api/materialize/SubscribeManager";
import {
  buildSubscribeQuery,
  useGlobalUpsertSubscribe,
} from "~/api/materialize/useSubscribe";

export interface CatalogIndex {
  id: string;
  onId: string;
  name: string;
  databaseName: string | null;
  schemaName: string;
  owner: string;
  indexedColumns: string;
  clusterId: string;
  clusterName: string;
}

function buildAllIndexesQuery() {
  return sql<CatalogIndex>`
    SELECT
      indexNames.id,
      indexes.on_id AS "onId",
      indexNames.name,
      indexNames.database_name AS "databaseName",
      indexNames.schema_name AS "schemaName",
      roles.name AS owner,
      showIndexes.key AS "indexedColumns",
      clusters.id AS "clusterId",
      clusters.name AS "clusterName"
    FROM mz_indexes indexes
    JOIN mz_object_fully_qualified_names indexNames ON indexNames.id = indexes.id
    JOIN mz_roles roles ON roles.id = indexes.owner_id
    JOIN mz_show_indexes showIndexes
      ON showIndexes.name = indexNames.name
     AND showIndexes.schema_id = indexNames.schema_id
    JOIN mz_clusters clusters ON clusters.id = indexes.cluster_id
  `;
}

export const catalogIndexes = atom<SubscribeState<CatalogIndex>>({
  data: [],
  error: undefined,
  snapshotComplete: false,
});

/** Call once in AppInitializer to start the global indexes SUBSCRIBE. */
export function useSubscribeToCatalogIndexes() {
  const subscribe = React.useMemo(
    () => buildSubscribeQuery(buildAllIndexesQuery(), { upsertKey: "id" }),
    [],
  );

  return useGlobalUpsertSubscribe({
    atom: catalogIndexes,
    subscribe,
    select: (row) => row.data,
    upsertKey: (row) => row.data.id,
  });
}

/**
 * Returns all indexes on the given object.
 * Data is live (updated via SUBSCRIBE) and available synchronously
 * after the initial snapshot completes.
 *
 * @example
 * ```tsx
 * const indexes = useObjectIndexes("u123");
 * // indexes: CatalogIndex[] for the relation with id "u123"
 * ```
 */
export function useObjectIndexes(objectId: string): CatalogIndex[] {
  const { data } = useAtomValue(catalogIndexes);
  return React.useMemo(
    () => data.filter((idx) => idx.onId === objectId),
    [data, objectId],
  );
}
