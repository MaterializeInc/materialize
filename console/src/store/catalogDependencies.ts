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
 * Global live cache of all object dependencies in the Materialize catalog.
 *
 * ## What
 * Maintains a Jotai atom containing every dependency edge from
 * `mz_internal.mz_object_dependencies`, enriched with object names and types
 * from `mz_object_fully_qualified_names` and `mz_objects`.
 *
 * ## Why
 * Previously, the catalog detail view opened two per-object WebSocket
 * SUBSCRIBEs (one for "depends on", one for "referenced by") on every mount.
 * By subscribing once at startup, both dependency directions are available
 * instantly from any component via client-side filtering.
 *
 * ## How
 * Follows the three-part pattern (atom → initializer → consumer hooks).
 * See `catalogColumns.ts` module doc for details.
 *
 * The raw data stores each edge as (objectId → dependencyId). Consumer hooks
 * filter in opposite directions:
 * - `useObjectDependencies(id)` — what does this object depend on?
 * - `useObjectReferencedBy(id)` — what depends on this object?
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

export interface CatalogDependency {
  /** The object that has the dependency (the "from" side). */
  objectId: string;
  objectName: string;
  objectSchemaName: string | null;
  objectDatabaseName: string | null;
  objectType: string;
  /** The object being depended on (the "to" side). */
  dependencyId: string;
  dependencyName: string;
  dependencySchemaName: string | null;
  dependencyDatabaseName: string | null;
  dependencyObjectType: string;
}

export const catalogDependencies = atom<SubscribeState<CatalogDependency>>({
  data: [],
  error: undefined,
  snapshotComplete: false,
});

/** Call once in AppInitializer to start the global dependencies SUBSCRIBE. */
export function useSubscribeToCatalogDependencies() {
  const subscribe = React.useMemo(() => {
    const query = sql<CatalogDependency>`
      SELECT
        d.object_id AS "objectId",
        obj_fqn.name AS "objectName",
        obj_fqn.schema_name AS "objectSchemaName",
        obj_fqn.database_name AS "objectDatabaseName",
        obj_o.type AS "objectType",
        d.referenced_object_id AS "dependencyId",
        dep_fqn.name AS "dependencyName",
        dep_fqn.schema_name AS "dependencySchemaName",
        dep_fqn.database_name AS "dependencyDatabaseName",
        dep_o.type AS "dependencyObjectType"
      FROM mz_internal.mz_object_dependencies d
      JOIN mz_object_fully_qualified_names obj_fqn ON obj_fqn.id = d.object_id
      JOIN mz_objects obj_o ON obj_o.id = d.object_id
      JOIN mz_object_fully_qualified_names dep_fqn ON dep_fqn.id = d.referenced_object_id
      JOIN mz_objects dep_o ON dep_o.id = d.referenced_object_id
      WHERE obj_o.type IN ('connection','index','materialized-view','secret','sink','source','table','view')
        AND dep_o.type IN ('connection','index','materialized-view','secret','sink','source','table','view')
    `;
    return buildSubscribeQuery(query, {
      upsertKey: ["objectId", "dependencyId"],
    });
  }, []);

  return useGlobalUpsertSubscribe({
    atom: catalogDependencies,
    subscribe,
    select: (row) => row.data,
    upsertKey: (row) => `${row.data.objectId}:${row.data.dependencyId}`,
  });
}

/** Row shape returned by dependency consumer hooks, matching the existing DepRow interface. */
export interface DepRow {
  id: string;
  name: string;
  schemaName: string | null;
  databaseName: string | null;
  objectType: string;
}

/**
 * Returns upstream dependencies for the given object (what it depends on).
 * Data is live (updated via SUBSCRIBE) and available synchronously
 * after the initial snapshot completes.
 *
 * @example
 * ```tsx
 * const deps = useObjectDependencies("u123");
 * // deps: DepRow[] — objects that u123 depends on
 * ```
 */
export function useObjectDependencies(objectId: string): DepRow[] {
  const { data } = useAtomValue(catalogDependencies);
  return React.useMemo(
    () =>
      data
        .filter((d) => d.objectId === objectId)
        .map((d) => ({
          id: d.dependencyId,
          name: d.dependencyName,
          schemaName: d.dependencySchemaName,
          databaseName: d.dependencyDatabaseName,
          objectType: d.dependencyObjectType,
        })),
    [data, objectId],
  );
}

/**
 * Returns downstream dependents for the given object (what depends on it).
 * Data is live (updated via SUBSCRIBE) and available synchronously
 * after the initial snapshot completes.
 *
 * @example
 * ```tsx
 * const refs = useObjectReferencedBy("u123");
 * // refs: DepRow[] — objects that depend on u123
 * ```
 */
export function useObjectReferencedBy(objectId: string): DepRow[] {
  const { data } = useAtomValue(catalogDependencies);
  return React.useMemo(
    () =>
      data
        .filter((d) => d.dependencyId === objectId)
        .map((d) => ({
          id: d.objectId,
          name: d.objectName,
          schemaName: d.objectSchemaName,
          databaseName: d.objectDatabaseName,
          objectType: d.objectType,
        })),
    [data, objectId],
  );
}
