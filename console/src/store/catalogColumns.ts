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
 * Global live cache of all user-visible columns in the Materialize catalog.
 *
 * ## What
 * Maintains a Jotai atom containing every column for every user object
 * (tables, views, materialized views, sources) via a single long-lived
 * SUBSCRIBE to `mz_columns` joined with `mz_comments`.
 *
 * ## Why
 * Previously, each component that needed column data (catalog tree preview,
 * catalog detail view) opened its own per-object WebSocket SUBSCRIBE on mount,
 * causing a ~1s spinner every time. By subscribing to all columns once at app
 * startup, any component can read column data synchronously from the atom.
 *
 * ## How
 * Follows the three-part pattern used by `allObjects.ts`, `allClusters.ts`, etc.:
 * 1. **Atom** (`catalogColumns`) — holds the current `SubscribeState<CatalogColumn>[]`
 * 2. **Initializer** (`useSubscribeToCatalogColumns`) — called once in `AppInitializer`
 *    to start the SUBSCRIBE WebSocket
 * 3. **Consumer hooks** (`useObjectColumns`, `useObjectDescription`) — filter the
 *    global data by object ID for use in any component
 *
 * ## When
 * The subscribe is started in `AppInitializer.tsx` alongside the other global
 * subscribes. Data becomes available after the initial snapshot completes
 * (typically < 500ms). Consumer hooks return empty arrays until then.
 */

import { atom, useAtomValue } from "jotai";
import { sql } from "kysely";
import React from "react";

import { SubscribeState } from "~/api/materialize/SubscribeManager";
import {
  buildSubscribeQuery,
  useGlobalUpsertSubscribe,
} from "~/api/materialize/useSubscribe";

export interface CatalogColumn {
  objectId: string;
  position: number;
  name: string;
  type: string;
  nullable: boolean;
  columnComment: string | null;
  relationComment: string | null;
}

function buildAllColumnsQuery() {
  return sql<CatalogColumn>`
    SELECT
      ofqn.id AS "objectId",
      c.position,
      c.name,
      c.type,
      c.nullable,
      col_comments.comment AS "columnComment",
      tbl_comments.comment AS "relationComment"
    FROM mz_object_fully_qualified_names ofqn
    JOIN mz_columns c ON c.id = ofqn.id
    LEFT JOIN mz_comments col_comments
      ON col_comments.id = ofqn.id
     AND col_comments.object_sub_id = c.position
    LEFT JOIN mz_comments tbl_comments
      ON tbl_comments.id = ofqn.id
     AND tbl_comments.object_sub_id IS NULL
  `;
}

export const catalogColumns = atom<SubscribeState<CatalogColumn>>({
  data: [],
  error: undefined,
  snapshotComplete: false,
});

/** Call once in AppInitializer to start the global columns SUBSCRIBE. */
export function useSubscribeToCatalogColumns() {
  const subscribe = React.useMemo(
    () =>
      buildSubscribeQuery(buildAllColumnsQuery(), {
        upsertKey: ["objectId", "name"],
      }),
    [],
  );

  return useGlobalUpsertSubscribe({
    atom: catalogColumns,
    subscribe,
    select: (row) => row.data,
    upsertKey: (row) => `${row.data.objectId}:${row.data.name}`,
  });
}

/**
 * Returns all columns for the given object, sorted by position.
 * Data is live (updated via SUBSCRIBE) and available synchronously
 * after the initial snapshot completes.
 *
 * @example
 * ```tsx
 * const columns = useObjectColumns("u123");
 * // columns: CatalogColumn[] sorted by position
 * ```
 */
export function useObjectColumns(objectId: string): CatalogColumn[] {
  const { data } = useAtomValue(catalogColumns);
  return React.useMemo(
    () =>
      data
        .filter((c) => c.objectId === objectId)
        .sort((a, b) => Number(a.position) - Number(b.position)),
    [data, objectId],
  );
}

/**
 * Returns the relation-level comment (description) for the given object,
 * or null if none exists. Reads from the global columns cache.
 *
 * @example
 * ```tsx
 * const description = useObjectDescription("u123");
 * // description: string | null
 * ```
 */
export function useObjectDescription(objectId: string): string | null {
  const columns = useObjectColumns(objectId);
  return columns.length > 0 ? columns[0].relationComment : null;
}
