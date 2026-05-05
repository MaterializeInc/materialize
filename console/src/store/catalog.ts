// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { atom, useAtomValue, useSetAtom } from "jotai";
import { atomWithStorage } from "jotai/utils";
import React, { useCallback } from "react";
import { useNavigate } from "react-router-dom";

import { monitorPath, shellPath } from "~/platform/routeHelpers";
import type { CatalogSelection } from "~/platform/worksheet/CatalogPanel";
import { allObjects } from "~/store/allObjects";
import {
  type CatalogColumn,
  useObjectColumns,
  useObjectDescription,
} from "~/store/catalogColumns";
import {
  type DepRow,
  useObjectDependencies,
  useObjectReferencedBy,
} from "~/store/catalogDependencies";
import { type CatalogIndex, useObjectIndexes } from "~/store/catalogIndexes";
import { useRegionSlug } from "~/store/environments";

/** Whether the data catalog panel is visible. Persisted to localStorage. */
export const catalogVisibleAtom = atomWithStorage(
  "worksheet-catalog-visible",
  false,
);

/** The catalog object currently shown in the detail view, or undefined for browse mode. */
export const catalogDetailAtom = atom<CatalogSelection | undefined>(undefined);

/**
 * Returns a function that navigates to the worksheet shell page and opens
 * the catalog detail view for the given object.
 */
export function useOpenCatalogDetail() {
  const setVisible = useSetAtom(catalogVisibleAtom);
  const setDetail = useSetAtom(catalogDetailAtom);
  const navigate = useNavigate();
  const regionSlug = useRegionSlug();

  return useCallback(
    (selection: CatalogSelection) => {
      setDetail(selection);
      setVisible(true);
      navigate(shellPath(regionSlug));
    },
    [setDetail, setVisible, navigate, regionSlug],
  );
}

/**
 * Returns a function that navigates to the connector monitor page and opens
 * the catalog detail sidebar for the given object.
 */
export function useOpenCatalogMonitor() {
  const setVisible = useSetAtom(catalogVisibleAtom);
  const setDetail = useSetAtom(catalogDetailAtom);
  const navigate = useNavigate();
  const regionSlug = useRegionSlug();

  return useCallback(
    (selection: CatalogSelection) => {
      setDetail(selection);
      setVisible(true);
      navigate(monitorPath(regionSlug, selection.id));
    },
    [setDetail, setVisible, navigate, regionSlug],
  );
}

/**
 * Returns the owner role name for the given object, or undefined if not found.
 * Reads from the global `allObjects` subscribe which includes `owner`
 * via the `mz_roles` join.
 */
export function useObjectOwner(objectId: string): string | undefined {
  const { data } = useAtomValue(allObjects);
  return React.useMemo(() => {
    const obj = data.find((o) => o.id === objectId);
    return obj?.owner ?? undefined;
  }, [data, objectId]);
}

/**
 * Unified hook for accessing all catalog metadata for a single object.
 *
 * ## Catalog Data Layer Architecture
 *
 * The catalog data layer maintains live local replicas of key Materialize
 * system catalog tables via long-lived WebSocket SUBSCRIBEs. Each data type
 * has a global Jotai atom that is populated by a single SUBSCRIBE started
 * in `AppInitializer`:
 *
 * | Data | Store | Subscribe target |
 * |------|-------|-----------------|
 * | Objects | `allObjects` | `mz_objects` + `mz_object_fully_qualified_names` |
 * | Columns | `catalogColumns` | `mz_columns` + `mz_comments` |
 * | Indexes | `catalogIndexes` | `mz_indexes` + `mz_show_indexes` + `mz_clusters` |
 * | Dependencies | `catalogDependencies` | `mz_internal.mz_object_dependencies` |
 * | Owner | (in `allObjects`) | `mz_roles` joined in allObjects query |
 *
 * Components should access catalog data through this hook or the individual
 * `useObject*` hooks. **Do not open per-object SUBSCRIBEs** — the data is
 * already available locally via the global subscribes.
 *
 */
export function useCatalogObject(objectId: string): {
  columns: CatalogColumn[];
  description: string | null;
  indexes: CatalogIndex[];
  owner: string | undefined;
  dependencies: DepRow[];
  referencedBy: DepRow[];
} {
  const columns = useObjectColumns(objectId);
  const description = useObjectDescription(objectId);
  const indexes = useObjectIndexes(objectId);
  const owner = useObjectOwner(objectId);
  const dependencies = useObjectDependencies(objectId);
  const referencedBy = useObjectReferencedBy(objectId);
  return { columns, description, indexes, owner, dependencies, referencedBy };
}
