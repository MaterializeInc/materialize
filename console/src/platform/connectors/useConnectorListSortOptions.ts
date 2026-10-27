// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";

import { useSyncObjectToSearchParams } from "~/hooks/useSyncObjectToSearchParams";
import { SortOrder } from "~/utils/sort";

export const SORT_OPTIONS = ["name", "status", "type"] as const;
export const DEFAULT_SORT_COLUMN = "status";
export const DEFAULT_SORT_ORDER: SortOrder = "asc";

export type SortField = (typeof SORT_OPTIONS)[number];

export const SORT_COLUMNS = ["name", "status", "type"] as const;

export type ConnectorListSortOption = typeof SORT_COLUMNS;

export function toggleOrder(previousOrder: SortOrder) {
  switch (previousOrder) {
    case "desc":
      return "asc";
    case "asc":
      return "desc";
  }
}

export function useConnectorListSortOptions() {
  const [sortColumn, setSortColumn] =
    React.useState<string>(DEFAULT_SORT_COLUMN);
  const [sortOrder, setSelectedSortOrder] = React.useState(DEFAULT_SORT_ORDER);

  useSyncObjectToSearchParams(
    React.useMemo(
      () => ({
        sortField: sortColumn,
        sortOrder,
      }),
      [sortColumn, sortOrder],
    ),
  );

  const selectedSortColumn = React.useMemo(() => {
    return (
      SORT_COLUMNS.find((column) => column === sortColumn) ??
      DEFAULT_SORT_COLUMN
    );
  }, [sortColumn]);

  const selectedSortOrder = React.useMemo(() => {
    return sortOrder === "asc" || sortOrder === "desc"
      ? sortOrder
      : DEFAULT_SORT_ORDER;
  }, [sortOrder]) as SortOrder;

  const toggleSort = React.useCallback(
    (column: string) => {
      let newSortOrder: SortOrder = DEFAULT_SORT_ORDER;
      if (column === sortColumn) {
        newSortOrder = toggleOrder(selectedSortOrder);
      }
      setSelectedSortOrder(newSortOrder);
      setSortColumn(column);
    },
    [selectedSortOrder, sortColumn],
  );

  return {
    selectedSortColumn,
    selectedSortOrder,
    toggleSort,
  };
}

export type ConnectorListSortOptionsState = ReturnType<
  typeof useConnectorListSortOptions
>;
