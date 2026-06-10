// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  ColumnDef,
  getCoreRowModel,
  getFilteredRowModel,
  getPaginationRowModel,
  getSortedRowModel,
  PaginationState,
  SortingState,
  TableOptions,
  useReactTable,
} from "@tanstack/react-table";
import React from "react";

const DEFAULT_PAGE_SIZE = 25;

/**
 * Thin wrapper around TanStack's `useReactTable` with sensible defaults
 * for sorting, filtering, and pagination row models. Pass any TanStack
 * option directly; this hook does not introduce custom `enable*`
 * wrapper props.
 *
 * @see https://tanstack.com/table/v8/docs/api/core/table
 */
export const useUniversalTable = <TData>(
  options: Omit<TableOptions<TData>, "getCoreRowModel"> & {
    /** Convenience shorthand for `initialState.sorting`. */
    initialSorting?: SortingState;
    /** Convenience shorthand for `initialState.pagination.pageSize`. Defaults to 25. */
    pageSize?: number;
  },
) => {
  const {
    initialSorting,
    pageSize = DEFAULT_PAGE_SIZE,
    ...tableOptions
  } = options;

  const [sorting, setSorting] = React.useState<SortingState>(
    initialSorting ?? [],
  );
  const [globalFilter, setGlobalFilter] = React.useState<string>(
    (tableOptions.initialState?.globalFilter as string) ?? "",
  );
  const [pagination, setPagination] = React.useState<PaginationState>({
    pageIndex: tableOptions.initialState?.pagination?.pageIndex ?? 0,
    pageSize,
  });

  return useReactTable({
    ...tableOptions,
    columns: tableOptions.columns as ColumnDef<TData, unknown>[],
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    globalFilterFn: tableOptions.globalFilterFn ?? "includesString",
    state: {
      sorting,
      globalFilter,
      pagination,
      ...tableOptions.state,
    },
    onSortingChange: tableOptions.onSortingChange ?? setSorting,
    onGlobalFilterChange: tableOptions.onGlobalFilterChange ?? setGlobalFilter,
    onPaginationChange: tableOptions.onPaginationChange ?? setPagination,
    // Background data refreshes (e.g., react-query polls) shouldn't snap
    // the user back to page 1. Auto-reset only fires when the user
    // explicitly changes filters/sorts via TanStack's defaults.
    autoResetPageIndex: tableOptions.autoResetPageIndex ?? false,
  });
};

/**
 * Parses sort, page, and search from a URL search string into initial state
 * for `useUniversalTable`. See `console/doc/guide-tanstack-table.md` for the
 * full URL-sync pattern (read on mount, write on change).
 *
 * URL format: `?sort=<columnId>&dir=asc|desc&page=<1-based>&q=<search>`
 */
export const getInitialTableState = (search: string) => {
  const params = new URLSearchParams(search);
  const sort = params.get("sort");
  const page = params.get("page");
  const parsedPage = page ? parseInt(page, 10) : NaN;
  return {
    sorting: sort
      ? ([{ id: sort, desc: params.get("dir") === "desc" }] as SortingState)
      : undefined,
    pageIndex: Number.isFinite(parsedPage)
      ? Math.max(0, parsedPage - 1)
      : undefined,
    globalFilter: params.get("q") ?? undefined,
  };
};
