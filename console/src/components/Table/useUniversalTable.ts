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
  ColumnFiltersState,
  ExpandedState,
  getCoreRowModel,
  getExpandedRowModel,
  getFilteredRowModel,
  getPaginationRowModel,
  getSortedRowModel,
  OnChangeFn,
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
    /**
     * Convenience shorthand for `initialState.expanded`. Only meaningful for
     * group tables, i.e. when `getSubRows` is also passed. `true` expands
     * every group.
     */
    initialExpanded?: ExpandedState;
  },
) => {
  const {
    initialSorting,
    pageSize = DEFAULT_PAGE_SIZE,
    initialExpanded,
    ...tableOptions
  } = options;

  const [sorting, setSorting] = React.useState<SortingState>(
    initialSorting ?? [],
  );
  const [globalFilter, setGlobalFilter] = React.useState<string>(
    (tableOptions.initialState?.globalFilter as string) ?? "",
  );
  const [columnFilters, setColumnFilters] = React.useState<ColumnFiltersState>(
    tableOptions.initialState?.columnFilters ?? [],
  );
  const [pagination, setPagination] = React.useState<PaginationState>({
    pageIndex: tableOptions.initialState?.pagination?.pageIndex ?? 0,
    pageSize,
  });
  const [expanded, setExpanded] = React.useState<ExpandedState>(
    initialExpanded ?? {},
  );

  const onPaginationChange = tableOptions.onPaginationChange ?? setPagination;
  // With autoResetPageIndex off (see below), TanStack no longer resets the
  // page when a filter changes, which can strand the user on a page past the
  // end of the filtered results. Reset explicitly for both filter kinds.
  const resetPageIndex = () =>
    onPaginationChange((prev) =>
      prev.pageIndex === 0 ? prev : { ...prev, pageIndex: 0 },
    );
  const onGlobalFilterChange: OnChangeFn<string> = (updater) => {
    (tableOptions.onGlobalFilterChange ?? setGlobalFilter)(updater);
    resetPageIndex();
  };
  const onColumnFiltersChange: OnChangeFn<ColumnFiltersState> = (updater) => {
    (tableOptions.onColumnFiltersChange ?? setColumnFilters)(updater);
    resetPageIndex();
  };

  const table = useReactTable({
    ...tableOptions,
    columns: tableOptions.columns as ColumnDef<TData, unknown>[],
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    getExpandedRowModel: getExpandedRowModel(),
    globalFilterFn: tableOptions.globalFilterFn ?? "includesString",
    state: {
      sorting,
      globalFilter,
      columnFilters,
      pagination,
      expanded,
      ...tableOptions.state,
    },
    onSortingChange: tableOptions.onSortingChange ?? setSorting,
    onGlobalFilterChange,
    onColumnFiltersChange,
    onPaginationChange,
    onExpandedChange: tableOptions.onExpandedChange ?? setExpanded,
    // Background data refreshes (e.g., react-query polls) shouldn't snap
    // the user back to page 1. Auto-reset only fires when the user
    // explicitly changes filters/sorts via TanStack's defaults.
    autoResetPageIndex: tableOptions.autoResetPageIndex ?? false,
    // Page size counts group rows only. `false` defers child-row expansion
    // to the pagination model so children render on their parent's page.
    // With manualPagination that model is bypassed entirely, so expansion
    // must happen in the expanded row model instead (`true`).
    paginateExpandedRows:
      tableOptions.paginateExpandedRows ??
      Boolean(tableOptions.manualPagination),
    // Group tables filter from leaf rows: a matching child keeps its parent
    // group visible instead of the whole group being dropped. Scoped to
    // getSubRows tables since leaf filtering re-allocates every row.
    filterFromLeafRows:
      tableOptions.filterFromLeafRows ?? Boolean(tableOptions.getSubRows),
  });

  // TanStack slices the visible page straight from the stored page index, and
  // auto-reset is off (see above), so an index can outlive the rows it was
  // valid for: a page restored from a URL, a data set that shrank, a filter
  // that narrowed. What renders then is a header with no rows beneath it, and
  // `TablePagination` hides itself once there is only one page, so no control
  // is left to page back with. Clamp to the last page that exists.
  //
  // A layout effect rather than an effect: this runs before the browser paints,
  // so the page that never existed is not shown on the way to the one that
  // does.
  //
  // NOTE: skipped under `manualPagination`, where the page count comes from the
  // caller and -1 means "not known yet" rather than "no pages".
  const pageCount = table.getPageCount();
  const { pageIndex } = table.getState().pagination;
  React.useLayoutEffect(() => {
    if (tableOptions.manualPagination) return;
    const lastPage = Math.max(0, pageCount - 1);
    if (pageIndex > lastPage) {
      table.setPageIndex(lastPage);
    }
  }, [table, tableOptions.manualPagination, pageCount, pageIndex]);

  return table;
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
