// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { HTMLChakraProps } from "@chakra-ui/react";
import { Column, Row, RowData, Table } from "@tanstack/react-table";
import React from "react";

/**
 * Variants matching the Chakra UI table theme in
 * `src/theme/components/Table.ts`.
 */
export type TableVariant =
  | "linkable"
  | "standalone"
  | "rounded"
  | "shell"
  | "borderless";

export interface UniversalTableProps<TData> {
  /** TanStack table instance returned by `useUniversalTable`. */
  table: Table<TData>;
  /** Chakra table variant. Defaults to `"linkable"`. */
  variant?: TableVariant;
  /**
   * Callback when a leaf row is clicked. Group rows toggle their expansion
   * on click instead and never fire this.
   */
  onRowClick?: (row: TData) => void;
  /** Show a loading skeleton while data is being fetched. */
  isLoading?: boolean;
  /** Number of skeleton rows to display when loading. */
  skeletonRowCount?: number;
  /** Chakra `sx` merged onto every body `<Tr>`. */
  rowSx?: HTMLChakraProps<"tr">["sx"];
  /**
   * Returns a class name for each body `<Tr>`, for styling that depends on the
   * row's data. Declare the styles as selectors in `rowSx` and switch on them
   * here:
   *
   * ```tsx
   * rowSx={{ td: { height: "8" }, "&.cluster-row td": { height: "16" } }}
   * getRowClassName={(row) =>
   *   row.original.rowType === "cluster" ? "cluster-row" : undefined
   * }
   * ```
   *
   * Preferred over returning a style object per row: the whole table serializes
   * one class instead of one per row, and CSS specificity resolves overlapping
   * declarations instead of an object spread silently replacing them.
   *
   * NOTE: a typo in either the name or the selector fails silently, so share a
   * constant between the two.
   */
  getRowClassName?: (row: Row<TData>) => string | undefined;
  /**
   * Returns a `data-testid` for each body `<Tr>`. Group rows are
   * distinguishable via `row.getCanExpand()`.
   */
  rowTestId?: (row: Row<TData>) => string | undefined;
  /**
   * Accessible name for a group row's expand caret, called only for rows that
   * can expand. Without it every caret in the table shares one generic name,
   * leaving screen reader users unable to tell them apart, so name the row's
   * subject: "Show replicas of quickstart". Returning `undefined` falls back to
   * that generic name.
   */
  expandLabel?: (row: Row<TData>) => string | undefined;
  /** Chakra `sx` merged onto the footer `<Tr>`. */
  footerSx?: HTMLChakraProps<"tr">["sx"];
  /** `data-testid` for the footer `<Tr>`. */
  footerTestId?: string;
  /** `data-testid` forwarded to the root `<Table>` element. */
  "data-testid"?: string;
}

export interface TablePaginationProps<TData> {
  table: Table<TData>;
  /** Pluralized item label, e.g. "clusters". Defaults to `"results"`. */
  itemLabel?: React.ReactNode;
}

export interface TableSearchProps {
  /**
   * Initial search value. Read once on mount; the component is uncontrolled
   * thereafter. Pass a `key` prop to force a reset.
   */
  initialValue?: string;
  /** Called with the new search value after the debounce window. */
  onValueChange: (value: string) => void;
  /** Placeholder text. Defaults to `"Search"`. */
  placeholder?: string;
  /** Debounce delay in milliseconds. Defaults to `250`. */
  debounceMs?: number;
}

/**
 * Adds Chakra-specific fields to TanStack's `ColumnMeta` (tooltip, responsive
 * width, per-cell Chakra props) that `UniversalTable` reads when rendering.
 * See `console/doc/guide-tanstack-table.md`.
 *
 * `TData` / `TValue` must match TanStack's declaration for interface merging,
 * even though we don't reference them here.
 */
declare module "@tanstack/react-table" {
  interface ColumnMeta<TData extends RowData, TValue> {
    /** Tooltip shown on the column header. */
    tooltip?: string;
    /** Responsive min-width for the column (Chakra responsive object). */
    minWidth?: Record<string, string> | string;
    /**
     * Chakra props spread onto the `<Td>` for every cell in this column,
     * including its footer cell when the column defines a `footer`.
     */
    cellProps?: HTMLChakraProps<"td">;
    /** Right-align the column: header, body cells, and footer cell. */
    isNumeric?: boolean;
    /**
     * Renders a per-column filter UI inside a popover anchored on the column
     * header. When defined, `UniversalTable` shows a small filter trigger next
     * to the sort caret (with an active-state dot when the column has a
     * filter value applied). Receives `table` so the panel can pull shared
     * data (e.g. dropdown options) from `table.options.meta`.
     */
    renderFilter?: (
      column: Column<TData, TValue>,
      table: Table<TData>,
    ) => React.ReactNode;
  }
}
