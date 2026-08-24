// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Box,
  Icon,
  IconButton,
  Popover,
  PopoverContent,
  PopoverTrigger,
  Skeleton,
  SystemStyleObject,
  Table,
  Tbody,
  Td,
  Tfoot,
  Th,
  Thead,
  Tooltip,
  Tr,
  useTheme,
} from "@chakra-ui/react";
import { flexRender, Header, Row, SortDirection } from "@tanstack/react-table";
import React from "react";

import {
  ChevronDownIcon,
  ChevronRightIcon,
  FilterIcon,
  InfoIcon,
} from "~/icons";
import { MaterializeTheme } from "~/theme";
import { viewportOverflowModifier } from "~/theme/components/Popover";

import { UniversalTableProps } from "./tableTypes";

const SKELETON_ROW_COUNT = 5;

const SortIndicator = ({ direction }: { direction: SortDirection | false }) => {
  if (!direction) return null;
  return (
    <ChevronDownIcon
      ml={1}
      aria-hidden="true"
      transform={direction === "asc" ? "rotate(180deg)" : undefined}
    />
  );
};

const ColumnFilterTrigger = <TData,>({
  header,
}: {
  header: Header<TData, unknown>;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const renderFilter = header.column.columnDef.meta?.renderFilter;
  if (!renderFilter) return null;
  const isActive = header.column.getFilterValue() !== undefined;
  return (
    <Popover
      gutter={2}
      modifiers={viewportOverflowModifier}
      variant="dropdown"
      placement="bottom-end"
    >
      <PopoverTrigger>
        <IconButton
          aria-label={`Filter ${header.column.id}`}
          icon={
            <Icon
              as={FilterIcon}
              color={
                isActive
                  ? colors.accent.brightPurple
                  : colors.foreground.secondary
              }
            />
          }
          size="xs"
          variant="ghost"
          minW="5"
          height="5"
          ml={1}
          onClick={(e) => {
            // Don't bubble into the header's sort-on-click handler.
            e.stopPropagation();
          }}
        />
      </PopoverTrigger>
      <PopoverContent
        motionProps={{ animate: false }}
        onClick={(e) => e.stopPropagation()}
      >
        <Box width="280px">
          {renderFilter(header.column, header.getContext().table)}
        </Box>
      </PopoverContent>
    </Popover>
  );
};

const ColumnHeader = <TData,>({
  header,
}: {
  header: Header<TData, unknown>;
}) => {
  const tooltip = header.column.columnDef.meta?.tooltip;
  const canSort = header.column.getCanSort();
  const canFilter = !!header.column.columnDef.meta?.renderFilter;
  const isNumeric = header.column.columnDef.meta?.isNumeric;

  return (
    <Th
      key={header.id}
      sx={{
        minW: header.column.columnDef.meta?.minWidth,
        width:
          header.column.getSize() !== 150 ? header.column.getSize() : undefined,
        cursor: canSort ? "pointer" : "default",
        userSelect: canSort ? "none" : undefined,
      }}
      onClick={canSort ? header.column.getToggleSortingHandler() : undefined}
    >
      <Box
        display="flex"
        alignItems="center"
        flexWrap="nowrap"
        whiteSpace="nowrap"
        justifyContent={isNumeric ? "flex-end" : undefined}
      >
        {header.isPlaceholder
          ? null
          : flexRender(header.column.columnDef.header, header.getContext())}
        {canSort && <SortIndicator direction={header.column.getIsSorted()} />}
        {tooltip && (
          <Tooltip label={tooltip} lineHeight={1.2}>
            <InfoIcon ml={1} />
          </Tooltip>
        )}
        {canFilter && <ColumnFilterTrigger header={header} />}
      </Box>
    </Th>
  );
};

// The caret column exists only to hold the toggle, so it is sized to the button.
// Child rows leave it empty so that data in the first columns is aligned.
const CARET_SIZE = 8;
const CARET_COLUMN_WIDTH = CARET_SIZE;

// Fallback caret name when the table supplies no `expandLabel`.
const DEFAULT_EXPAND_LABEL = "Display child rows";

const ParentRowCaret = ({
  isOpen,
  label,
  onToggle,
}: {
  isOpen: boolean;
  label: string;
  onToggle: () => void;
}) => (
  <IconButton
    aria-label={label}
    aria-expanded={isOpen}
    variant="ghost"
    w={CARET_SIZE}
    h={CARET_SIZE}
    minW={CARET_SIZE}
    p={0}
    onClick={(event) => {
      // The enclosing row toggles expansion as well. Without this the row's
      // handler fires next and immediately undoes the caret's toggle.
      event.stopPropagation();
      onToggle();
    }}
  >
    <ChevronRightIcon
      aria-hidden="true"
      flexShrink={0}
      transform={isOpen ? "rotate(90deg)" : undefined}
      transition="transform 0.1s"
    />
  </IconButton>
);

const BodyRow = <TData,>({
  row,
  onRowClick,
  rowSx,
  getRowClassName,
  rowTestId,
  expandLabel,
  isGroupTable,
}: {
  row: Row<TData>;
  onRowClick?: (row: TData) => void;
  rowSx?: UniversalTableProps<TData>["rowSx"];
  // callers can pass in a callback that returns a class name for each row.
  // This enables style attribute-level overrides of the defaults.
  getRowClassName?: UniversalTableProps<TData>["getRowClassName"];
  rowTestId?: UniversalTableProps<TData>["rowTestId"];
  expandLabel?: UniversalTableProps<TData>["expandLabel"];
  // True when the table defines `getSubRows`. Such tables carry a leading
  // caret column, kept empty on rows that cannot expand so every row lines up.
  isGroupTable?: boolean;
}) => {
  const { colors, space } = useTheme<MaterializeTheme>();
  // NOTE: an expandable row is assumed to be a group heading from
  // getSubRows. A row-detail expander via getRowCanExpand on flat data
  // would need its own treatment.
  const isParentRow = row.getCanExpand();
  const handleClick = isParentRow
    ? row.getToggleExpandedHandler()
    : onRowClick
      ? () => onRowClick(row.original)
      : undefined;

  // Ledger look: compact borderless rows, each account group opened by
  // a taller top-bordered row, a bordered total row closing the table.
  const defaultRowSx: SystemStyleObject = isGroupTable
    ? {
        td: {
          borderBottomWidth: 0,
          height: "auto",
          paddingBottom: space[3],
          verticalAlign: "top",
        },
        "&[data-parent-row] td": {
          height: "auto",
          borderTopWidth: "1px",
          borderTopStyle: "solid",
          borderTopColor: colors.border.secondary,
          paddingTop: space[3],
          textStyle: "heading-xs",
          verticalAlign: "middle",
        },
        // hide top border of first row because header has a bottom border
        "&[data-parent-row]:first-child td": {
          borderTopWidth: "0",
        },
      }
    : {};

  return (
    <Tr
      onClick={handleClick}
      // Chakra appends its own generated class, so `rowSx` selectors of the
      // form `&.my-class` match on the compound of the two.
      className={getRowClassName?.(row)}
      data-testid={rowTestId?.(row)}
      // A styling hook for `rowSx` selectors that need to target parent rows.
      data-parent-row={isParentRow ? "" : undefined}
      sx={{
        cursor: handleClick ? "pointer" : undefined,
        ...defaultRowSx,
        ...rowSx,
      }}
    >
      {isGroupTable && (
        <Td width={CARET_COLUMN_WIDTH}>
          {isParentRow && (
            <ParentRowCaret
              isOpen={row.getIsExpanded()}
              label={expandLabel?.(row) ?? DEFAULT_EXPAND_LABEL}
              onToggle={row.getToggleExpandedHandler()}
            />
          )}
        </Td>
      )}
      {row.getVisibleCells().map((cell) => (
        <Td
          key={cell.id}
          textAlign={cell.column.columnDef.meta?.isNumeric ? "end" : undefined}
          {...cell.column.columnDef.meta?.cellProps}
        >
          {flexRender(cell.column.columnDef.cell, cell.getContext())}
        </Td>
      ))}
    </Tr>
  );
};

const LoadingRows = ({
  columnCount,
  rowCount,
}: {
  columnCount: number;
  rowCount: number;
}) => (
  <>
    {Array.from({ length: rowCount }).map((_row, rowIndex) => (
      <Tr key={`skeleton-${rowIndex}`}>
        {Array.from({ length: columnCount }).map((_col, colIndex) => (
          <Td key={`skeleton-${rowIndex}-${colIndex}`}>
            <Skeleton height={4} />
          </Td>
        ))}
      </Tr>
    ))}
  </>
);

export const UniversalTable = <TData,>({
  table,
  variant = "linkable",
  onRowClick,
  isLoading = false,
  skeletonRowCount = SKELETON_ROW_COUNT,
  rowSx,
  getRowClassName,
  rowTestId,
  expandLabel,
  footerSx,
  footerTestId,
  "data-testid": testId,
}: UniversalTableProps<TData>) => {
  const headerGroups = table.getHeaderGroups();
  const rows = table.getRowModel().rows;
  const isGroupTable = Boolean(table.options.getSubRows);
  const columnCount = table.getAllColumns().length + (isGroupTable ? 1 : 0);
  const hasFooter = table
    .getAllLeafColumns()
    .some((column) => column.columnDef.footer);

  return (
    <Table variant={variant} data-testid={testId} borderRadius="xl">
      <Thead>
        {headerGroups.map((headerGroup) => (
          <Tr key={headerGroup.id}>
            {isGroupTable && <Th width={CARET_COLUMN_WIDTH} />}
            {headerGroup.headers.map((header) => (
              <ColumnHeader key={header.id} header={header} />
            ))}
          </Tr>
        ))}
      </Thead>
      <Tbody>
        {isLoading ? (
          <LoadingRows columnCount={columnCount} rowCount={skeletonRowCount} />
        ) : (
          rows.map((row) => (
            <BodyRow
              key={row.id}
              row={row}
              onRowClick={onRowClick}
              rowSx={rowSx}
              getRowClassName={getRowClassName}
              rowTestId={rowTestId}
              expandLabel={expandLabel}
              isGroupTable={isGroupTable}
            />
          ))
        )}
      </Tbody>
      {hasFooter && !isLoading && (
        <Tfoot>
          {table.getFooterGroups().map((footerGroup) => (
            <Tr key={footerGroup.id} sx={footerSx} data-testid={footerTestId}>
              {isGroupTable && <Td width={CARET_COLUMN_WIDTH} />}
              {footerGroup.headers.map((header) => (
                <Td
                  key={header.id}
                  textAlign={
                    header.column.columnDef.meta?.isNumeric ? "end" : undefined
                  }
                  {...header.column.columnDef.meta?.cellProps}
                >
                  {header.isPlaceholder
                    ? null
                    : flexRender(
                        header.column.columnDef.footer,
                        header.getContext(),
                      )}
                </Td>
              ))}
            </Tr>
          ))}
        </Tfoot>
      )}
    </Table>
  );
};
