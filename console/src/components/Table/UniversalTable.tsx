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

// Caret width (4) + caret/label gap (2), so an indented child row's label
// lines up just past its parent row's caret.
const CHILD_ROW_INDENT = 6;

const GroupRowCaret = ({ isOpen }: { isOpen: boolean }) => (
  <ChevronRightIcon
    aria-hidden="true"
    flexShrink={0}
    marginRight={2}
    transform={isOpen ? "rotate(90deg)" : undefined}
    transition="transform 0.1s"
  />
);

const BodyRow = <TData,>({
  row,
  onRowClick,
  rowSx,
  rowTestId,
}: {
  row: Row<TData>;
  onRowClick?: (row: TData) => void;
  rowSx?: UniversalTableProps<TData>["rowSx"];
  rowTestId?: UniversalTableProps<TData>["rowTestId"];
}) => {
  // NOTE: an expandable row is assumed to be a group heading from
  // getSubRows. A row-detail expander via getRowCanExpand on flat data
  // would need its own treatment.
  const isGroupRow = row.getCanExpand();
  const needsIndent = isGroupRow || row.depth > 0;
  const handleClick = isGroupRow
    ? row.getToggleExpandedHandler()
    : onRowClick
      ? () => onRowClick(row.original)
      : undefined;
  const groupRowProps = isGroupRow
    ? {
        "aria-expanded": row.getIsExpanded(),
        tabIndex: 0,
        onKeyDown: (e: React.KeyboardEvent) => {
          if (e.key === "Enter" || e.key === " ") {
            e.preventDefault();
            row.toggleExpanded();
          }
        },
      }
    : undefined;

  return (
    <Tr
      onClick={handleClick}
      data-testid={rowTestId?.(row)}
      {...groupRowProps}
      sx={{
        cursor: handleClick ? "pointer" : undefined,
        ...(isGroupRow && { td: { textStyle: "heading-xs" } }),
        ...rowSx,
      }}
    >
      {row.getVisibleCells().map((cell, cellIndex) => {
        const content = flexRender(
          cell.column.columnDef.cell,
          cell.getContext(),
        );
        return (
          <Td
            key={cell.id}
            textAlign={
              cell.column.columnDef.meta?.isNumeric ? "end" : undefined
            }
            {...cell.column.columnDef.meta?.cellProps}
          >
            {cellIndex === 0 && needsIndent ? (
              <Box
                display="flex"
                alignItems="center"
                paddingLeft={row.depth * CHILD_ROW_INDENT}
              >
                {isGroupRow && <GroupRowCaret isOpen={row.getIsExpanded()} />}
                {content}
              </Box>
            ) : (
              content
            )}
          </Td>
        );
      })}
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
  rowTestId,
  footerSx,
  footerTestId,
  "data-testid": testId,
}: UniversalTableProps<TData>) => {
  const headerGroups = table.getHeaderGroups();
  const rows = table.getRowModel().rows;
  const columnCount = table.getAllColumns().length;
  const hasFooter = table
    .getAllLeafColumns()
    .some((column) => column.columnDef.footer);

  return (
    <Table variant={variant} data-testid={testId} borderRadius="xl">
      <Thead>
        {headerGroups.map((headerGroup) => (
          <Tr key={headerGroup.id}>
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
              rowTestId={rowTestId}
            />
          ))
        )}
      </Tbody>
      {hasFooter && !isLoading && (
        <Tfoot>
          {table.getFooterGroups().map((footerGroup) => (
            <Tr key={footerGroup.id} sx={footerSx} data-testid={footerTestId}>
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
