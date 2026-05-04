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
  Skeleton,
  Table,
  Tbody,
  Td,
  Th,
  Thead,
  Tooltip,
  Tr,
} from "@chakra-ui/react";
import { flexRender, Header, SortDirection } from "@tanstack/react-table";
import React from "react";

import { ChevronDownIcon, InfoIcon } from "~/icons";

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

const ColumnHeader = <TData,>({
  header,
}: {
  header: Header<TData, unknown>;
}) => {
  const tooltip = header.column.columnDef.meta?.tooltip;
  const canSort = header.column.getCanSort();

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
      <Box display="flex" alignItems="center">
        {header.isPlaceholder
          ? null
          : flexRender(header.column.columnDef.header, header.getContext())}
        {canSort && <SortIndicator direction={header.column.getIsSorted()} />}
        {tooltip && (
          <Tooltip label={tooltip} lineHeight={1.2}>
            <InfoIcon ml={1} />
          </Tooltip>
        )}
      </Box>
    </Th>
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
  "data-testid": testId,
}: UniversalTableProps<TData>) => {
  const headerGroups = table.getHeaderGroups();
  const rows = table.getRowModel().rows;
  const columnCount = table.getAllColumns().length;

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
            <Tr
              key={row.id}
              onClick={onRowClick ? () => onRowClick(row.original) : undefined}
              sx={{
                cursor: onRowClick ? "pointer" : undefined,
              }}
            >
              {row.getVisibleCells().map((cell) => (
                <Td key={cell.id} {...cell.column.columnDef.meta?.cellProps}>
                  {flexRender(cell.column.columnDef.cell, cell.getContext())}
                </Td>
              ))}
            </Tr>
          ))
        )}
      </Tbody>
    </Table>
  );
};
