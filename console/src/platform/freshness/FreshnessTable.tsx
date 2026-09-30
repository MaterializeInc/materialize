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
  Table,
  Tbody,
  Td,
  Text,
  Th,
  Thead,
  Tr,
  useTheme,
} from "@chakra-ui/react";
import React from "react";

import { NULL_LAG_TEXT } from "~/api/materialize/freshness/lagHistory";
import StatusPill from "~/components/StatusPill";
import {
  bucketForHydration,
  HYDRATION_LABELS,
  STATUS_COLOR_SCHEMES,
} from "~/platform/maintained-objects/filters";
import { MaterializeTheme } from "~/theme";
import { truncateMaxWidth } from "~/theme/components/Table";
import { formatDurationForAxis } from "~/utils/format";

import { FreshnessRow, SortKey, sortRows } from "./freshnessRows";

const COLUMNS: { key: SortKey; label: string; numeric: boolean }[] = [
  { key: "objectName", label: "Object", numeric: false },
  { key: "objectType", label: "Type", numeric: false },
  { key: "current", label: "Now", numeric: true },
  { key: "peak", label: "Peak", numeric: true },
  { key: "p90", label: "p90", numeric: true },
];

const HydrationPill = ({ row }: { row: FreshnessRow }) => {
  const bucket = bucketForHydration(row.hydratedReplicas, row.totalReplicas);
  // No rows in `mz_hydration_statuses` for the object yet.
  if (!bucket) return <>-</>;

  return (
    <StatusPill
      status={bucket}
      label={HYDRATION_LABELS[bucket]}
      colorScheme={STATUS_COLOR_SCHEMES[bucket]}
    />
  );
};

export interface FreshnessTableProps {
  rows: FreshnessRow[];
  emptyMessage: React.ReactNode;
  /** Clicking a row toggles it onto the graph. Omitted where that is not offered. */
  onToggleRow?: (key: string) => void;
}

/**
 * Objects and their freshness, sortable.
 *
 * The swatch is the only link between a row and its line, so it appears exactly
 * where a line is drawn in color: on everything over the threshold, plus
 * anything picked by hand. A value over the threshold is marked in the cell as
 * well, because the swatch says "this is on the graph" and not which reading
 * put it there.
 */
export const FreshnessTable = ({
  rows,
  emptyMessage,
  onToggleRow,
}: FreshnessTableProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const [sort, setSort] = React.useState<{ key: SortKey; direction: 1 | -1 }>({
    key: "peak",
    direction: -1,
  });

  const sorted = React.useMemo(
    () => sortRows(rows, sort.key, sort.direction),
    [rows, sort],
  );

  if (rows.length === 0) {
    return (
      <Box padding="4" color={colors.foreground.secondary}>
        {emptyMessage}
      </Box>
    );
  }

  const cell = (value: number | null, breaching: boolean) => (
    <Td
      isNumeric
      color={breaching ? colors.accent.red : undefined}
      fontWeight={breaching ? "500" : undefined}
    >
      {value === null ? "—" : formatDurationForAxis(value)}
    </Td>
  );

  /**
   * The Now cell, which is where an unreadable object is called out. Its
   * `breachValue` is `Infinity` so that it sorts and highlights as the worst
   * row, and naming the state here is what keeps that number off the screen.
   */
  const nowCell = (row: FreshnessRow) =>
    row.notQueryable ? (
      <Td isNumeric color={colors.accent.red} fontWeight="500">
        {NULL_LAG_TEXT}
      </Td>
    ) : (
      cell(row.current, row.breaching && row.breachValue === row.current)
    );

  return (
    <Table variant="standalone">
      <Thead>
        <Tr>
          <Th width="8" aria-label="Graph color" />
          {COLUMNS.map((column) => (
            <Th
              key={column.key}
              isNumeric={column.numeric}
              cursor="pointer"
              userSelect="none"
              aria-sort={
                sort.key === column.key
                  ? sort.direction === -1
                    ? "descending"
                    : "ascending"
                  : "none"
              }
              onClick={() =>
                setSort((prev) =>
                  prev.key === column.key
                    ? {
                        key: column.key,
                        direction: prev.direction === 1 ? -1 : 1,
                      }
                    : // First click on a new column picks the direction that
                      // column is usually read in: durations worst first, names
                      // alphabetically.
                      { key: column.key, direction: column.numeric ? -1 : 1 },
                )
              }
            >
              {column.label}
              {sort.key === column.key && (sort.direction === -1 ? " ▾" : " ▴")}
            </Th>
          ))}
          <Th>Hydration</Th>
        </Tr>
      </Thead>
      <Tbody>
        {sorted.map((row) => (
          <Tr
            key={row.key}
            cursor={onToggleRow ? "pointer" : undefined}
            _hover={
              onToggleRow
                ? { background: colors.background.secondary }
                : undefined
            }
            onClick={onToggleRow ? () => onToggleRow(row.key) : undefined}
          >
            <Td py="2" pr="0">
              {row.color && (
                <Box
                  width="10px"
                  height="10px"
                  borderRadius="2px"
                  background={row.color}
                  role="img"
                  aria-label="Shown on the graph"
                />
              )}
            </Td>
            <Td {...truncateMaxWidth} py="2">
              <Text noOfLines={1}>{row.objectName}</Text>
              {row.namespace && (
                <Text
                  textStyle="text-small"
                  color={colors.foreground.secondary}
                  noOfLines={1}
                >
                  {row.namespace}
                </Text>
              )}
            </Td>
            <Td>
              <Text textStyle="text-ui-sm" color={colors.foreground.secondary}>
                {row.objectType}
              </Text>
            </Td>
            {nowCell(row)}
            {cell(row.peak, row.breaching && row.breachValue === row.peak)}
            {cell(row.p90, row.breaching && row.breachValue === row.p90)}
            <Td>
              <HydrationPill row={row} />
            </Td>
          </Tr>
        ))}
      </Tbody>
    </Table>
  );
};
