// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Text, useTheme, VStack } from "@chakra-ui/react";
import { createColumnHelper } from "@tanstack/react-table";
import React from "react";

import { NULL_LAG_TEXT } from "~/api/materialize/freshness/lagHistory";
import StatusPill from "~/components/StatusPill";
import { sortingFunctions } from "~/components/Table/tableColumnBuilders";
import { TablePagination } from "~/components/Table/TablePagination";
import { UniversalTable } from "~/components/Table/UniversalTable";
import { useUniversalTable } from "~/components/Table/useUniversalTable";
import {
  bucketForHydration,
  HYDRATION_LABELS,
  STATUS_COLOR_SCHEMES,
} from "~/platform/maintained-objects/filters";
import { MaterializeTheme } from "~/theme";
import { truncateMaxWidth } from "~/theme/components/Table";
import { formatDurationExact } from "~/utils/format";

import { FreshnessRow, UNREADABLE } from "./freshnessRows";

const PAGE_SIZE = 25;

/** Shown where a statistic has no value, matching the rest of the Console. */
const NO_VALUE = "—";

const ObjectCell = ({ row }: { row: FreshnessRow }) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <>
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
    </>
  );
};

/**
 * A statistic, marked when it is the one that put its row over the threshold.
 *
 * The swatch says a row is on the graph; it does not say which of its three
 * numbers is responsible, so the responsible one is marked here.
 */
const StatCell = ({
  value,
  row,
}: {
  value: number | null;
  row: FreshnessRow;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const unreadable = value === UNREADABLE;
  const marked = unreadable || (row.breaching && row.breachValue === value);

  return (
    <Text
      color={marked ? colors.accent.red : undefined}
      fontWeight={marked ? "500" : undefined}
    >
      {value === null
        ? NO_VALUE
        : unreadable
          ? NULL_LAG_TEXT
          : formatDurationExact(value)}
    </Text>
  );
};

const HydrationCell = ({ row }: { row: FreshnessRow }) => {
  const bucket = bucketForHydration(row.hydratedReplicas, row.totalReplicas);
  // No rows in `mz_hydration_statuses` for the object yet.
  if (!bucket) return <>{NO_VALUE}</>;

  return (
    <StatusPill
      status={bucket}
      label={HYDRATION_LABELS[bucket]}
      colorScheme={STATUS_COLOR_SCHEMES[bucket]}
    />
  );
};

const ColorSwatch = ({ row }: { row: FreshnessRow }) => {
  if (!row.color) return null;
  return (
    <Box
      boxSize="2.5"
      borderRadius="sm"
      background={row.color}
      role="img"
      aria-label="Shown on the graph"
    />
  );
};

const columnHelper = createColumnHelper<FreshnessRow>();

const columns = [
  columnHelper.display({
    id: "swatch",
    header: () => <Box aria-label="Graph color" />,
    cell: (info) => <ColorSwatch row={info.row.original} />,
    size: 32,
  }),
  columnHelper.accessor("objectName", {
    header: "Object",
    sortingFn: sortingFunctions.nullsLast,
    cell: (info) => <ObjectCell row={info.row.original} />,
    meta: { cellProps: truncateMaxWidth },
  }),
  columnHelper.accessor("objectType", {
    header: "Type",
    sortingFn: sortingFunctions.nullsLast,
  }),
  columnHelper.accessor("current", {
    header: "Now",
    // `numericNullsLast` rather than the text collation, which compares 12.48
    // as (12, 48) against 12.5 as (12, 5) and calls the first one larger.
    sortingFn: sortingFunctions.numericNullsLast,
    cell: (info) => (
      <StatCell value={info.row.original.current} row={info.row.original} />
    ),
  }),
  columnHelper.accessor("peak", {
    // Named as Monitoring's Objects page names it, so the same statistic does
    // not carry two names across the product.
    header: "pMAX",
    sortingFn: sortingFunctions.numericNullsLast,
    cell: (info) => (
      <StatCell value={info.getValue()} row={info.row.original} />
    ),
  }),
  columnHelper.accessor("p90", {
    header: "p90",
    sortingFn: sortingFunctions.numericNullsLast,
    cell: (info) => (
      <StatCell value={info.getValue()} row={info.row.original} />
    ),
  }),
  columnHelper.display({
    id: "hydration",
    header: "Hydration",
    cell: (info) => <HydrationCell row={info.row.original} />,
  }),
];

export interface FreshnessTableProps {
  rows: FreshnessRow[];
  /** Names the rows in the pagination footer. */
  itemLabel?: string;
}

/**
 * Objects and their freshness.
 *
 * Paginated because "All objects" lists every object on the cluster, and a
 * cluster can carry thousands. It opens in the order it is given, which is
 * worst first by whatever the active predicate judges.
 */
const FreshnessTableInner = ({
  rows,
  itemLabel = "objects",
}: FreshnessTableProps) => {
  const table = useUniversalTable({
    data: rows,
    columns,
    // `buildFreshnessRows` orders worst first by `breachValue`, which is
    // whichever statistic the active predicate judges, so the incoming order
    // already follows the predicate.
    //
    // NOTE: a descending sort reverses the nulls-last comparators, so naming a
    // column here puts the rows with nothing to judge at the top.
    initialSorting: [],
    pageSize: PAGE_SIZE,
    getRowId: (row) => row.key,
  });

  return (
    <VStack spacing="4" alignItems="stretch" width="100%">
      <UniversalTable table={table} variant="standalone" />
      {rows.length > PAGE_SIZE && (
        <TablePagination table={table} itemLabel={itemLabel} />
      )}
    </VStack>
  );
};

/**
 * Memoized because a threshold drag re-renders the page on every pointer move,
 * and re-rendering a row per object at that rate is what makes the drag
 * stutter. The props it receives are memoized upstream for the same reason: a
 * single rebuilt array here would make this memo a no-op.
 */
export const FreshnessTable = React.memo(FreshnessTableInner);
