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
import { Link as RouterLink, useSearchParams } from "react-router-dom";

import { NULL_LAG_TEXT } from "~/api/materialize/freshness/lagHistory";
import StatusPill from "~/components/StatusPill";
import { sortingFunctions } from "~/components/Table/tableColumnBuilders";
import { TablePagination } from "~/components/Table/TablePagination";
import { UniversalTable } from "~/components/Table/UniversalTable";
import { useUniversalTable } from "~/components/Table/useUniversalTable";
import TextLink from "~/components/TextLink";
import {
  bucketForHydration,
  HYDRATION_LABELS,
  STATUS_COLOR_SCHEMES,
} from "~/platform/maintained-objects/filters";
import { objectExplorerObjectPath } from "~/platform/routeHelpers";
import { useRegionSlug } from "~/store/environments";
import { MaterializeTheme } from "~/theme";
import { kebabToTitleCase } from "~/util";
import { formatDurationExact } from "~/utils/format";

import { FreshnessScope, NO_VALUE, SCOPE_LABELS } from "./constants";
import { FreshnessRow, qualifiedSchemaName, UNREADABLE } from "./freshnessRows";
import { withScope } from "./useFreshnessParams";

const PAGE_SIZE = 25;

/**
 * The object's name, linking to its workflow graph in Object Explorer, which
 * every type this page shows has and which carries its current freshness.
 *
 * A link rather than a click handler on the row: `UniversalTable`'s
 * `onRowClick` binds `onClick` alone, so a row is reachable with a mouse and
 * with nothing else.
 */
const ObjectCell = ({ row }: { row: FreshnessRow }) => {
  const regionSlug = useRegionSlug();
  // Null only for a line whose object is no longer in the list, which has no
  // Object Explorer path to link to.
  const schemaName = row.schemaName;
  return (
    <>
      {schemaName === null ? (
        <Text textStyle="text-ui-med" noOfLines={1}>
          {row.objectName}
        </Text>
      ) : (
        <TextLink
          as={RouterLink}
          to={`${objectExplorerObjectPath(regionSlug, {
            databaseName: row.databaseName,
            schemaName,
            objectType: row.objectType,
            objectName: row.objectName,
            id: row.key,
          })}/workflow`}
          textStyle="text-ui-med"
          noOfLines={1}
        >
          {row.objectName}
        </TextLink>
      )}
    </>
  );
};

/**
 * A statistic, marked when it is the one that put its row over the threshold.
 *
 * The swatch says a row is on the graph; it does not say which of its
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
      textStyle={marked ? "text-ui-med" : undefined}
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

/**
 * Where an object sits in the scope the page is not gathered by: its schema
 * when gathered by cluster, its cluster when gathered by schema.
 */
function placementOf(
  row: FreshnessRow,
  scope: FreshnessScope,
): { scope: FreshnessScope; id: string; label: string } | null {
  if (scope === "cluster") {
    return row.schemaId && row.schemaName
      ? {
          scope: "schema",
          id: row.schemaId,
          label: qualifiedSchemaName(row.databaseName, row.schemaName),
        }
      : null;
  }
  return row.clusterId && row.clusterName
    ? { scope: "cluster", id: row.clusterId, label: row.clusterName }
    : null;
}

/** Links to this page gathered by the object's placement. */
const PlacementCell = ({
  row,
  scope,
}: {
  row: FreshnessRow;
  scope: FreshnessScope;
}) => {
  const [searchParams] = useSearchParams();
  const placement = placementOf(row, scope);
  // A table runs on no cluster.
  if (!placement) return <>{NO_VALUE}</>;

  return (
    <TextLink
      as={RouterLink}
      to={{
        search: withScope(
          searchParams,
          placement.scope,
          placement.id,
        ).toString(),
      }}
      noOfLines={1}
    >
      {placement.label}
    </TextLink>
  );
};

const columnHelper = createColumnHelper<FreshnessRow>();

const buildColumns = (scope: FreshnessScope) => [
  columnHelper.display({
    id: "swatch",
    header: () => <Box aria-label="Graph color" />,
    cell: (info) => <ColorSwatch row={info.row.original} />,
  }),
  columnHelper.accessor("objectName", {
    header: "Object",
    sortingFn: sortingFunctions.nullsLast,
    cell: (info) => <ObjectCell row={info.row.original} />,
    // The Object column takes all the width the others do not need, so every
    // other column, the swatch included, sizes to its content.
    //
    // NOTE: `maxW: 0` is what makes the name truncate. An auto-layout table
    // cell otherwise grows to fit its text, so without it the name would
    // widen the table rather than truncate.
    meta: { cellProps: { width: "100%", maxW: 0 } },
  }),
  // Headed by the scope it links to, the one the page is not gathered by.
  columnHelper.accessor((row) => placementOf(row, scope)?.label ?? null, {
    id: "placement",
    header: SCOPE_LABELS[scope === "cluster" ? "schema" : "cluster"].menu,
    sortingFn: sortingFunctions.nullsLast,
    cell: (info) => <PlacementCell row={info.row.original} scope={scope} />,
  }),
  columnHelper.accessor("objectType", {
    header: "Type",
    // Sorted on the raw value, which orders the same as the formatted one.
    sortingFn: sortingFunctions.nullsLast,
    cell: (info) => kebabToTitleCase(info.getValue()),
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
  columnHelper.accessor("p90", {
    header: "p90",
    sortingFn: sortingFunctions.numericNullsLast,
    cell: (info) => (
      <StatCell value={info.getValue()} row={info.row.original} />
    ),
  }),
  columnHelper.accessor("p99", {
    header: "p99",
    sortingFn: sortingFunctions.numericNullsLast,
    cell: (info) => (
      <StatCell value={info.getValue()} row={info.row.original} />
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
  columnHelper.display({
    id: "hydration",
    header: "Hydration",
    cell: (info) => <HydrationCell row={info.row.original} />,
  }),
];

const COLUMNS = {
  cluster: buildColumns("cluster"),
  schema: buildColumns("schema"),
};

export interface FreshnessTableProps {
  rows: FreshnessRow[];
  /** What the page is gathered by, which decides the placement column. */
  scope: FreshnessScope;
  /** Names the rows in the pagination footer. */
  itemLabel?: string;
}

/**
 * Objects and their freshness.
 *
 * Paginated because "All Objects" lists every object on the cluster, and a
 * cluster can carry thousands. It opens in the order it is given, which is
 * worst first by whatever the active predicate judges.
 */
const FreshnessTableBase = ({
  rows,
  scope,
  itemLabel = "objects",
}: FreshnessTableProps) => {
  const table = useUniversalTable({
    data: rows,
    columns: COLUMNS[scope],
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
export const FreshnessTable = React.memo(FreshnessTableBase);
