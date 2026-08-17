// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { HStack, Text, Tooltip, useTheme, VStack } from "@chakra-ui/react";
import { createColumnHelper } from "@tanstack/react-table";
import React from "react";

import {
  ClusterWithOwnership,
  Replica,
} from "~/api/materialize/cluster/clusterList";
import useLatestOfflineReplica, {
  LatestOfflineReplicaInfo,
} from "~/api/materialize/cluster/useLatestOfflineReplica";
import { OVERFLOW_BUTTON_WIDTH } from "~/components/OverflowMenu";
import PercentBar from "~/components/PercentBar";
import { sortingFunctions } from "~/components/Table/tableColumnBuilders";
import { TablePagination } from "~/components/Table/TablePagination";
import { TableSearch } from "~/components/Table/TableSearch";
import { UniversalTable } from "~/components/Table/UniversalTable";
import { useUniversalTable } from "~/components/Table/useUniversalTable";
import WarningIcon from "~/svg/WarningIcon";
import { MaterializeTheme } from "~/theme";
import { truncateMaxWidth } from "~/theme/components/Table";
import {
  formatDate,
  FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
} from "~/utils/dateFormat";

import {
  ClusterActionsCell,
  ClusterNameCell,
  ClusterTableMeta,
} from "./clusterTableCells";

/**
 * One row in the clusters table: a cluster, or one of its replicas nested
 * underneath it. A union because TanStack's `getSubRows` requires child rows
 * to share the parent's row type.
 */
type ClusterRow =
  | ({ rowType: "cluster" } & ClusterWithOwnership)
  | ({ rowType: "replica" } & Replica);

const CLUSTER_ROW_CLASS = "cluster-row";

const ReplicaNameCell = ({ replica }: { replica: Replica }) => replica.name;

const ReplicaCpuCell = ({ cpuPercent }: { cpuPercent: number | null }) => {
  // NOTE: an idle replica reports 0, which must render as "0.0%" rather than
  // being treated as "no reading". Only a missing sample is a dash.
  if (cpuPercent === null) {
    return <>-</>;
  }
  return <PercentBar value={cpuPercent} />;
};

/** Formats a status-change timestamp for display, or "-" when there is none. */
const formatStatusChange = (timestamp: string | null | undefined) =>
  timestamp ? formatDate(timestamp, FRIENDLY_DATETIME_FORMAT_NO_SECONDS) : "-";

const ReplicaLastStatusChangeCell = ({
  updatedAt,
  offlineStatus,
}: {
  updatedAt: string | null;
  offlineStatus: LatestOfflineReplicaInfo | undefined;
}) => (
  <HStack>
    <Text as="span" noOfLines={1}>
      {formatStatusChange(updatedAt)}
    </Text>
    {offlineStatus?.shouldSurfaceOom && (
      <Tooltip
        px={3}
        py={2}
        minWidth="fit-content"
        rounded="md"
        label={`This replica ran out of memory on ${formatDate(
          offlineStatus.lastOfflineAt,
          FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
        )}`}
      >
        <WarningIcon
          // The tooltip text is only reachable on hover, so the icon needs a
          // name of its own to mean anything to a screen reader.
          role="img"
          aria-label="Ran out of memory"
        />
      </Tooltip>
    )}
  </HStack>
);

/**
 * The CPU figure a cluster row sorts by: its busiest replica, or null when none
 * of them reports a sample.
 *
 * A cluster has no utilization of its own. Sorting it on a constant would leave
 * every cluster tied, and TanStack breaks ties by row index, so the column would
 * only ever reorder replicas within a cluster and never move the clusters.
 */
const maxReplicaCpuPercent = (replicas: Replica[]) =>
  replicas.reduce<number | null>(
    (max, { cpuPercent }) =>
      cpuPercent !== null && (max === null || cpuPercent > max)
        ? cpuPercent
        : max,
    null,
  );

const columnHelper = createColumnHelper<ClusterRow>();

const columns = [
  columnHelper.accessor("name", {
    header: "Name",
    sortingFn: "alphanumeric",
    cell: (info) => {
      const row = info.row.original;
      return row.rowType === "cluster" ? (
        <ClusterNameCell cluster={row} />
      ) : (
        <ReplicaNameCell replica={row} />
      );
    },
    meta: {
      minWidth: { md: "280px", sm: "auto" },
      cellProps: truncateMaxWidth,
    },
  }),
  columnHelper.accessor(
    (row) => (row.rowType === "replica" ? row.size : null),
    {
      id: "sizes",
      header: "Size",
      sortingFn: sortingFunctions.nullsLast,
      // A cluster row is a heading, so it stays blank. Only a replica that
      // genuinely reports no size gets a dash.
      cell: (info) =>
        info.row.original.rowType === "cluster"
          ? null
          : (info.getValue() ?? "-"),
    },
  ),
  columnHelper.accessor(
    // Sorting recurses into sub-rows with this same value, so a cluster
    // ordering by its busiest replica and its replicas ordering among
    // themselves both fall out of one accessor.
    (row) =>
      row.rowType === "cluster"
        ? maxReplicaCpuPercent(row.replicas)
        : row.cpuPercent,
    {
      id: "cpuPercent",
      header: "CPU",
      sortingFn: sortingFunctions.numericNullsLast,
      cell: (info) => {
        const row = info.row.original;
        // Utilization is per replica, so a cluster row has nothing to show
        // here. Deliberately blank rather than a dash.
        if (row.rowType === "cluster") {
          return null;
        }
        return <ReplicaCpuCell cpuPercent={info.getValue()} />;
      },
    },
  ),
  columnHelper.accessor(
    (row) => {
      if (row.rowType === "cluster") {
        return row.latestStatusUpdate;
      }
      // A replica carries one status row per process and the query leaves them
      // unordered, so pick the newest by timestamp.
      return row.statuses.reduce<string | null>(
        (latest, { updated_at }) =>
          latest === null || Date.parse(updated_at) > Date.parse(latest)
            ? updated_at
            : latest,
        null,
      );
    },
    {
      id: "lastStatusChange",
      header: "Last status change",
      sortingFn: sortingFunctions.nullsLast,
      cell: (info) => {
        const row = info.row.original;
        if (row.rowType === "cluster") {
          return null;
        }
        const meta = info.table.options.meta as ClusterTableMeta;
        return (
          <ReplicaLastStatusChangeCell
            updatedAt={info.getValue()}
            offlineStatus={meta.offlineReplicaMap?.get(row.id)}
          />
        );
      },
    },
  ),
  columnHelper.display({
    id: "actions",
    header: "",
    cell: (info) => {
      const row = info.row.original;
      return row.rowType === "cluster" ? (
        <ClusterActionsCell cluster={row} />
      ) : (
        ""
      );
    },
    enableSorting: false,
    size: OVERFLOW_BUTTON_WIDTH,
  }),
];

export interface ClusterUsageTableProps {
  clusters: ClusterWithOwnership[];
}

/**
 * Clusters with their replicas nested underneath, each replica carrying its own
 * size, CPU utilization, and status.
 */
export const ClusterUsageTable = ({ clusters }: ClusterUsageTableProps) => {
  const { data: offlineReplicaMap, error: offlineReplicaError } =
    useLatestOfflineReplica();
  const { colors, space } = useTheme<MaterializeTheme>();

  const meta: ClusterTableMeta = { offlineReplicaMap };

  // TanStack recomputes its row models whenever `data` changes identity, so the
  // tagged copies have to outlive the render that built them.
  const rows = React.useMemo<ClusterRow[]>(
    () => clusters.map((cluster) => ({ rowType: "cluster", ...cluster })),
    [clusters],
  );

  const table = useUniversalTable({
    data: rows,
    columns,
    initialSorting: [{ id: "name", desc: false }],
    pageSize: 20,
    // Replicas carry the data in this table, so show them without requiring a
    // click. Cluster rows act as headings.
    initialExpanded: true,
    getSubRows: (row) =>
      row.rowType === "cluster" && row.replicas.length > 0
        ? row.replicas.map((r) => ({ rowType: "replica" as const, ...r }))
        : undefined,
    state: {
      columnVisibility: {
        lastStatusChange: !offlineReplicaError,
      },
    },
    meta,
  });

  return (
    <VStack spacing={4} align="stretch">
      <TableSearch
        onValueChange={table.setGlobalFilter}
        placeholder="Search clusters..."
      />
      <UniversalTable
        table={table}
        variant="borderless"
        data-testid="cluster-table"
        expandLabel={(row) => `Show replicas of ${row.original.name}`}
        // Keyed off the class rather than `[data-parent-row]` so a cluster with
        // no replicas — which has nothing to expand, and so is not a group row
        // — still reads as a heading.
        rowSx={{
          [`&.${CLUSTER_ROW_CLASS} td`]: {
            borderTopWidth: "1px",
            borderTopStyle: "solid",
            borderTopColor: colors.border.secondary,
            paddingTop: space[3],
          },
        }}
        getRowClassName={(row) =>
          row.original.rowType === "cluster" ? CLUSTER_ROW_CLASS : undefined
        }
      />
      <TablePagination table={table} itemLabel="clusters" />
    </VStack>
  );
};
