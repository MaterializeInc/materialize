// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { HStack, Text, Tooltip, useTheme, VStack } from "@chakra-ui/react";
import { createColumnHelper, FilterFn, filterFns } from "@tanstack/react-table";
import React from "react";

import {
  ClusterWithOwnership,
  Replica,
} from "~/api/materialize/cluster/clusterList";
import { ReplicaUtilization } from "~/api/materialize/cluster/replicaUtilization";
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
import { useReplicaUtilization } from "./queries";

/**
 * The utilization readings a replica row displays, as fractions of the
 * replica's allocation. Null for a replica with no sample in the window.
 */
type ReplicaUtilizationValues = Omit<ReplicaUtilization, "replicaId">;

const NO_UTILIZATION: ReplicaUtilizationValues = {
  cpuPercent: null,
  memoryPercent: null,
  diskPercent: null,
  heapPercent: null,
};

/**
 * Utilization arrives from a separate query than the replica itself, so it is
 * merged onto the replica before the table sees it. Sorting a cluster row by
 * its busiest replica happens in a column accessor, which TanStack gives the
 * row and nothing else, so the readings have to live on the row data rather
 * than be looked up from table meta.
 */
type ReplicaWithUtilization = Replica & ReplicaUtilizationValues;

type ClusterRowData = Omit<ClusterWithOwnership, "replicas"> & {
  replicas: ReplicaWithUtilization[];
};

/**
 * One row in the clusters table: a cluster, or one of its replicas nested
 * underneath it. A union because TanStack's `getSubRows` requires child rows
 * to share the parent's row type.
 */
type ClusterRow =
  | ({ rowType: "cluster" } & ClusterRowData)
  // `clusterName` mirrors the parent's name for the global filter's benefit.
  | ({ rowType: "replica"; clusterName: string } & ReplicaWithUtilization);

const CLUSTER_ROW_CLASS = "cluster-row";

const ReplicaNameCell = ({ replica }: { replica: Replica }) => replica.name;

const ReplicaPercentCell = ({ value }: { value: number | null }) => {
  // NOTE: an idle replica reports 0, which must render as "0.0%" rather than
  // being treated as "no reading". Only a missing sample is a dash.
  if (value === null) {
    return <>-</>;
  }
  return <PercentBar fraction={value} />;
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
 * The value a cluster row sorts by in a per-replica column: the greatest of its
 * replicas' values under `compare`, or null when none of them has one.
 *
 * A cluster has no reading of its own in these columns. Sorting it on a constant
 * would leave every cluster tied, and TanStack breaks ties by row index, so the
 * column would only ever reorder replicas within a cluster and never move the
 * clusters themselves.
 *
 * NOTE: `compare` must order values the same way the column's `sortingFn` does.
 * If the two disagree, a cluster can sort above another whose replicas all rank
 * higher, because the two levels are being ranked by different rules.
 */
const maxReplicaValue = <R extends Replica, T>(
  replicas: R[],
  read: (replica: R) => T | null,
  compare: (a: T, b: T) => number,
): T | null =>
  replicas.reduce<T | null>((max, replica) => {
    const value = read(replica);
    if (value === null) return max;
    return max === null || compare(value, max) > 0 ? value : max;
  }, null);

/** Orders sizes the way `nullsLast` does, so "50cc" ranks below "100cc". */
const compareSizes = (a: string, b: string) =>
  a.localeCompare(b, undefined, { numeric: true });

const compareTimestamps = (a: string, b: string) =>
  Date.parse(a) - Date.parse(b);

/**
 * The newest of a replica's status timestamps, or null when it has none. A
 * replica carries one status row per process and the query leaves them
 * unordered, so position must not decide which one wins.
 */
const latestReplicaStatusAt = (replica: Replica) =>
  replica.statuses.reduce<string | null>(
    (latest, { updated_at }) =>
      latest === null || Date.parse(updated_at) > Date.parse(latest)
        ? updated_at
        : latest,
    null,
  );

/**
 * Global filter that also keeps a replica whose own cluster matches, so a hit on
 * a cluster row displays all that cluster's replicas.
 */
const globalFilterFn: FilterFn<ClusterRow> = (
  row,
  columnId,
  filterValue,
  addMeta,
) => {
  if (filterFns.includesString(row, columnId, filterValue, addMeta)) {
    return true;
  }
  return (
    row.original.rowType === "replica" &&
    row.original.clusterName
      .toLowerCase()
      .includes(String(filterValue).toLowerCase())
  );
};

const columnHelper = createColumnHelper<ClusterRow>();

/**
 * A utilization column, read from a replica by `read`.
 *
 * They all share one shape because the reading is per replica in every case: a
 * cluster row ranks by its busiest replica and renders blank, and a replica
 * renders a bar, or a dash when it has no sample.
 */
const percentColumn = (
  id: string,
  header: string,
  read: (replica: ReplicaWithUtilization) => number | null,
) =>
  columnHelper.accessor(
    // Sorting recurses into sub-rows with this same value, so a cluster
    // ordering by its busiest replica and its replicas ordering among
    // themselves both fall out of one accessor.
    (row) =>
      row.rowType === "cluster"
        ? maxReplicaValue(row.replicas, read, (a, b) => a - b)
        : read(row),
    {
      id,
      header,
      sortingFn: sortingFunctions.numericNullsLast,
      sortDescFirst: true,
      cell: (info) => {
        const row = info.row.original;
        // Utilization is per replica, so a cluster row has nothing to show
        // here. Deliberately blank rather than a dash.
        if (row.rowType === "cluster") {
          return null;
        }
        return <ReplicaPercentCell value={info.getValue()} />;
      },
    },
  );

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
    (row) =>
      row.rowType === "cluster"
        ? maxReplicaValue(row.replicas, (r) => r.size, compareSizes)
        : row.size,
    {
      id: "sizes",
      header: "Size",
      sortingFn: sortingFunctions.nullsLast,
      sortDescFirst: true,
      // A cluster row is a heading, so it stays blank. Only a replica that
      // genuinely reports no size gets a dash.
      cell: (info) =>
        info.row.original.rowType === "cluster"
          ? null
          : (info.getValue() ?? "-"),
    },
  ),
  percentColumn("cpuPercent", "CPU", (replica) => replica.cpuPercent),
  // NOTE: this is `memory_percent`, RAM against the size's RAM allocation.
  percentColumn("memoryPercent", "Memory", (replica) => replica.memoryPercent),
  // NOTE: the denominator is the size's configured disk allocation, so this is
  // null, and renders a dash, for any replica on a size that allocates no disk.
  percentColumn("diskPercent", "Disk", (replica) => replica.diskPercent),
  // NOTE: `heap_percent` is RAM plus swap over the heap limit. The heap limit
  // comes from the orchestrator, not the size catalog, so this is null on any
  // environment that does not report one.
  percentColumn("heapPercent", "Heap", (replica) => replica.heapPercent),
  columnHelper.accessor(
    // NOTE: deliberately not the cluster's own `latestStatusUpdate`. That comes
    // from the replica status *history*, so it counts replicas that have since
    // been dropped and can name a time no visible replica row reports. Rolling
    // the replicas up keeps a cluster ranked by the rows shown beneath it.
    (row) =>
      row.rowType === "cluster"
        ? maxReplicaValue(
            row.replicas,
            latestReplicaStatusAt,
            compareTimestamps,
          )
        : latestReplicaStatusAt(row),
    {
      id: "lastStatusChange",
      header: "Last status change",
      sortingFn: sortingFunctions.nullsLast,
      sortDescFirst: false,
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
 * size, utilization, and status.
 */
export const ClusterUsageTable = ({ clusters }: ClusterUsageTableProps) => {
  const { data: offlineReplicaMap, error: offlineReplicaError } =
    useLatestOfflineReplica();
  const { data: replicaUtilization } = useReplicaUtilization();
  const { colors, space } = useTheme<MaterializeTheme>();

  const meta: ClusterTableMeta = { offlineReplicaMap };

  // TanStack recomputes its row models whenever `data` changes identity, so the
  // tagged copies have to outlive the render that built them.
  const rows = React.useMemo<ClusterRow[]>(
    () =>
      clusters.map((cluster) => ({
        rowType: "cluster",
        ...cluster,
        replicas: cluster.replicas.map((replica) => {
          const utilization = replicaUtilization?.get(replica.id);
          return {
            ...replica,
            cpuPercent: utilization?.cpuPercent ?? NO_UTILIZATION.cpuPercent,
            memoryPercent:
              utilization?.memoryPercent ?? NO_UTILIZATION.memoryPercent,
            diskPercent: utilization?.diskPercent ?? NO_UTILIZATION.diskPercent,
            heapPercent: utilization?.heapPercent ?? NO_UTILIZATION.heapPercent,
          };
        }),
      })),
    [clusters, replicaUtilization],
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
        ? row.replicas.map((r) => ({
            rowType: "replica" as const,
            clusterName: row.name,
            ...r,
          }))
        : undefined,
    globalFilterFn,
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
