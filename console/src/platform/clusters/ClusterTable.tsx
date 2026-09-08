// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { HStack, Text, Tooltip, VStack } from "@chakra-ui/react";
import { createColumnHelper } from "@tanstack/react-table";
import React from "react";

import { ClusterWithOwnership } from "~/api/materialize/cluster/clusterList";
import useLatestOfflineReplica, {
  LatestOfflineReplicaInfo,
  LatestOfflineReplicaMap,
} from "~/api/materialize/cluster/useLatestOfflineReplica";
import { OVERFLOW_BUTTON_WIDTH } from "~/components/OverflowMenu";
import { sortingFunctions } from "~/components/Table/tableColumnBuilders";
import { TablePagination } from "~/components/Table/TablePagination";
import { TableSearch } from "~/components/Table/TableSearch";
import { UniversalTable } from "~/components/Table/UniversalTable";
import { useUniversalTable } from "~/components/Table/useUniversalTable";
import WarningIcon from "~/svg/WarningIcon";
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
 * The cluster's most recent replica outage worth surfacing as an out-of-memory
 * warning, or undefined when it has none.
 *
 * NOTE: the offline map is keyed by replica, not by cluster, so a cluster-level
 * warning has to roll its replicas up. Looking the cluster's own id up in that
 * map finds nothing and silently drops every warning.
 */
const latestReplicaOom = (
  cluster: ClusterWithOwnership,
  offlineReplicaMap: LatestOfflineReplicaMap | undefined,
) =>
  cluster.replicas.reduce<LatestOfflineReplicaInfo | undefined>(
    (latest, replica) => {
      const status = offlineReplicaMap?.get(replica.id);
      if (!status?.shouldSurfaceOom) return latest;
      // Several replicas of one cluster can have been killed. The newest is the
      // one worth naming.
      return latest && latest.lastOfflineAt >= status.lastOfflineAt
        ? latest
        : status;
    },
    undefined,
  );

const LastStatusChangeCell = ({
  cluster,
  offlineReplicaMap,
}: {
  cluster: ClusterWithOwnership;
  offlineReplicaMap: LatestOfflineReplicaMap | undefined;
}) => {
  const offlineStatus = latestReplicaOom(cluster, offlineReplicaMap);

  const lastStatusChangeString = cluster.latestStatusUpdate
    ? formatDate(
        cluster.latestStatusUpdate,
        FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
      )
    : "-";

  return (
    <HStack>
      <Text noOfLines={1} paddingRight="6" position="relative">
        {lastStatusChangeString}
        {offlineStatus && (
          <Tooltip
            px={3}
            py={2}
            minWidth="fit-content"
            rounded="md"
            label={`A replica ran out of memory on ${formatDate(
              offlineStatus.lastOfflineAt,
              FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
            )}`}
          >
            <WarningIcon
              position="absolute"
              right="0"
              // The tooltip text is only reachable on hover, so the icon needs a
              // name of its own to mean anything to a screen reader.
              role="img"
              aria-label="Ran out of memory"
            />
          </Tooltip>
        )}
      </Text>
    </HStack>
  );
};

const columnHelper = createColumnHelper<ClusterWithOwnership>();

const columns = [
  columnHelper.accessor("name", {
    header: "Name",
    sortingFn: "alphanumeric",
    cell: (info) => <ClusterNameCell cluster={info.row.original} />,
    meta: {
      minWidth: { md: "280px", sm: "auto" },
      cellProps: truncateMaxWidth,
    },
  }),
  columnHelper.accessor((row) => row.replicas.length, {
    id: "replicaCount",
    header: "Replicas",
    sortingFn: "basic",
  }),
  columnHelper.accessor(
    (row) => {
      const sizes = new Set(row.replicas.map((r) => r.size));
      return sizes.size > 0 ? Array.from(sizes).join(", ") : null;
    },
    {
      id: "sizes",
      header: "Size",
      sortingFn: sortingFunctions.nullsLast,
      cell: (info) => info.getValue() ?? "-",
    },
  ),
  columnHelper.accessor("latestStatusUpdate", {
    id: "lastStatusChange",
    header: "Last status change",
    sortingFn: sortingFunctions.nullsLast,
    cell: (info) => {
      const meta = info.table.options.meta as ClusterTableMeta;
      return (
        <LastStatusChangeCell
          cluster={info.row.original}
          offlineReplicaMap={meta.offlineReplicaMap}
        />
      );
    },
  }),
  columnHelper.display({
    id: "actions",
    header: "",
    cell: (info) => <ClusterActionsCell cluster={info.row.original} />,
    enableSorting: false,
    size: OVERFLOW_BUTTON_WIDTH,
  }),
];

export interface ClusterTableProps {
  clusters: ClusterWithOwnership[];
}

/**
 * One row per cluster, summarizing its replicas as a count and a list of sizes.
 */
export const ClusterTable = ({ clusters }: ClusterTableProps) => {
  const { data: offlineReplicaMap, error: offlineReplicaError } =
    useLatestOfflineReplica();

  const meta: ClusterTableMeta = { offlineReplicaMap };

  const table = useUniversalTable({
    data: clusters,
    columns,
    initialSorting: [{ id: "name", desc: false }],
    pageSize: 20,
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
        variant="linkable"
        data-testid="cluster-table"
      />
      <TablePagination table={table} itemLabel="clusters" />
    </VStack>
  );
};
