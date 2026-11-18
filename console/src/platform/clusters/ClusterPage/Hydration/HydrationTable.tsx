// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { HStack, Text, useTheme, VStack } from "@chakra-ui/react";
import { createColumnHelper } from "@tanstack/react-table";
import React from "react";

import { SizeAllocation } from "~/api/materialize/cluster/replicaProcessMetrics";
import { OOM_REASON } from "~/api/materialize/cluster/replicaStatusTimeline";
import { TooltipColorSwatch } from "~/components/graphComponents";
import PercentBar from "~/components/PercentBar";
import { sortingFunctions } from "~/components/Table/tableColumnBuilders";
import { TablePagination } from "~/components/Table/TablePagination";
import { UniversalTable } from "~/components/Table/UniversalTable";
import { useUniversalTable } from "~/components/Table/useUniversalTable";
import TextLink from "~/components/TextLink";
import { MaterializeTheme } from "~/theme";
import {
  formatDate,
  FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
} from "~/utils/dateFormat";
import { formatDurationExact } from "~/utils/format";

import { percentOf } from "../ResourceUsage/processReadingsModel";
import { InfoTip } from "./hydrationComponents";
import {
  formatPeakBytes,
  HYDRATION_HELP,
  HydrationRow,
  ROW_KIND_NAMES,
  rowKind,
  rowTime,
} from "./hydrationView";

interface HydrationTableMeta {
  colorById: Map<string, string>;
  nameById: Map<string, string>;
  /** Each replica's per-process CPU, memory and disk. */
  allocationById: Map<string, SizeAllocation>;
  selectedId: string | undefined;
  onSelect: (row: HydrationRow) => void;
}

const columnHelper = createColumnHelper<HydrationRow>();

const SelectCell = ({
  row,
  meta,
}: {
  row: HydrationRow;
  meta: HydrationTableMeta;
}) => (
  <TextLink
    as="button"
    type="button"
    fontWeight={row.id === meta.selectedId ? "600" : undefined}
    aria-pressed={row.id === meta.selectedId}
    onClick={() => meta.onSelect(row)}
  >
    {formatDate(rowTime(row), FRIENDLY_DATETIME_FORMAT_NO_SECONDS)}
  </TextLink>
);

const ReplicaCell = ({
  replicaId,
  meta,
}: {
  replicaId: string;
  meta: HydrationTableMeta;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <HStack spacing={2}>
      <TooltipColorSwatch
        color={meta.colorById.get(replicaId) ?? colors.foreground.tertiary}
      />
      <Text as="span">{meta.nameById.get(replicaId) ?? replicaId}</Text>
    </HStack>
  );
};

const COLUMNS = [
  columnHelper.accessor((row) => rowTime(row).getTime(), {
    id: "started",
    header: "Time",
    meta: {
      tooltip: "When the hydration began, or when the replica restarted.",
    },
    cell: (info) => (
      <SelectCell
        row={info.row.original}
        meta={info.table.options.meta as HydrationTableMeta}
      />
    ),
  }),
  columnHelper.accessor("replicaId", {
    header: "Replica",
    cell: (info) => (
      <ReplicaCell
        replicaId={info.getValue()}
        meta={info.table.options.meta as HydrationTableMeta}
      />
    ),
  }),
  columnHelper.accessor(rowKind, {
    id: "kind",
    header: "Hydration",
    meta: { tooltip: HYDRATION_HELP.kind },
    cell: (info) =>
      info.row.original.type === "unrecorded" ? (
        <HStack spacing={1}>
          <Text as="span">
            {ROW_KIND_NAMES[info.getValue()].short} · Being recorded
          </Text>
          <InfoTip subject="being recorded" label={HYDRATION_HELP.recording} />
        </HStack>
      ) : (
        ROW_KIND_NAMES[info.getValue()].short
      ),
  }),
  columnHelper.accessor(
    (row) => (row.type === "unfinished" ? null : row.durationMs),
    {
      id: "duration",
      header: "Duration",
      sortingFn: sortingFunctions.numericNullsLast,
      meta: { isNumeric: true, tooltip: HYDRATION_HELP.duration },
      cell: (info) => {
        const value = info.getValue();
        return value === null ? "-" : formatDurationExact(value);
      },
    },
  ),
  columnHelper.accessor(
    (row) => (row.type === "unfinished" ? null : row.objectCount),
    {
      id: "objects",
      header: "Objects",
      sortingFn: sortingFunctions.numericNullsLast,
      meta: { isNumeric: true, tooltip: HYDRATION_HELP.objects },
      cell: (info) => info.getValue() ?? "-",
    },
  ),
  columnHelper.display({
    id: "peakMemory",
    header: "Peak memory",
    meta: { tooltip: HYDRATION_HELP.peakMemory },
    cell: (info) => {
      const row = info.row.original;
      if (row.type === "unfinished") {
        return row.reason === OOM_REASON ? "Out of memory" : "-";
      }
      const meta = info.table.options.meta as HydrationTableMeta;
      const percent = percentOf(
        row.peakMemoryBytes,
        meta.allocationById.get(row.replicaId)?.memoryBytes ?? null,
      );
      return percent === null ? (
        (formatPeakBytes(row.peakMemoryBytes) ?? "-")
      ) : (
        <PercentBar fraction={percent / 100} />
      );
    },
  }),
  columnHelper.display({
    id: "peakDisk",
    header: "Peak disk",
    meta: { isNumeric: true, tooltip: HYDRATION_HELP.peakDisk },
    cell: (info) => {
      const row = info.row.original;
      return row.type === "unfinished"
        ? "-"
        : (formatPeakBytes(row.peakDiskBytes) ?? "-");
    },
  }),
];

export interface HydrationTableProps extends HydrationTableMeta {
  rows: HydrationRow[];
}

/** Every hydration in the window. */
export const HydrationTable = ({ rows, ...meta }: HydrationTableProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const table = useUniversalTable({
    data: rows,
    columns: COLUMNS,
    meta,
    getRowId: ({ id }) => id,
  });
  return rows.length === 0 ? (
    <Text textStyle="text-small" color={colors.foreground.secondary}>
      No hydrations in this period.
    </Text>
  ) : (
    <VStack spacing={3} alignItems="stretch">
      <UniversalTable table={table} variant="rounded" />
      <TablePagination table={table} itemLabel="hydrations" />
    </VStack>
  );
};
