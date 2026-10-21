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

import { OOM_REASON } from "~/api/materialize/cluster/replicaStatusTimeline";
import { TooltipColorSwatch } from "~/components/graphComponents";
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
import { formatDurationExact, formatPercentage } from "~/utils/format";

import { percentOf } from "../ResourceUsage/processReadingsModel";
import {
  formatPeakBytes,
  HYDRATION_HELP,
  HydrationRow,
  HydrationRowKind,
  ROW_KIND_NAMES,
  rowKind,
  rowTime,
} from "./hydrationView";

interface HydrationTableMeta {
  colorById: Map<string, string>;
  nameById: Map<string, string>;
  heapLimitById: Map<string, number>;
  selectedId: string | undefined;
  onSelect: (rowId: string) => void;
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
    onClick={() => meta.onSelect(row.id)}
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
    cell: (info) => ROW_KIND_NAMES[info.getValue()].short,
  }),
  columnHelper.accessor(
    (row) => (row.type === "hydration" ? row.durationMs : null),
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
    (row) => (row.type === "hydration" ? row.objectCount : null),
    {
      id: "objects",
      header: "Objects",
      sortingFn: sortingFunctions.numericNullsLast,
      meta: { isNumeric: true, tooltip: HYDRATION_HELP.objects },
      cell: (info) => info.getValue() ?? "-",
    },
  ),
  columnHelper.display({
    id: "peakHeap",
    header: "Peak heap",
    meta: { isNumeric: true, tooltip: HYDRATION_HELP.peakHeap },
    cell: (info) => {
      const row = info.row.original;
      if (row.type === "unfinished") {
        return row.reason === OOM_REASON ? "Out of memory" : "-";
      }
      const meta = info.table.options.meta as HydrationTableMeta;
      const percent = percentOf(
        row.peakHeapBytes,
        meta.heapLimitById.get(row.replicaId) ?? null,
      );
      const peakBytes = formatPeakBytes(row.peakHeapBytes) ?? "-";
      return percent === null
        ? peakBytes
        : `${formatPercentage(percent / 100, 0)} · ${peakBytes}`;
    },
  }),
  columnHelper.display({
    id: "peakDisk",
    header: "Peak disk",
    meta: { isNumeric: true, tooltip: HYDRATION_HELP.peakDisk },
    cell: (info) => {
      const row = info.row.original;
      return row.type === "hydration"
        ? (formatPeakBytes(row.peakDiskBytes) ?? "-")
        : "-";
    },
  }),
];

export interface HydrationTableProps extends HydrationTableMeta {
  rows: HydrationRow[];
}

/** Every hydration in the window, with filters by kind. */
export const HydrationTable = ({ rows, ...meta }: HydrationTableProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const [shownKinds, setShownKinds] = React.useState<Set<HydrationRowKind>>(
    () => new Set(["full", "additive", "unfinished"]),
  );
  const shownRows = React.useMemo(
    () => rows.filter((row) => shownKinds.has(rowKind(row))),
    [rows, shownKinds],
  );
  const table = useUniversalTable({
    data: shownRows,
    columns: COLUMNS,
    meta,
    getRowId: ({ id }) => id,
  });
  const toggleKind = (kind: HydrationRowKind) =>
    setShownKinds((previous) => {
      const next = new Set(previous);
      if (next.has(kind)) next.delete(kind);
      else next.add(kind);
      return next;
    });
  return (
    <VStack spacing={3} alignItems="stretch">
      <HStack
        as="ul"
        aria-label="Show hydrations"
        spacing={4}
        flexWrap="wrap"
        listStyleType="none"
      >
        {(Object.keys(ROW_KIND_NAMES) as HydrationRowKind[]).map((kind) => (
          <li key={kind}>
            <Text
              as="button"
              type="button"
              textStyle="text-small"
              aria-pressed={shownKinds.has(kind)}
              opacity={shownKinds.has(kind) ? 1 : 0.5}
              onClick={() => toggleKind(kind)}
            >
              {ROW_KIND_NAMES[kind].short}
            </Text>
          </li>
        ))}
      </HStack>
      {shownRows.length === 0 ? (
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          No matching hydrations in this period.
        </Text>
      ) : (
        <>
          <UniversalTable table={table} variant="rounded" />
          <TablePagination table={table} itemLabel="hydrations" />
        </>
      )}
    </VStack>
  );
};
