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
import { formatBytesShort, formatDurationExact } from "~/utils/format";

import { percentOf } from "../ResourceUsage/processReadingsModel";
import { HYDRATION_HELP, HydrationRow, rowTime } from "./hydrationView";

type RowKind = "full" | "newObjects" | "unfinished";

const KIND_LABELS: Record<RowKind, string> = {
  full: "Full hydration",
  newObjects: "New objects",
  unfinished: "Restarted before finishing",
};

const kindOf = (row: HydrationRow): RowKind =>
  row.type === "unfinished" ? "unfinished" : row.kind;

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

const bytes = (value: number | null) =>
  value === null ? "-" : formatBytesShort(BigInt(Math.round(value)));

const COLUMNS = [
  columnHelper.accessor((row) => rowTime(row).getTime(), {
    id: "started",
    header: "Started",
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
  columnHelper.accessor(kindOf, {
    id: "kind",
    header: "Kind",
    meta: {
      tooltip: "Inferred: Materialize doesn't record why a hydration started.",
    },
    cell: (info) => KIND_LABELS[info.getValue()],
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
    id: "peakMemory",
    header: "Peak memory",
    meta: { isNumeric: true, tooltip: HYDRATION_HELP.peakMemory },
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
      return percent === null
        ? bytes(row.peakHeapBytes)
        : `${Math.round(percent)}% · ${bytes(row.peakHeapBytes)}`;
    },
  }),
  columnHelper.display({
    id: "peakDisk",
    header: "Peak disk or swap",
    meta: { isNumeric: true, tooltip: HYDRATION_HELP.peakDisk },
    cell: (info) => {
      const row = info.row.original;
      return row.type === "hydration" ? bytes(row.peakDiskBytes) : "-";
    },
  }),
];

export interface HydrationTableProps extends HydrationTableMeta {
  rows: HydrationRow[];
}

/** Every hydration in the window, with filters by kind. */
export const HydrationTable = ({ rows, ...meta }: HydrationTableProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const [shownKinds, setShownKinds] = React.useState<Set<RowKind>>(
    () => new Set(["full", "newObjects", "unfinished"]),
  );
  const shownRows = React.useMemo(
    () => rows.filter((row) => shownKinds.has(kindOf(row))),
    [rows, shownKinds],
  );
  const table = useUniversalTable({
    data: shownRows,
    columns: COLUMNS,
    meta,
    getRowId: ({ id }) => id,
  });
  const toggleKind = (kind: RowKind) =>
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
        aria-label="Show hydrations of kind"
        spacing={4}
        flexWrap="wrap"
        listStyleType="none"
      >
        {(Object.keys(KIND_LABELS) as RowKind[]).map((kind) => (
          <li key={kind}>
            <Text
              as="button"
              type="button"
              textStyle="text-small"
              aria-pressed={shownKinds.has(kind)}
              opacity={shownKinds.has(kind) ? 1 : 0.5}
              onClick={() => toggleKind(kind)}
            >
              {KIND_LABELS[kind]}
            </Text>
          </li>
        ))}
      </HStack>
      {shownRows.length === 0 ? (
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          No hydrations of these kinds in this period.
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
