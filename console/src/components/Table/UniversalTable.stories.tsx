// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, VStack } from "@chakra-ui/react";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { createColumnHelper } from "@tanstack/react-table";
import React from "react";

import { sortingFunctions } from "./tableColumnBuilders";
import { TablePagination } from "./TablePagination";
import { TableSearch } from "./TableSearch";
import { UniversalTableProps } from "./tableTypes";
import { UniversalTable } from "./UniversalTable";
import { useUniversalTable } from "./useUniversalTable";

interface Cluster {
  id: string;
  name: string;
  replicas: number;
  size: string | null;
  creditsPerHour: number;
}

const CLUSTERS: Cluster[] = [
  {
    id: "u1",
    name: "analytics",
    replicas: 2,
    size: "25cc",
    creditsPerHour: 0.5,
  },
  {
    id: "u2",
    name: "quickstart",
    replicas: 1,
    size: "100cc",
    creditsPerHour: 2,
  },
  {
    id: "u3",
    name: "prod_serving",
    replicas: 3,
    size: "400cc",
    creditsPerHour: 8,
  },
  { id: "u4", name: "staging", replicas: 1, size: "50cc", creditsPerHour: 1 },
  {
    id: "u5",
    name: "batch_etl",
    replicas: 2,
    size: "200cc",
    creditsPerHour: 4,
  },
  { id: "u6", name: "adhoc", replicas: 0, size: null, creditsPerHour: 0 },
  {
    id: "u7",
    name: "ingest_kafka",
    replicas: 1,
    size: "100cc",
    creditsPerHour: 2,
  },
  {
    id: "u8",
    name: "ingest_postgres",
    replicas: 1,
    size: "50cc",
    creditsPerHour: 1,
  },
];

const columnHelper = createColumnHelper<Cluster>();

const COLUMNS = [
  columnHelper.accessor("name", {
    header: "Name",
    sortingFn: "alphanumeric",
  }),
  columnHelper.accessor("size", {
    header: "Size",
    sortingFn: sortingFunctions.nullsLast,
    cell: (info) => info.getValue() ?? "-",
    meta: { tooltip: "Cluster size configuration" },
  }),
  columnHelper.accessor("replicas", {
    header: "Replicas",
    sortingFn: "basic",
    meta: { isNumeric: true },
  }),
  columnHelper.accessor("creditsPerHour", {
    header: "Credits/hr",
    sortingFn: "basic",
    cell: (info) => info.getValue().toFixed(2),
    meta: { isNumeric: true },
  }),
];

type ClusterTableProps = Omit<UniversalTableProps<Cluster>, "table"> & {
  data: Cluster[];
};

/**
 * Composes the hook and the component the way a real screen does. `data` is a
 * story-only prop; everything else is forwarded to `UniversalTable` untouched.
 */
const ClusterTable = ({ data, ...tableProps }: ClusterTableProps) => {
  const table = useUniversalTable({
    data,
    columns: COLUMNS,
    initialSorting: [{ id: "name", desc: false }],
  });

  return <UniversalTable {...tableProps} table={table} />;
};

const meta = {
  title: "Components/UniversalTable",
  component: ClusterTable,
  args: {
    data: CLUSTERS,
    variant: "linkable",
    isLoading: false,
  },
  argTypes: {
    variant: {
      control: { type: "select" },
      options: ["linkable", "standalone", "rounded", "shell", "borderless"],
    },
    skeletonRowCount: { control: { type: "range", min: 1, max: 12, step: 1 } },
    data: { control: false },
  },
} satisfies Meta<typeof ClusterTable>;

export default meta;

type Story = StoryObj<typeof meta>;

/** Sortable headers, a tooltip on Size, right-aligned numeric columns. */
export const Default: Story = {};

/**
 * Skeleton rows stand in while the query is in flight, so the table keeps its
 * height instead of the page reflowing when rows land.
 */
export const Loading: Story = {
  args: { isLoading: true },
};

/** Header with nothing beneath it. The caller supplies any empty-state copy. */
export const NoRows: Story = {
  args: { data: [] },
};

/** Rows respond to clicks, which is what the `linkable` variant is styled for. */
export const ClickableRows: Story = {
  args: {
    onRowClick: (cluster) => window.alert(`Navigate to ${cluster.name}`),
  },
};

/** The `standalone` variant, for a table that is not embedded in a list page. */
export const StandaloneVariant: Story = {
  args: { variant: "standalone" },
};

const FooterTable = ({
  data,
  variant,
}: Pick<ClusterTableProps, "data" | "variant">) => {
  const columnsWithFooter = [
    columnHelper.accessor("name", {
      header: "Name",
      footer: "Total",
      sortingFn: "alphanumeric",
    }),
    columnHelper.accessor("size", {
      header: "Size",
      cell: (info) => info.getValue() ?? "-",
    }),
    columnHelper.accessor("replicas", {
      header: "Replicas",
      footer: () => CLUSTERS.reduce((sum, c) => sum + c.replicas, 0),
      meta: { isNumeric: true },
    }),
    columnHelper.accessor("creditsPerHour", {
      header: "Credits/hr",
      cell: (info) => info.getValue().toFixed(2),
      footer: () =>
        CLUSTERS.reduce((sum, c) => sum + c.creditsPerHour, 0).toFixed(2),
      meta: { isNumeric: true },
    }),
  ];
  const table = useUniversalTable({ data, columns: columnsWithFooter });

  return <UniversalTable table={table} variant={variant} />;
};

/**
 * A footer renders only when at least one column defines one, and it is
 * suppressed while loading.
 */
export const WithFooter: Story = {
  render: (args) => <FooterTable data={args.data} variant={args.variant} />,
};

interface LedgerRow {
  id: string;
  name: string;
  detail: string;
  creditsPerHour: number;
  replicas?: LedgerRow[];
}

const LEDGER: LedgerRow[] = [
  {
    id: "u3",
    name: "prod_serving",
    detail: "3 replicas",
    creditsPerHour: 8,
    replicas: [
      { id: "u3-r1", name: "r1", detail: "400cc", creditsPerHour: 4 },
      { id: "u3-r2", name: "r2", detail: "200cc", creditsPerHour: 2 },
      { id: "u3-r3", name: "r3", detail: "200cc", creditsPerHour: 2 },
    ],
  },
  {
    id: "u5",
    name: "batch_etl",
    detail: "2 replicas",
    creditsPerHour: 4,
    replicas: [
      { id: "u5-r1", name: "r1", detail: "100cc", creditsPerHour: 2 },
      { id: "u5-r2", name: "r2", detail: "100cc", creditsPerHour: 2 },
    ],
  },
  { id: "u6", name: "adhoc", detail: "no replicas", creditsPerHour: 0 },
];

const ledgerColumnHelper = createColumnHelper<LedgerRow>();

const ReplicaLedgerTable = () => {
  const table = useUniversalTable({
    data: LEDGER,
    columns: [
      ledgerColumnHelper.accessor("name", { header: "Cluster" }),
      ledgerColumnHelper.accessor("detail", { header: "Detail" }),
      ledgerColumnHelper.accessor("creditsPerHour", {
        header: "Credits/hr",
        cell: (info) => info.getValue().toFixed(2),
        meta: { isNumeric: true },
      }),
    ],
    getSubRows: (row) => row.replicas,
    initialExpanded: true,
  });

  return (
    <UniversalTable
      table={table}
      variant="standalone"
      expandLabel={(row) => `Show replicas of ${row.original.name}`}
    />
  );
};

/**
 * `getSubRows` turns on the leading caret column and the ledger styling:
 * group headings carry a top border, children sit borderless beneath them.
 */
export const GroupedRows: Story = {
  render: () => <ReplicaLedgerTable />,
};

const SearchableClusterTable = ({ data }: Pick<ClusterTableProps, "data">) => {
  const table = useUniversalTable({ data, columns: COLUMNS, pageSize: 3 });

  return (
    <VStack alignItems="stretch" spacing="4">
      <Box maxWidth="80">
        <TableSearch
          onValueChange={(value) => table.setGlobalFilter(value)}
          placeholder="Search clusters"
        />
      </Box>
      <UniversalTable table={table} />
      <TablePagination table={table} itemLabel="clusters" />
    </VStack>
  );
};

/**
 * The search box and pager are separate components a screen composes around
 * the table. Both drive the same TanStack instance, and a search resets the
 * page so a filter cannot strand the reader past the last result.
 */
export const WithSearchAndPagination: Story = {
  render: (args) => <SearchableClusterTable data={args.data} />,
};
