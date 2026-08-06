// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { createColumnHelper, Row } from "@tanstack/react-table";
import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";

import { renderComponent } from "~/test/utils";

import { sortingFunctions } from "./tableColumnBuilders";
import { TablePagination } from "./TablePagination";
import { TableSearch } from "./TableSearch";
import { UniversalTable } from "./UniversalTable";
import { useUniversalTable } from "./useUniversalTable";

interface TestCluster {
  id: string;
  name: string;
  replicas: number;
  size: string | null;
}

const testData: TestCluster[] = [
  { id: "1", name: "analytics", replicas: 2, size: "25cc" },
  { id: "2", name: "default", replicas: 1, size: "100cc" },
  { id: "3", name: "prod", replicas: 3, size: null },
  { id: "4", name: "staging", replicas: 1, size: "50cc" },
  { id: "5", name: "batch", replicas: 2, size: "200cc" },
];

const columnHelper = createColumnHelper<TestCluster>();

const columns = [
  columnHelper.accessor("name", {
    header: "Name",
    sortingFn: "alphanumeric",
  }),
  columnHelper.accessor("replicas", {
    header: "Replicas",
    sortingFn: "basic",
  }),
  columnHelper.accessor("size", {
    header: "Size",
    sortingFn: sortingFunctions.nullsLast,
    cell: (info) => info.getValue() ?? "-",
    meta: {
      tooltip: "Cluster size configuration",
    },
  }),
];

const BasicTable = ({ data = testData }: { data?: TestCluster[] }) => {
  const table = useUniversalTable({ data, columns });
  return <UniversalTable table={table} data-testid="test-table" />;
};

const SortableTable = ({
  data = testData,
  initialSorting,
}: {
  data?: TestCluster[];
  initialSorting?: { id: string; desc: boolean }[];
}) => {
  const table = useUniversalTable({ data, columns, initialSorting });
  return <UniversalTable table={table} data-testid="test-table" />;
};

const SearchableTable = ({
  data = testData,
  pageSize,
}: {
  data?: TestCluster[];
  pageSize?: number;
}) => {
  const table = useUniversalTable({ data, columns, pageSize });
  return (
    <div>
      <TableSearch
        onValueChange={table.setGlobalFilter}
        placeholder="Search clusters..."
      />
      <UniversalTable table={table} data-testid="test-table" />
      <TablePagination table={table} itemLabel="clusters" />
    </div>
  );
};

const PaginatedTable = ({
  data = testData,
  pageSize = 2,
}: {
  data?: TestCluster[];
  pageSize?: number;
}) => {
  const table = useUniversalTable({ data, columns, pageSize });
  return (
    <div>
      <UniversalTable table={table} data-testid="test-table" />
      <TablePagination table={table} itemLabel="clusters" />
    </div>
  );
};

const footerColumns = [
  columnHelper.accessor("name", { header: "Name", footer: "Total" }),
  columnHelper.accessor("replicas", {
    header: "Replicas",
    footer: ({ table }) =>
      table
        .getRowModel()
        .rows.reduce((sum, row) => sum + row.original.replicas, 0),
  }),
  columnHelper.accessor("size", { header: "Size" }),
];

const FooterTable = () => {
  const table = useUniversalTable({ data: testData, columns: footerColumns });
  return (
    <UniversalTable
      table={table}
      data-testid="test-table"
      footerTestId="total-row"
    />
  );
};

const ClickableTable = ({
  data = testData,
  onClick,
}: {
  data?: TestCluster[];
  onClick: (row: TestCluster) => void;
}) => {
  const table = useUniversalTable({ data, columns });
  return (
    <UniversalTable
      table={table}
      data-testid="test-table"
      onRowClick={onClick}
    />
  );
};

const LoadingTable = () => {
  const table = useUniversalTable({ data: [], columns });
  return (
    <UniversalTable
      table={table}
      data-testid="test-table"
      isLoading
      skeletonRowCount={3}
    />
  );
};

interface TestAccount {
  name: string;
  cost: string;
  clusters?: TestAccount[];
}

const groupedData: TestAccount[] = [
  {
    name: "account-a",
    cost: "$100",
    clusters: [
      { name: "cluster-a1", cost: "$60" },
      { name: "cluster-a2", cost: "$40" },
    ],
  },
  {
    name: "account-b",
    cost: "$50",
    clusters: [{ name: "cluster-b1", cost: "$50" }],
  },
  {
    name: "account-c",
    cost: "$25",
    clusters: [{ name: "cluster-c1", cost: "$25" }],
  },
];

const groupColumnHelper = createColumnHelper<TestAccount>();

const groupColumns = [
  groupColumnHelper.accessor("name", { header: "Name" }),
  groupColumnHelper.accessor("cost", { header: "Cost" }),
];

const GroupedTable = ({
  initialExpanded,
  onRowClick,
  pageSize,
  rowTestId,
}: {
  initialExpanded?: true;
  onRowClick?: (row: TestAccount) => void;
  pageSize?: number;
  rowTestId?: (row: Row<TestAccount>) => string | undefined;
}) => {
  const table = useUniversalTable({
    data: groupedData,
    columns: groupColumns,
    getSubRows: (row) => row.clusters,
    initialExpanded,
    pageSize,
  });
  return (
    <div>
      <TableSearch
        onValueChange={table.setGlobalFilter}
        placeholder="Search accounts..."
      />
      <UniversalTable
        table={table}
        data-testid="test-table"
        onRowClick={onRowClick}
        rowTestId={rowTestId}
      />
      <TablePagination table={table} itemLabel="accounts" />
    </div>
  );
};

describe("UniversalTable", () => {
  describe("Basic Rendering", () => {
    it("renders column headers", async () => {
      await renderComponent(<BasicTable />);

      expect(screen.getByText("Name")).toBeInTheDocument();
      expect(screen.getByText("Replicas")).toBeInTheDocument();
      expect(screen.getByText("Size")).toBeInTheDocument();
    });

    it("renders data rows", async () => {
      await renderComponent(<BasicTable />);

      expect(screen.getByText("analytics")).toBeInTheDocument();
      expect(screen.getByText("default")).toBeInTheDocument();
      expect(screen.getByText("prod")).toBeInTheDocument();
    });
  });

  describe("Sorting", () => {
    it("sorts ascending on first header click", async () => {
      const user = userEvent.setup();
      await renderComponent(<SortableTable />);

      await user.click(screen.getByText("Name"));

      const rows = screen.getAllByRole("row");
      expect(rows[1]).toHaveTextContent("analytics");
      expect(rows[2]).toHaveTextContent("batch");
      expect(rows[3]).toHaveTextContent("default");
    });

    it("sorts descending on second header click", async () => {
      const user = userEvent.setup();
      await renderComponent(<SortableTable />);

      await user.click(screen.getByText("Name"));
      await user.click(screen.getByText("Name"));

      const rows = screen.getAllByRole("row");
      expect(rows[1]).toHaveTextContent("staging");
      expect(rows[2]).toHaveTextContent("prod");
    });

    it("applies initial sorting on mount", async () => {
      await renderComponent(
        <SortableTable initialSorting={[{ id: "replicas", desc: false }]} />,
      );

      const rows = screen.getAllByRole("row");
      expect(rows[1]).toHaveTextContent("default");
      expect(rows[rows.length - 1]).toHaveTextContent("prod");
    });
  });

  describe("Global Filtering", () => {
    it("filters rows by search text", async () => {
      const user = userEvent.setup();
      await renderComponent(<SearchableTable />);

      await user.type(
        screen.getByPlaceholderText("Search clusters..."),
        "analytics",
      );

      await waitFor(() => {
        expect(screen.getByText("analytics")).toBeInTheDocument();
        expect(screen.queryByText("default")).not.toBeInTheDocument();
        expect(screen.queryByText("prod")).not.toBeInTheDocument();
      });
    });

    it("clears search via the clear button", async () => {
      const user = userEvent.setup();
      await renderComponent(<SearchableTable />);

      await user.type(
        screen.getByPlaceholderText("Search clusters..."),
        "analytics",
      );

      await waitFor(() => {
        expect(screen.queryByText("default")).not.toBeInTheDocument();
      });

      await user.click(screen.getByLabelText("Clear search"));

      await waitFor(() => {
        expect(screen.getByText("analytics")).toBeInTheDocument();
        expect(screen.getByText("default")).toBeInTheDocument();
      });
    });
  });

  describe("Pagination", () => {
    it("paginates results and renders count text", async () => {
      await renderComponent(<PaginatedTable pageSize={2} />);

      const rows = screen.getAllByRole("row");
      expect(rows).toHaveLength(3); // 1 header + 2 data
      expect(screen.getByText(/Showing 1-2 of 5 clusters/)).toBeInTheDocument();
      expect(screen.getByText("page 1 of 3")).toBeInTheDocument();
    });

    it("navigates between pages with next/previous buttons", async () => {
      const user = userEvent.setup();
      await renderComponent(<PaginatedTable pageSize={2} />);

      await user.click(screen.getByLabelText("Next page"));
      expect(screen.getByText("page 2 of 3")).toBeInTheDocument();
      expect(screen.getByText(/Showing 3-4 of 5/)).toBeInTheDocument();

      await user.click(screen.getByLabelText("Previous page"));
      expect(screen.getByText("page 1 of 3")).toBeInTheDocument();
    });

    it("disables previous button on first page", async () => {
      await renderComponent(<PaginatedTable pageSize={2} />);

      expect(screen.getByLabelText("Previous page")).toBeDisabled();
    });

    it("disables next button on last page", async () => {
      const user = userEvent.setup();
      await renderComponent(<PaginatedTable pageSize={2} />);

      await user.click(screen.getByLabelText("Next page"));
      await user.click(screen.getByLabelText("Next page"));

      expect(screen.getByLabelText("Next page")).toBeDisabled();
    });

    it("hides pagination when data fits on one page", async () => {
      await renderComponent(<PaginatedTable pageSize={10} />);

      expect(screen.queryByLabelText("Next page")).not.toBeInTheDocument();
      expect(screen.queryByLabelText("Previous page")).not.toBeInTheDocument();
    });

    it("resets to the first page when the search filter changes", async () => {
      const user = userEvent.setup();
      await renderComponent(<SearchableTable pageSize={2} />);

      await user.click(screen.getByLabelText("Next page"));
      await user.click(screen.getByLabelText("Next page"));
      expect(screen.getByText("page 3 of 3")).toBeInTheDocument();

      // "a" matches analytics, default, staging, batch: 4 rows, 2 pages.
      await user.type(screen.getByPlaceholderText("Search clusters..."), "a");

      await waitFor(() => {
        expect(
          screen.getByText(/Showing 1-2 of 4 clusters/),
        ).toBeInTheDocument();
      });
      expect(screen.getByText("page 1 of 2")).toBeInTheDocument();
    });
  });

  describe("Row Click", () => {
    it("calls onRowClick with the row's data", async () => {
      const onClick = vi.fn();
      const user = userEvent.setup();
      await renderComponent(<ClickableTable onClick={onClick} />);

      await user.click(screen.getByText("analytics"));

      expect(onClick).toHaveBeenCalledWith(
        expect.objectContaining({ id: "1", name: "analytics" }),
      );
    });
  });

  describe("Loading State", () => {
    it("renders skeleton rows when isLoading is true", async () => {
      await renderComponent(<LoadingTable />);

      expect(screen.getAllByRole("row")).toHaveLength(4); // 1 header + 3 skeletons
    });
  });

  describe("Footer", () => {
    it("renders footer cells when columns define footers", async () => {
      await renderComponent(<FooterTable />);

      expect(screen.getByText("Total")).toBeInTheDocument();
      expect(screen.getByText("9")).toBeInTheDocument(); // sum of replicas

      // The footerless "size" column still renders an (empty) footer cell,
      // and footerTestId targets the footer row.
      const tfootRow = screen.getByTestId("total-row");
      expect(within(tfootRow).getAllByRole("cell")).toHaveLength(3);
    });

    it("omits the footer when no column defines one", async () => {
      await renderComponent(<BasicTable />);

      // thead + tbody only, no tfoot
      expect(screen.getAllByRole("rowgroup")).toHaveLength(2);
    });
  });

  describe("Row Test IDs", () => {
    it("applies rowTestId to group and leaf rows", async () => {
      await renderComponent(
        <GroupedTable
          initialExpanded
          rowTestId={(row) =>
            row.getCanExpand() ? "account-row" : "cluster-row"
          }
        />,
      );

      expect(screen.getAllByTestId("account-row")).toHaveLength(3);
      expect(screen.getAllByTestId("cluster-row")).toHaveLength(4);
    });
  });

  describe("Group Rows", () => {
    it("renders groups collapsed by default", async () => {
      await renderComponent(<GroupedTable />);

      expect(screen.getByText("account-a")).toBeInTheDocument();
      expect(screen.queryByText("cluster-a1")).not.toBeInTheDocument();
      expect(screen.getByText("account-a").closest("tr")).toHaveAttribute(
        "aria-expanded",
        "false",
      );
    });

    it("expands all groups with initialExpanded", async () => {
      await renderComponent(<GroupedTable initialExpanded />);

      expect(screen.getByText("cluster-a1")).toBeInTheDocument();
      expect(screen.getByText("cluster-b1")).toBeInTheDocument();
    });

    it("toggles children on group row click", async () => {
      const user = userEvent.setup();
      await renderComponent(<GroupedTable />);

      await user.click(screen.getByText("account-a"));
      expect(screen.getByText("cluster-a1")).toBeInTheDocument();
      expect(screen.getByText("cluster-a2")).toBeInTheDocument();
      expect(screen.getByText("account-a").closest("tr")).toHaveAttribute(
        "aria-expanded",
        "true",
      );

      await user.click(screen.getByText("account-a"));
      expect(screen.queryByText("cluster-a1")).not.toBeInTheDocument();
    });

    it("toggles children with the keyboard", async () => {
      const user = userEvent.setup();
      await renderComponent(<GroupedTable />);

      const groupRow = screen.getByText("account-a").closest("tr");
      if (!groupRow) throw new Error("group row not found");
      groupRow.focus();
      await user.keyboard("{Enter}");

      expect(screen.getByText("cluster-a1")).toBeInTheDocument();
    });

    it("fires onRowClick for child rows but not group rows", async () => {
      const onClick = vi.fn();
      const user = userEvent.setup();
      await renderComponent(
        <GroupedTable initialExpanded onRowClick={onClick} />,
      );

      await user.click(screen.getByText("account-a"));
      expect(onClick).not.toHaveBeenCalled();

      await user.click(screen.getByText("cluster-b1"));
      expect(onClick).toHaveBeenCalledWith(
        expect.objectContaining({ name: "cluster-b1" }),
      );
    });

    it("keeps a group visible when only a child matches the search", async () => {
      const user = userEvent.setup();
      await renderComponent(<GroupedTable initialExpanded />);

      await user.type(
        screen.getByPlaceholderText("Search accounts..."),
        "cluster-a2",
      );

      await waitFor(() => {
        expect(screen.queryByText("account-b")).not.toBeInTheDocument();
      });
      expect(screen.getByText("account-a")).toBeInTheDocument();
      expect(screen.getByText("cluster-a2")).toBeInTheDocument();
      expect(screen.queryByText("cluster-a1")).not.toBeInTheDocument();
    });

    it("paginates by group rows, keeping children on the parent's page", async () => {
      await renderComponent(<GroupedTable initialExpanded pageSize={2} />);

      expect(screen.getByText("account-a")).toBeInTheDocument();
      expect(screen.getByText("cluster-a1")).toBeInTheDocument();
      expect(screen.getByText("account-b")).toBeInTheDocument();
      expect(screen.getByText("cluster-b1")).toBeInTheDocument();
      expect(screen.queryByText("account-c")).not.toBeInTheDocument();
      expect(screen.getByText(/Showing 1-2 of 3 accounts/)).toBeInTheDocument();
    });
  });
});
