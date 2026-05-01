// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { createColumnHelper } from "@tanstack/react-table";
import { screen, waitFor } from "@testing-library/react";
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

const SearchableTable = ({ data = testData }: { data?: TestCluster[] }) => {
  const table = useUniversalTable({ data, columns });
  return (
    <div>
      <TableSearch
        onValueChange={table.setGlobalFilter}
        placeholder="Search clusters..."
      />
      <UniversalTable table={table} data-testid="test-table" />
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
});
