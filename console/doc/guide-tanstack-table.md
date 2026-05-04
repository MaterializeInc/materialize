# TanStack Table Guide

How to build interactive data tables in the console with sorting, search, and pagination.

## Overview

The console uses [TanStack Table](https://tanstack.com/table/latest) (v8) — a headless library that provides table logic (sorting, filtering, pagination, global search) without controlling rendering. We pair it with Chakra UI components for the visuals.

## Files

Everything lives in [`src/components/Table/`](../src/components/Table/). Import directly from each module:

| File | Purpose |
| --- | --- |
| `useUniversalTable.ts` | React hook wrapping TanStack's `useReactTable`; also exports `getInitialTableState` for URL-driven initial state |
| `UniversalTable.tsx` | Renders the table with Chakra UI |
| `TableSearch.tsx` | Debounced global search input |
| `TablePagination.tsx` | Page count text and prev/next buttons; auto-hides on a single page |
| `tableColumnBuilders.ts` | `sortingFunctions.nullsLast` and `globalTextFilter` |
| `tableTypes.ts` | TypeScript interfaces and `ColumnMeta` augmentation |

## Quick start

The examples in this guide use a simple `User` type so the patterns are easy to follow. Substitute your own type when adapting them.

```tsx
interface User {
  id: string;
  name: string;
  email: string;
  role: "admin" | "member";
  createdAt: string; // ISO date
}
```

### Basic table with sorting

A minimal sortable table. Click any column header to sort.

```tsx
import { createColumnHelper } from "@tanstack/react-table";
import { UniversalTable } from "~/components/Table/UniversalTable";
import { useUniversalTable } from "~/components/Table/useUniversalTable";

const columnHelper = createColumnHelper<User>();

const columns = [
  columnHelper.accessor("name", {
    header: "Name",
    sortingFn: "alphanumeric",
  }),
  columnHelper.accessor("email", {
    header: "Email",
    sortingFn: "alphanumeric",
  }),
];

export const UserList = ({ users }: { users: User[] }) => {
  const table = useUniversalTable({ data: users, columns });
  return <UniversalTable table={table} />;
};
```

### Full-featured table with search, pagination, and clickable rows

A typical list page: search across columns, paginate at 25 rows, and navigate on row click.

```tsx
import { VStack } from "@chakra-ui/react";
import { useNavigate } from "react-router-dom";
import { TablePagination } from "~/components/Table/TablePagination";
import { TableSearch } from "~/components/Table/TableSearch";
import { UniversalTable } from "~/components/Table/UniversalTable";
import { useUniversalTable } from "~/components/Table/useUniversalTable";

export const UserList = ({ users }: { users: User[] }) => {
  const navigate = useNavigate();
  const table = useUniversalTable({
    data: users,
    columns,
    initialSorting: [{ id: "name", desc: false }],
    pageSize: 25,
  });

  return (
    <VStack spacing={4} align="stretch">
      <TableSearch
        onValueChange={table.setGlobalFilter}
        placeholder="Search users..."
      />
      <UniversalTable
        table={table}
        variant="linkable"
        onRowClick={(user) => navigate(`/users/${user.id}`)}
        isLoading={!users}
      />
      <TablePagination table={table} itemLabel="users" />
    </VStack>
  );
};
```

## Important: keep `columns` static

This is the most common bug when working with TanStack Table. **Don't put dynamic data in the `columns` `useMemo` dependency array.** Doing so causes the entire table to remount on every change, which breaks tooltips, popovers, and other interactive elements inside cells.


// columns are static. Read dynamic data inside the cell renderer.
const columns = React.useMemo(
  () => [
    columnHelper.accessor("name", {
      cell: (info) => <UserNameCell user={info.row.original} />,
    }),
  ],
  [],
);

const UserNameCell = ({ user }: { user: User }) => {
  const userStatus = useUserStatus();
  return (
    <HStack>
      <Text>{user.name}</Text>
      {userStatus.data?.[user.id]?.isOnline && <OnlineIcon />}
    </HStack>
  );
};
```


## Column patterns

### Custom cell rendering

Cells can render any React content. Use `info.getValue()` for the cell value or `info.row.original` for the full row.

```tsx
columnHelper.accessor("role", {
  header: "Role",
  cell: (info) => (
    <Badge colorScheme={info.getValue() === "admin" ? "purple" : "gray"}>
      {info.getValue()}
    </Badge>
  ),
});
```

### Computed columns

Columns that don't map directly to a field. Use an accessor function with a stable `id`:

```tsx
columnHelper.accessor((row) => row.email.split("@")[1], {
  id: "domain",
  header: "Domain",
  sortingFn: "alphanumeric",
});
```

### Action menu column

A column for `OverflowMenu` actions. Disable sorting and pin a fixed width.

```tsx
import OverflowMenu, { OVERFLOW_BUTTON_WIDTH } from "~/components/OverflowMenu";

columnHelper.display({
  id: "actions",
  header: "",
  cell: (info) => (
    <OverflowMenu
      items={[
        {
          visible: info.row.original.role !== "admin",
          render: () => <DeleteUserMenuItem user={info.row.original} />,
        },
      ]}
    />
  ),
  enableSorting: false,
  size: OVERFLOW_BUTTON_WIDTH,
});
```

### Conditional columns

Add or remove columns based on a flag. Wrap the array in `useMemo` — and only depend on the flag, not on changing data.

```tsx
const columns = React.useMemo(() => {
  const cols = [
    columnHelper.accessor("name", { header: "Name" }),
    columnHelper.accessor("email", { header: "Email" }),
  ];

  if (showRole) {
    cols.push(columnHelper.accessor("role", { header: "Role" }));
  }

  return cols;
}, [showRole]);
```

### Date and timestamp columns

Format inside the cell renderer; sort with `"datetime"` for `Date` objects or `"basic"` for ISO strings.

```tsx
import {
  formatDate,
  FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
} from "~/utils/dateFormat";

columnHelper.accessor("createdAt", {
  header: "Created",
  sortingFn: "basic",
  cell: (info) =>
    formatDate(info.getValue(), FRIENDLY_DATETIME_FORMAT_NO_SECONDS),
});
```

### Header tooltips and responsive widths

The `meta` field carries metadata consumed by `UniversalTable`:

```tsx
columnHelper.accessor("role", {
  header: "Role",
  meta: {
    tooltip: "Whether the user can manage other users",
    minWidth: { md: "120px", sm: "auto" },
  },
});
```

This adds an info icon next to the header text and applies a responsive `min-width`.

### Nullable columns

Use `sortingFunctions.nullsLast` so empty values don't get jumbled with real ones:

```tsx
import { sortingFunctions } from "~/components/Table/tableColumnBuilders";

columnHelper.accessor("lastLoginAt", {
  header: "Last login",
  sortingFn: sortingFunctions.nullsLast,
  cell: (info) => info.getValue() ?? "Never",
});
```

> **Note:** TanStack inverts the comparison for descending sort, so nulls move to the top in descending order. Either way, nulls stay grouped together.

## Sorting functions

Use TanStack's built-ins whenever possible:

| `sortingFn` | When to use |
| --- | --- |
| `"alphanumeric"` | Strings that may contain numbers (handles "item2" vs "item10") |
| `"text"` | Plain case-insensitive text — faster than `alphanumeric` |
| `"basic"` | Numbers, booleans, dates — simple `<` / `>` comparison |
| `"datetime"` | `Date` objects |
| `sortingFunctions.nullsLast` | Any column where the value can be `null` / `undefined` |

For one-off custom logic, pass an inline function:

```tsx
columnHelper.accessor("name", {
  header: "Name",
  sortingFn: (rowA, rowB, columnId) => {
    const a = rowA.getValue<string>(columnId);
    const b = rowB.getValue<string>(columnId);
    return a.localeCompare(b, undefined, { numeric: true });
  },
});
```

## API reference

### `useUniversalTable(options)`

Wraps TanStack's `useReactTable` with the core, sorted, filtered, and paginated row models pre-wired. Pass any TanStack option directly. Convenience shorthands:

| Option | Description |
| --- | --- |
| `data` | Array of rows. Required. |
| `columns` | Array of column definitions. Required. |
| `initialSorting` | Shorthand for `initialState.sorting`. |
| `pageSize` | Shorthand for `initialState.pagination.pageSize`. Defaults to `25`. |

Returns a TanStack `Table` instance. Use its native API — `table.getState()`, `table.setGlobalFilter("foo")`, `table.previousPage()`, etc. See the [TanStack docs](https://tanstack.com/table/v8/docs/api/core/table).

### `<UniversalTable />`

| Prop | Description |
| --- | --- |
| `table` | The instance returned by `useUniversalTable`. Required. |
| `variant` | Chakra table variant. Defaults to `"linkable"`. See [Variants](#variants). |
| `onRowClick` | Callback receiving the row's data when clicked. |
| `isLoading` | When `true`, renders skeleton rows in place of data. |
| `skeletonRowCount` | Number of skeleton rows. Defaults to `5`. |
| `data-testid` | Forwarded to the root `<Table>` element. |

```tsx
<UniversalTable
  table={table}
  variant="linkable"
  onRowClick={(user) => navigate(`/users/${user.id}`)}
  isLoading={!users}
/>
```

### `<TableSearch />`

A debounced global search input. The component is **uncontrolled** — `initialValue` is read once on mount. To programmatically reset the input, pass a different `key` prop.

| Prop | Description |
| --- | --- |
| `onValueChange` | Called with the new value after the debounce window. Required. |
| `initialValue` | Initial input value. Defaults to `""`. |
| `placeholder` | Defaults to `"Search"`. |
| `debounceMs` | Debounce delay. Defaults to `250`. |

```tsx
<TableSearch
  onValueChange={table.setGlobalFilter}
  placeholder="Search users..."
/>
```

The default `globalTextFilter` matches across every cell's stringified value. Override by passing `globalFilterFn` to `useUniversalTable`.

### `<TablePagination />`

Renders page count text and prev/next buttons. Auto-hides when `pageCount <= 1`.

| Prop | Description |
| --- | --- |
| `table` | The table instance. Required. |
| `itemLabel` | Label used in "Showing X-Y of Z {itemLabel}". Defaults to `"results"`. Accepts `ReactNode`. |

```tsx
<TablePagination table={table} itemLabel="users" />
```

Renders something like: `Showing 1-25 of 87 users    [<]  1 of 4  [>]`

## Variants

The `variant` prop maps to the Chakra theme in [`src/theme/components/Table.ts`](../src/theme/components/Table.ts):

| Variant | Use case |
| --- | --- |
| `"linkable"` *(default)* | Rows are clickable and navigate somewhere |
| `"standalone"` | Read-only data display, no row interaction |
| `"borderless"` | Embedded inside a card or other container |
| `"shell"` | Terminal-style listings |
| `"rounded"` | Plain bordered table |

## URL state sync

Reflect sort, page, and search in the URL so users can bookmark or share table views. Two pieces:

- **Read:** `getInitialTableState` parses the URL once and seeds `useUniversalTable`'s initial state.
- **Write:** the project-wide [`useSyncObjectToSearchParams`](../src/hooks/useSyncObjectToSearchParams.ts) hook (same pattern as `useConnectorListSortOptions` and `QueryHistoryList`) writes state changes back.

```tsx
import React from "react";
import { useSyncObjectToSearchParams } from "~/hooks/useSyncObjectToSearchParams";
import { UniversalTable } from "~/components/Table/UniversalTable";
import {
  getInitialTableState,
  useUniversalTable,
} from "~/components/Table/useUniversalTable";

export const UserList = ({ users }: { users: User[] }) => {
  // Read URL once on mount — feeds initial state, no flicker.
  const initial = React.useMemo(
    () => getInitialTableState(window.location.search),
    [],
  );

  const table = useUniversalTable({
    data: users,
    columns,
    initialSorting: initial.sorting ?? [{ id: "name", desc: false }],
    pageSize: 25,
    initialState: {
      pagination: { pageIndex: initial.pageIndex ?? 0, pageSize: 25 },
      globalFilter: initial.globalFilter ?? "",
    },
  });

  // Write current state back whenever it changes.
  const { sorting, pagination, globalFilter } = table.getState();
  useSyncObjectToSearchParams(
    React.useMemo(
      () => ({
        sort: sorting[0]?.id,
        dir: sorting[0]?.desc ? "desc" : "asc",
        page: pagination.pageIndex + 1,
        q: globalFilter,
      }),
      [sorting, pagination.pageIndex, globalFilter],
    ),
  );

  return <UniversalTable table={table} />;
};
```

URL format: `?sort=name&dir=asc&page=2&q=ada`

## Migrating an existing table

A typical Chakra-only list page has a manual `.map()` over rows. To migrate:

1. Define a `columnHelper` typed to your row type
2. Translate each `<Th>` / `<Td>` pair into a column definition with `header` and (if needed) `cell`
3. Replace the `<Table>` JSX with `<UniversalTable>`
4. Optionally add `<TableSearch>` and `<TablePagination>`

**Before:**

```tsx
<Table variant="linkable">
  <Thead>
    <Tr>
      <Th>Name</Th>
      <Th>Email</Th>
    </Tr>
  </Thead>
  <Tbody>
    {users.map((u) => (
      <Tr key={u.id} onClick={() => navigate(`/users/${u.id}`)}>
        <Td>{u.name}</Td>
        <Td>{u.email}</Td>
      </Tr>
    ))}
  </Tbody>
</Table>
```

**After:**

```tsx
const columnHelper = createColumnHelper<User>();
const columns = [
  columnHelper.accessor("name", { header: "Name", sortingFn: "alphanumeric" }),
  columnHelper.accessor("email", { header: "Email", sortingFn: "alphanumeric" }),
];

const UserList = ({ users }: { users: User[] }) => {
  const navigate = useNavigate();
  const table = useUniversalTable({ data: users, columns });
  return (
    <UniversalTable
      table={table}
      onRowClick={(u) => navigate(`/users/${u.id}`)}
    />
  );
};
```

You get sorting for free. Render `<TableSearch>` and `<TablePagination>` to add search and pagination.

## Troubleshooting

| Symptom | Likely cause |
| --- | --- |
| Tooltips disappear / table flickers when data refreshes | Dynamic dependencies in your `columns` `useMemo`. See [Important: keep columns static](#important-keep-columns-static). |
| Column header doesn't respond to clicks | Missing `sortingFn` on the column, or `enableSorting: false`. |
| Search returns no matches | The default `globalTextFilter` checks all cells via `String()`. Custom values without a sensible string representation won't match — pass a custom `globalFilterFn` or normalize the value in the accessor. |
| Pagination buttons don't appear | Your data fits in one page. `TablePagination` auto-hides. Use a smaller `pageSize` to test. |
| TypeScript error about `SortingFn<unknown>` | Cast the column's `sortingFn` to `SortingFn<YourType>`, or rely on `sortingFunctions` (already typed for any data type). |
| Action column shows a sort indicator | Set `enableSorting: false` on the column. |

## Running the tests

```bash
yarn test src/components/Table/
```
