# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this directory.

## Build and Development Commands

```bash
# Install dependencies
corepack enable
yarn install

# Start local dev server (points to staging cloud)
yarn start

# Start with local cloud stack
DEFAULT_STACK=local yarn start

# Build for production
yarn build

# Build for local testing (no Sentry)
yarn build:local

# Type checking
yarn typecheck

# Linting (zero warnings allowed - CI will fail on any warning)
yarn lint          # Check for issues
yarn lint:fix      # Auto-fix issues (run before committing)

# Run unit/integration tests
yarn test

# Run a single test file
yarn test path/to/file.test.ts

# Run SQL tests (requires mzcompose - see doc/guide-testing.md)
yarn test:sql

# Run E2E tests (requires local cloud stack - see doc/guide-testing.md)
yarn test:e2e

# Generate API types from OpenAPI specs
yarn gen:api

# Generate database types from Materialize system catalog
yarn gen:types

# Optional: Enable pre-commit hook to auto-run ESLint --fix on staged files
# Run from the materialize repo root:
ln -s ../../console/misc/githooks/pre-commit .git/hooks/pre-commit
```

## Architecture Overview

**Client-only React app** hosted on Vercel, communicating with Materialize Cloud APIs and Materialize database endpoints.

### Key Directory Structure

- `src/api/` - API clients and query builders
  - `materialize/` - Kysely query builders for Materialize system catalog queries
  - `schemas/` - Generated TypeScript types from OpenAPI specs
  - `mocks/` - MSW handlers for testing
- `src/platform/` - Main application routes and pages organized by feature (clusters, sources, sinks, etc.)
- `src/components/` - Reusable UI components
- `src/hooks/` - Custom React hooks
- `src/store/` - Jotai atoms for global state
- `src/theme/` - Chakra UI theme customizations
- `src/test/` - Test utilities and helpers
- `types/materialize.d.ts` - Generated types from Materialize system catalog (via `yarn gen:types`)

### Path Aliases

Use `~/` for imports from src: `import { foo } from "~/api/materialize"`

### State Management

- **Jotai** for global state (sparingly used - prefer component state)
- **React Query** for server state
- **Kysely** as query builder (NOT as database driver) for type-safe SQL against Materialize system catalog

### Stacks

The app uses "stacks" (local, staging, production) rather than "environments" since "environment" refers to a Materialize database. Stack switching is available at runtime.

## Code Style

### Component Conventions

- **Prettier** enforced via ESLint
- Arrow functions for React components (enforced by ESLint)
- Multiple components per file allowed when one consumes another
- Never name component files `index.tsx`
- Props interface named `$ComponentProps` and exported above component:

```tsx
export interface MyComponentProps {
  title: string;
}

export const MyComponent = ({ title }: MyComponentProps) => {
  return <div>{title}</div>;
};
```

### Import Restrictions

These are enforced by ESLint to ensure consistent patterns:

- Use `~/components/Modal` instead of `@chakra-ui/react` Modal (custom wrapper)
- Use `formatDate` from `~/utils/dateFormat` instead of `date-fns` format
- Import from `~/external-library-wrappers/frontegg.ts` instead of `@frontegg/*` (enables mocking)
- Import icons from `~/icons` instead of `*.svg?react`

### TypeScript Patterns

- **Avoid `!` (non-null assertion) outside tests** - easy to hide real errors:
  ```tsx
  // Bad: hides potential bugs
  const length = items!.length;

  // Good: explicit check
  const isEmpty = !items || items.length === 0;
  ```

- **Use `parseInt()` over `+`** for explicit number conversion:
  ```tsx
  // Prefer
  const num = parseInt(str, 10);
  // Over
  const num = +str;
  ```

- **Prefer `undefined` over `null`** for optional values (works with `?` syntax)

- **Use TSDoc for doc comments** so they appear in IDE hints:
  ```tsx
  /** Gets the background color based on status. */
  const getStatusColor = (status: Status) => { ... };
  ```

### React Patterns

- **Keep callback references stable** for hooks that return functions:
  ```tsx
  // Functions from useSql hooks may be used in dependency arrays
  const { refetch } = useSqlTyped(query);
  ```

- **Use `useMemo` for expensive computations** or to keep prop references stable for child components

- **Use `React.lazy()` for code splitting** large components:
  ```tsx
  const HeavyComponent = React.lazy(() => import("./HeavyComponent"));
  ```

- **Use `useSqlLazy` for imperative calls** (form submissions, user actions) instead of `useSql`

- **Prefer `await` over callbacks** for cleaner async flow in form handlers

### Naming Conventions

- `SCREAMING_CAPS` for module-level constants
- No leading underscore except for unused function parameters (`_unused`)
- Use semantic names for regex: `MATERIALIZE_IDENTIFIER_REGEX` not `VALID_NAME_REGEX`
- Query builder functions follow `buildXQuery()` pattern

### File Organization

- Keep constants in component file unless shared across multiple files
- Prefer semantic organization over generic `utils` folders
- Extract components only when actually reusable
- Co-locate tests with components (e.g., `Component.tsx` and `Component.test.tsx`)

### Page Component Structure

Every feature page follows this pattern — outer wrapper with error boundary + suspense, inner component with logic:

```tsx
export const MyPage = () => (
  <AppErrorBoundary fallback={<ErrorBox message="Failed to load data" />}>
    <React.Suspense fallback={<LoadingContainer />}>
      <MyPageInner />
    </React.Suspense>
  </AppErrorBoundary>
);

const MyPageInner = () => {
  const { data, refetch } = useMyData();
  if (!data?.length) return <EmptyListWrapper>...</EmptyListWrapper>;
  return <MainContentContainer>...</MainContentContainer>;
};
```

### Overflow Menu Pattern

Use `OverflowMenu` with `{ visible, render }` objects — conditionally show items without nulls in JSX:

```tsx
<OverflowMenu
  items={[
    { visible: canEdit, render: () => <EditMenuItem /> },
    { visible: canDelete, render: () => <DeleteMenuItem onDelete={refetch} /> },
  ]}
/>
```

### Toast Deduplication

Reuse toast IDs to prevent stacking on repeated events:

```tsx
const TOAST_ID = "my-feature-status";
if (!toast.isActive(TOAST_ID)) {
  toast({ id: TOAST_ID, description: "Connection restored" });
}
```

### Theme Tokens

Reference semantic theme tokens — never use raw color values:

```tsx
// Good: uses theme-aware token
<Box bg="background.secondary" color="foreground.primary" />

// Bad: hardcoded color
<Box bg="#f5f5f5" color="#333" />
```

Light/dark mode palettes are in `src/theme/light.ts` and `src/theme/dark.ts`. Use `useTheme<MaterializeTheme>()` when accessing custom theme properties.

## API & Data Fetching

### Kysely Query Builder

Queries follow a 3-part pattern: builder function, type inference, and fetch wrapper:

```tsx
// 1. Query builder
export const buildClustersQuery = (filters?: Filters) => {
  return queryBuilder
    .selectFrom("mz_clusters as c")
    .select(["c.id", "c.name"])
    .$if(condition, (qb) => qb.select("extra_field"))
    .compile();
};

// 2. Type inference from builder
export type Cluster = InferResult<ReturnType<typeof buildClustersQuery>>[0];

// 3. Fetch wrapper with Sentry span
export async function fetchClusters(params, queryKey, requestOptions?) {
  const compiled = buildClustersQuery(params).compile();
  return Sentry.startSpan({ name: "clusters-query", op: "http.client" }, () =>
    executeSqlV2({ queries: compiled, queryKey, requestOptions })
  );
}
```

- Use `sql` tagged template for raw SQL to prevent injection
- Use `$if()` only for optional selects (per Kysely docs)

### Query Keys

Define hierarchical query keys per feature for targeted cache invalidation:

```tsx
export const clusterQueryKeys = {
  all: () => buildRegionQueryKey("clusters"),
  list: (filters?) => [...clusterQueryKeys.all(), buildQueryKeyPart("list", filters)],
  replicas: (params) => [...clusterQueryKeys.all(), buildQueryKeyPart("replicas", params)],
};
```

### Data Fetching Hooks

- `useSuspenseQuery` — for required data (must be inside Suspense boundary)
- `useQuery` — for optional data
- `useSqlTyped` / `useSql` — declarative data fetching with automatic polling/refetch
- `useSqlLazy` — imperative calls for form submissions and user actions
- `useSqlMany` — multiple queries in a single request
- Set `refetchInterval` for metrics/status queries (not static data)
- Set `staleTime` to reduce unnecessary refetches

### Form Patterns

Forms use React Hook Form with `mode: "onTouched"` (validate on blur, not keystroke):

```tsx
const { register, handleSubmit, setError, formState } = useForm<FormState>({
  mode: "onTouched",
});

// useSqlLazy for submission
const { runSql, loading } = useSqlLazy({ queryBuilder: (values) => buildStatement(values) });

const onSubmit = (values: FormState) => {
  runSql(values, {
    onSuccess: () => { onClose(); toast({ description: "Created" }); },
    onError: (err) => {
      if (isDuplicateNameError(err)) setError("name", { message: "Name exists" });
      else setGeneralFormError(err.message);
    },
  });
};
```

- Separate field-specific errors (`setError`) from general form errors (state)
- Parse SQL errors with helper functions (e.g., `isDuplicateReplicaName()`)
- Toast on success, inline Alert on error
- Loading state on submit button via `isLoading={loading}`

### Error Handling

- Don't expose HTTP status codes in user-facing error messages
- Use generic fallback: "An error occurred" for unexpected errors
- Use custom error classes for specific types (`PermissionError`, `DatabaseError`)
- Parse error details with helpers (`isPermissionError()`, `duplicateReplicaName()`)
- `PermissionError` sets `skipQueryRetry = true` — don't retry auth failures
- Consider transactions when creating multiple related objects

## Testing

For detailed testing instructions, see [doc/guide-testing.md](doc/guide-testing.md).

### Philosophy

- **Don't test implementation details** - test user-visible behavior
- **Add test coverage when fixing bugs** - prevents regressions
- **Extract pure functions and unit test them** - easier to test in isolation

### Test Structure

```tsx
describe("ComponentName", () => {
  beforeEach(() => {
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: queryKeys.list(),
        results: mapKyselyToTabular({ rows: [], columns }),
      })
    );
  });

  it("should show error message when request fails", async () => {
    const { getByText } = await renderComponent(<Component />);
    await waitFor(() => {
      expect(getByText("Error message")).toBeInTheDocument();
    });
  });
});
```

- Use `renderComponent()` from `~/test/utils.tsx` to render with all providers
- Use descriptive test names: "should show error when connection fails"
- One behavior per test when practical

### Debugging Tests

```bash
# Debug SQL mock handlers (most useful)
DEBUG=console:msw:sql yarn test

# Debug all MSW handlers
DEBUG=console:msw yarn test

# Enable all console debug logs
DEBUG=console:* yarn test

# Show console.log output inline
PRINT_TEST_CONSOLE_LOGS=true yarn test
```

Test output goes to `log/test.log` - use `less -R log/test.log` or `grc tail -f log/test.log`.

### Async Testing

```tsx
// Use waitFor for async assertions
await waitFor(() => {
  expect(screen.getByText("Loaded")).toBeInTheDocument();
});

// For animated elements that take time to appear
await waitFor(async () =>
  expect(await screen.findByText("Content")).toBeVisible()
);
```

## Common Pitfalls

### SQL Issues

- **Integer division truncates** - use proper casting for percentages:
  ```sql
  -- May truncate: (used * 100) / total
  -- Better: CAST((used * 100.0) / total AS INTEGER)
  ```

- **Operator precedence** - always group OR conditions:
  ```sql
  -- Wrong: WHERE a OR b AND c  (AND binds tighter)
  -- Right: WHERE (a OR b) AND c
  ```

- **User input escaping** - always use `sql` tagged template for raw SQL

### Async/Timing

- **Use `return await` in try/catch** to catch promise rejections:
  ```tsx
  try {
    return await asyncOperation(); // Correct
  } catch (e) {
    // This won't catch without await
  }
  ```

- **Clear intervals/timeouts on unmount** - prevent memory leaks
- **Race conditions** - check if component is still mounted before state updates

### Browser-Specific

- Add comments for Safari-specific workarounds
- Test visual changes in Safari, Chrome, and Firefox

## Materialize Version Compatibility

Use `useEnvironmentGate` hook to handle different Materialize versions at runtime. For detailed patterns, see [doc/mz-backwards-compatibility.md](doc/mz-backwards-compatibility.md).

When modifying `types/materialize.d.ts` for backwards compatibility:
1. Add new columns for new versions
2. Keep old columns for backwards compatibility
3. Use discriminated unions for changed column types
4. Annotate changes with TODO comments for cleanup after rollout
