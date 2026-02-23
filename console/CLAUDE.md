# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

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
ln -s ../../misc/githooks/pre-commit .git/hooks/pre-commit
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

## API & Data Fetching

### Kysely Query Builder

Kysely provides type-safe SQL against the Materialize system catalog:

```tsx
// Query builder pattern
export const buildClustersQuery = () => {
  return queryBuilder
    .selectFrom("mz_clusters as c")
    .select(["c.id", "c.name"])
    .$if(condition, (qb) => qb.select("extra_field"))
    .compile();
};
```

- Use `sql` tagged template for raw SQL to prevent injection
- Use `$if()` only for optional selects (per Kysely docs)

### useSql Hooks

- `useSql` / `useSqlTyped` - Declarative data fetching with automatic polling/refetch
- `useSqlLazy` - Imperative calls for form submissions and user actions
- `useSqlMany` - Multiple queries in a single request

### Error Handling

- Don't expose HTTP status codes in user-facing error messages
- Use generic fallback: "An error occurred" for unexpected errors
- Consider transactions when creating multiple related objects
- Extract common error messages to shared utilities

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
