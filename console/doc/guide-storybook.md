# Storybook Guide

How to develop console components in isolation, without running the app.

## Overview

[Storybook](https://storybook.js.org/docs) renders a single component on its own
page with data you supply, so you can see states that are hard to reach in a
running console: a table mid-load, a list with no rows, a graph where every
series is breaching at once. Each state is a **story**, written beside the
component, so it stays there instead of living in a harness that gets deleted
when the PR lands.

Stories are development fixtures, not tests. They assert nothing and do not run
in CI.

## Running it

```bash
yarn storybook          # dev server with hot reload, http://localhost:6006
yarn storybook:build    # static build, into storybook-static/ (gitignored)
```

## Writing a story

Create `<Component>.stories.tsx` next to the component. Storybook's
[Writing Stories](https://storybook.js.org/docs/writing-stories) covers the
format, [args](https://storybook.js.org/docs/writing-stories/args) covers props,
and [Controls](https://storybook.js.org/docs/essentials/controls) covers the
live props panel.

[`src/components/Table/UniversalTable.stories.tsx`](../src/components/Table/UniversalTable.stories.tsx)
is a worked example in this repo.

Two things the upstream docs will not tell you:

**A hook cannot go directly in `render`.** A story's `render` is a plain
function, not a component, so `react-hooks/rules-of-hooks` fails the lint. See
React's [Only call Hooks from React functions](https://react.dev/reference/rules/rules-of-hooks#only-call-hooks-from-react-functions).
Move the hook into a wrapper component:

```tsx
// Fails lint: `render` is not a component.
export const Default: Story = {
  render: (args) => {
    const table = useUniversalTable({ data: args.data, columns: COLUMNS });
    return <UniversalTable table={table} />;
  },
};

// Works.
const ClusterTable = ({ data }: { data: Cluster[] }) => {
  const table = useUniversalTable({ data, columns: COLUMNS });
  return <UniversalTable table={table} />;
};

export const Default: Story = {
  render: (args) => <ClusterTable data={args.data} />,
};
```

Controlled components need the same wrapper: hold the value there with
`useState`, or the control renders but does not respond.

**Keep fixture data deterministic.** Seed anything generated rather than using
`Math.random()` or `Date.now()`, and use synthetic names. Never put customer,
vendor, or account names in a story file.

## Themes

Every story runs inside the providers `App.tsx` puts above a screen, minus
routing and data fetching, so our theme applies as it does in the app. The
**Theme** toolbar control switches light and dark.

Check both before opening a PR. Most components inherit dark mode from Chakra
tokens without referencing color mode explicitly, so a regression there is easy
to miss.

## Gotchas

**Storybook's build does not type-check.** A successful `yarn storybook:build`
does not mean the app compiles. `yarn typecheck` and `yarn lint` remain the
gate. Both cover story files today because `tsconfig.json` declares no `include`
list; if one is added, stories need to stay in it.

**No Chakra section in the sidebar.** Remote Storybook composition is disabled
in `.storybook/main.ts`, because Chakra's published Storybook tracks a later
major than the version this app pins and advertises components we do not have.
Use [v2.chakra-ui.com](https://v2.chakra-ui.com) for Chakra's documentation.

**Two Vite plugins are filtered out** of the inherited app config:
`vite-plugin-html`, which would boot the whole console inside the story iframe,
and the Sentry plugin, which needs credentials unavailable here. Everything else
is inherited, so `~/` aliases, SVG imports, and WASM work as in the app.
