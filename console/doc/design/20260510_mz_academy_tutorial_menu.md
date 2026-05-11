# MZ Academy tutorial menu — design

## Goal

In the Materialize Console SQL Shell, replace the single "Quickstart" toggle in
the header with an "MZ Academy" pull-down menu. The menu lets the user pick
between multiple tutorials. For this iteration there are two:

- **Quickstart** — the existing built-in tutorial (auction load generator).
- **MZ Academy: intro to Materialize** — a new tutorial that walks through
  views, materialized views, indexes, idiomatic Materialize SQL, and temporal
  filters against a pre-seeded e-commerce dataset (PostgreSQL CDC + a Kafka
  clickstream).

The framing is generic: this is useful for any individual learning Materialize
on their own as well as for a group running through the material together.

## Constraints

- The academy tutorial requires a Materialize emulator that has a PostgreSQL
  source on the e-commerce schema, a Kafka clickstream source, and three user
  clusters (`source_cluster`, `transform_cluster`, `serving_cluster`). Step 0
  of the tutorial describes what to verify; if the user runs the SQL without
  the environment, Materialize surfaces helpful errors (object not found,
  etc.) — we do not gate execution.
- Preserve the existing Quickstart behavior exactly when "Quickstart" is the
  active tutorial.
- Match existing console patterns: Chakra UI components, jotai state,
  TypeScript, file/style conventions used elsewhere in `console/src/`.

## Approach

Three approaches considered.

### A. Single Menu replacing the Button
Replace the existing single button with a Chakra `Menu`. When closed the
`MenuButton` shows the currently selected tutorial name plus a chevron. Click
opens a menu with the tutorials; selecting one opens the panel. When the panel
is open the button morphs to "Close X". Pros: one affordance. Cons: the button
has two click meanings (open menu when closed, close panel when open) — that
flipping is confusing and breaks the discoverability of the picker once the
panel is open.

### B. Split button — main toggle + chevron picker  (chosen)
A small HStack: a primary Button to toggle the panel for the currently-selected
tutorial, plus an `IconButton` showing a chevron that opens a Menu listing the
tutorials. Pros: each control has a single, obvious meaning, and the chevron is
a clear signal that there's more than one tutorial. Cons: two adjacent header
controls instead of one. Recommendation: B.

### C. Tabs inside the tutorial panel
Keep one button (renamed to "MZ Academy") that toggles the panel. Inside the
panel, put a `Tabs` at the top to pick the active tutorial. Pros: no new header
affordance. Cons: switching tutorials requires opening the panel first, so the
common case ("I want to start the academy course") costs an extra click and is
hidden until discovered.

**Chosen: B.** Aligns with established split-button patterns (Chakra examples
under `console/src/platform/`) and keeps the existing one-click toggle behavior
intact.

## Components

### `store/tutorialIds.ts`  (new)
Single source of truth for the set of tutorial IDs:

```ts
export const TUTORIAL_IDS = ["quickstart", "academy"] as const;
export type TutorialId = (typeof TUTORIAL_IDS)[number];

export const TUTORIAL_LABELS: Record<TutorialId, string> = {
  quickstart: "Quickstart",
  academy: "MZ Academy: intro to Materialize",
};

export const TUTORIAL_SHORT_LABELS: Record<TutorialId, string> = {
  quickstart: "Quickstart",
  academy: "MZ Academy",
};
```

### `store/shell.ts`  (modified)
Extend `ShellState` with `activeTutorial: TutorialId`. Persist to
`localStorage` under `mz-shell-active-tutorial`. Default `quickstart` to
preserve existing first-run behavior. Switching tutorials resets
`currentTutorialStep` to 0.

### `Tutorial.tsx`  (refactored)
The existing module-level `stepsData` is the Quickstart's content. Refactor so
the file exports a `TUTORIALS` registry keyed by `TutorialId`, and the
`<Tutorial>` component reads the active tutorial from `shellStateAtom` and
selects the right step array. The text label that today reads "QUICKSTART"
above the progress bar becomes the active tutorial's `TUTORIAL_SHORT_LABELS`
value, uppercased.

Shared step primitives (`Runnable`, `TextContainer`, `RunnableContainer`,
`StepLayout`) move from `Tutorial.tsx` into a sibling `TutorialPrimitives.tsx`
so both tutorial step files can import them.

### `AcademyTutorial.tsx`  (new)
Exports `academyStepsData: StepData[]` with the following sections, each
written in the same prose style as the existing Quickstart (concise, runnable
SQL via the `<Runnable>` primitive). Topic coverage:

1. **Introduction / environment check.** Explains that the tutorial assumes a
   PostgreSQL CDC + Kafka clickstream environment already wired up. Names the
   seed schemas and the expected sources/tables so the learner can verify by
   running `SHOW SOURCES; SHOW TABLES;`.
2. **Clusters.** Three-cluster architecture (`source_cluster`,
   `transform_cluster`, `serving_cluster`) and what each is for.
3. **Sources.** PG source + Kafka source overview; verify with `SHOW SOURCES`.
4. **Views.** Build `shopping_cart_line_item_subtotals`,
   `shopping_cart_totals`, `shopping_cart_checkout`,
   `current_totals_all_shopping_carts`. Explain re-computation cost.
5. **Materialized views.** Stack `product_sales` view under a
   `product_performance` materialized view; compare `EXPLAIN` plans.
6. **Indexes — basics.** Index on a view + index on a materialized view;
   point-lookup vs full-scan; cluster-local property.
7. **Indexes — reuse.** Index sharing within a cluster; creation order matters;
   verify with `mz_internal.mz_materialization_dependencies`.
8. **Idiomatic SQL — top-K.** `LATERAL` + `ORDER BY LIMIT` over `RANK() OVER`;
   `DISTINCT ON` for top-1.
9. **Idiomatic SQL — aggregate-as-window rewrites.** `COUNT(*) OVER (PARTITION
   BY ...)` rewritten as a `GROUP BY` view + join.
10. **Temporal filters.** `mz_now()` constraints (comparison-only, AND-only in
    materialized/indexed views); top-100-customers-in-last-minute example.
11. **Filter pushdown.** Filter-then-materialize vs materialize-then-filter; the
    `pushdown=` annotation in `EXPLAIN MATERIALIZED VIEW`.
12. **MCP server.** Brief: the emulator serves `/api/mcp/developer` on port
    6876; link to docs for setting up a Claude Code / MCP client. (No
    interactive MCP setup inside the tutorial — that's out of scope for a
    web-based panel.)
13. **Summary / what's next.** Pointers to docs and patterns.

All SQL strings are typed out fresh inline; the tutorial does not depend on
any external materials.

### `ShellHeader.tsx`  (modified)
Replace the single `<Button>` for "Quickstart" with:

```
<HStack>
  <Button …onClick={toggleVisibility}>
    {tutorialVisible
      ? `Close ${TUTORIAL_SHORT_LABELS[activeTutorial]}`
      : TUTORIAL_SHORT_LABELS[activeTutorial]}
  </Button>
  <Menu>
    <MenuButton as={IconButton} icon={<ChevronDownIcon />} … />
    <Portal>
      <MenuList>
        {TUTORIAL_IDS.map(id =>
          <MenuItem onClick={() => selectTutorial(id)}>
            {TUTORIAL_LABELS[id]}
          </MenuItem>)}
      </MenuList>
    </Portal>
  </Menu>
</HStack>
```

`selectTutorial(id)` updates `activeTutorial`, persists to localStorage, resets
`currentTutorialStep` to 0, and opens the panel.

## Data flow

```
ShellHeader (dispatch tutorial select / toggle visibility)
   ↓
shellStateAtom (jotai) — { tutorialVisible, activeTutorial, currentTutorialStep }
   ↓
Tutorial.tsx — reads activeTutorial, looks up TUTORIALS[activeTutorial],
              renders that stepsData via the existing Steps/Progress UI.
```

## Error handling

- Tutorials run user SQL via the existing `runCommand` plumbing. Errors surface
  in the shell history exactly as they do today (no new code path).
- LocalStorage-stored `activeTutorial` is validated against `TUTORIAL_IDS` on
  load; if it's a stale/unknown value (e.g. an older build wrote something
  else), we fall back to `quickstart`.

## Testing

- Existing console test suite (`yarn test`) must stay green. Specific files to
  watch: `Shell.test.tsx`, `TutorialSchemaWidget.test.tsx`,
  `TutorialInsertionWidget.test.tsx`.
- Manual browser smoke test against the running emulator using Playwright (or
  manual): open the SQL Shell, verify default tutorial is Quickstart, switch to
  MZ Academy, navigate steps, run a Runnable from a step.

## Deployment to the running emulator

For the duration of this work the materialized container is already running
with the stock console bundle at `/usr/share/nginx/html`. Iterating:

1. `yarn build:local` in `console/` produces a fresh `dist/`.
2. `docker cp console/dist/. mz101-materialized:/usr/share/nginx/html/` swaps
   the bundled console in the running container. nginx serves from disk so no
   restart is needed.

A full release would re-build the `materialized` mzbuild image with the new
console, which is out of scope for the AFK exercise.

## Out of scope (YAGNI)

- User-authored tutorials.
- Server-side tutorial registry / dynamically-loaded tutorial content.
- Per-tutorial completion tracking (we keep a single `currentTutorialStep`,
  reset on switch).
- Localized strings.
- New theming for the menu/header.
- Interactive MCP setup inside the academy panel.
