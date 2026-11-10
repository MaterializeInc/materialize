# What past Console dependency upgrades needed

Distilled from the Console upgrade PRs of October and November 2026, the bot PRs
they replaced, and Renovate's lock file maintenance (#40580). Every rule names
the PR that taught it. Add to this file when a triage teaches something new.

## Reading the PR

- **Check which manifest it touches.** The same package lives in several
  `package.json` files. The jsdom and Vitest majors in #40565 to #40576 only
  touched `ci/test/docs-widgets/`, not `console/`.
- **Look for a newer bot PR or a human PR for the same package first.** Bots
  regroup and supersede their own PRs (#40155 → #40242, #40341 → #40394), and
  humans often have a migration open already (#39610 next to #40629, #39684
  next to #40637).
- **Read every release note in the range, patch releases too.** jsdom 30.1.2
  stopped focusing `display: none` elements (#39683). react-virtualized-auto-sizer
  1.0.26 started measuring with fractional sizes (#39609). Neither was a major.
  When a release has only compare links, read the source diff (#40629).
- **0.x minors are breaking.** Renovate's semver `isBreaking` is true for every
  update from a 0.x version, but its `updateType` stays `minor`, so this repo's
  rule automerges it on green CI. #40629 (codemirror-toolkit 0.6 → 0.7) showed
  "Automerge: Enabled" while it needed a migration. kysely 0.28 → 0.29 was the
  same shape (#39534).
- **`devDependencies` doesn't mean tooling.** `p-retry` is a devDependency that
  the app imports and Vite bundles. Grep imports to decide (#40638).
- **A bumped `resolutions` entry is a forced override.** Renovate moved the
  security floors for `js-yaml` and `diff` across majors (#40634, #40633), which
  forces the new major onto every transitive consumer, including tools behind
  `gen:api` and `gen:types`. List each consumer's declared range. If the entry
  is only a security floor, the latest release of the old major may be enough.

## Shapes of breaking change to grep for

- Removed default export (use-resize-observer 10, #39609).
- A new `exports` map that breaks deep imports, including `vi.mock` paths
  (visx 4, #39611).
- Bundled types replacing `@types/*`, with a default export that is now a value
  instead of a namespace (dagre 3, #39685).
- Sub-packages folded into the main package, breaking imports we never
  declared in `package.json` (Chakra 2.9, #39613).
- Renamed config that is silently ignored: msw 3's `onUnhandledRequest` became
  `onUnhandledFrame` (#39684), vite-plugin-svgr 5 ignores `esbuildOptions` and
  reads `oxcOptions` (#39683). Confirm the option still takes effect.
- Removed APIs renamed under a new hook model (codemirror-toolkit 0.7 removed
  `createCodeMirrorWithContext` and `useViewDispatch`, #39610 and #40629).
- Stricter types in minor releases: msw 2.15 typed the cloned request body,
  downshift 9.4 made `selectedItem` nullable, a Frontegg minor made the signup
  email optional (#40580).

## What CI can't see

- **Real browsers.** Jsdom does no layout. visx 4's `ParentSize` rendered 0px
  tall in a parent without a fixed height, and no test caught it (#39611).
  Measurement libraries (ResizeObserver, AutoSizer, ParentSize) need a headless
  Chromium or preview check.
- **Old browsers.** CI runs current Chromium. kysely 0.29 calls
  `Array.prototype.toSorted` (#39686). string-width 8 emits `v`-flag regexes
  that Chrome 109 can't parse, so the whole Console fails to load there
  (#39687). Check the production bundle against browserslist `defaults`.
- **Node floors.** msw 3 and kysely 0.29 need Node 22.12 or later (ESM-only,
  and kysely-codegen 0.20 `require`s kysely). CI and Volta run Node 26 while
  `engines.node` still allows 20 (#39684, #39686, #39683).
- **Dev scripts.** `yarn gen:types` was broken from kysely 0.28 (#35669) until
  #40550, because CI never runs it. kysely-codegen 0.20 also started writing a
  file into `node_modules` unless `outFile` is `null`. `lint-staged` runs only
  from the opt-in local pre-commit hook (#40636).
- **Generated output.** For a query builder, compile every affected query shape
  on both versions and diff the SQL and parameters (64 shapes in #39686). For a
  layout algorithm, run old and new on generated inputs and bisect the change
  (dagre, 270 random graphs, #39685).
- **Duplicate copies of packages that compare by identity.** A second
  `@lezer/highlight` copy silently turns off SQL highlighting (#39610). Two
  copies of visx internals installed side by side (#39611). Grep `yarn.lock`
  for duplicates of `@lezer/*`, `@emotion/*`, `@types/react`, visx.
- **Flakes under load.** msw 3's socket-level fetch mocking (#39684, #40637)
  and Chakra 2.10's delayed modal focus (#39613) failed intermittently. Run the
  touched tests, or the full suite for test infrastructure, several times in a
  row (#39684 used five).
- **Tests that pass for the wrong reason.** Loading-state tests that assert
  before the mock responds pass on any version (#39615). Test changes for a
  bump should pass on both the old and the new version (#39683).
- **UX behavior.** Chakra 2.10 changed popover and menu z-index, outside-click
  and Escape handling, and focus lock (#39613). Turn changelog items like these
  into a preview checklist ordered by risk.
- **Security.** When a dependency changes how it stores keyed data (plain
  objects, `in` checks), check call sites that key by user-controlled strings
  such as role or object names (#39685).

## Groups, lock files and holds

- **One broken build turns about seven checks red.** A failed `yarn build`
  fails `console-unit-tests`, both e2e jobs, both Vercel previews and both
  `rust-build-*` jobs, since the Rust builds bundle the Console (#39533,
  #40393, #40580). Find the first real error in `console-lint-and-typecheck`
  or the build before reading anything else.
- **A group fails as a whole.** The vite group (#40393) bundled svgr 5 and
  jsdom 30 majors with safe updates, and nothing landed. Name the member that
  breaks and propose splitting it out, as #39683 and #40521 did.
- **Lock file maintenance is a batch of unreviewed minors.** #40580 floated
  vite-plugin-svgr to 4.5 (broke `yarn build`), jotai to 2.20 (broke 82 tests
  from 2.10 on), and re-resolved `@types/react@*` to React 19 types. The fix
  pinned `jotai` to `~2.9.3` and added a `@types/react@*` resolution.
- **Holds must live in code or config.** The auto-sizer 1.0.25 hold existed only
  in #39609's description, and #40521 undid it. Dependabot `ignore` comments
  (auto-sizer 2.x, Chakra 3) didn't carry over to Renovate, though the Chakra
  major ignore was ported to `.github/renovate.json5`. Flag a PR that undoes a
  hold, and suggest encoding new holds as a `package.json` pin or an
  `allowedVersions` rule with a comment.
- **Current holds:** `jotai` `~2.9.3`, the `@types/react@*` resolution to React
  18 types, and Chakra majors disabled in `.github/renovate.json5`.

## Working with Renovate PRs

- **Don't push to a Renovate branch.** Renovate then posts "will not
  automatically rebase this PR" and won't autoclose it (#40637, #40629).
  Whoever pushed owns every later rebase.
- **Re-run CI on the merged lock file before approving a stale PR.** Several
  dependency PRs land in a week, and branches don't have to be up to date to
  merge (review request on #39613). Renovate can also rebase after an approval
  without asking for a new one (#40636).
- **An approval doesn't merge a red PR.** Auto-merge waits silently (#40341).
  Report red as red.
- **Don't blame the bump for an unrelated red job without evidence.**
  `console-e2e-slash-prod` and `kafka-rtr` failed on unrelated PRs in the same
  hour (#39683, #40241). msw is used only by unit tests, so an e2e failure on an
  msw PR isn't msw (#40637).
- **Read existing review comments, including bot reviews, before the verdict.**
  An automated QA review caught the visx collapse on #39508 and a security
  regression on #39685.

## By package

| Package | What it needed |
|---|---|
| `@chakra-ui/*` | 2.9 folded sub-packages into `@chakra-ui/react`, removed `keyframes` (now `@emotion/react`), typed toast `render` strictly. `Menu` calls `scrollTo`, so jsdom needs a stub in `src/__mocks__/globalMocks.js`. Modal focus is delayed. Majors disabled (#39613, #40521) |
| `@codemirror-toolkit/react`, `@lezer/*` | 0.7 replaced the context API. `useViewEffect` reruns when its setup changes identity. Keep one `@lezer/highlight` copy, guarded by `CommandBlock/theme.test.ts` (#39610, #40629) |
| `@dagrejs/dagre`, `@dagrejs/graphlib` | dagre 3 bundles types and changes node order within a rank in most graphs (#39685) |
| `@frontegg/*` | Minors changed signup and auth types. Typecheck, then try login on the preview (#40341, #40394, #40580) |
| `@hookform/resolvers` | 7.0 broke only the typebox resolver. We import only `/zod` (#40454) |
| `jotai` | 2.10 adds the environment version to query keys, so default msw handlers stop matching (82 tests). Held at `~2.9.3` (#40580) |
| `jsdom` | 30.1.2 doesn't focus `display: none` elements. Query by role, not label (#39683) |
| `kysely`, `kysely-codegen` | 0.29 rejects `limit(sql.raw(...))`, calls `toSorted`, and is ESM-only. kysely-codegen 0.20 renamed `schemas` to `defaultSchemas`, lists materialized views itself, and writes into `node_modules` without `outFile: null`. Run `yarn gen:types` against a real Materialize (#39686, #40550) |
| `msw` | 3 renamed `onUnhandledRequest` to `onUnhandledFrame` (silently ignored), mocks fetch at the socket, and needs Node 22.12. `src/api/mocks/server.ts` uses the experimental `defineNetwork` with a TODO tied to undici 8.12.3 (#39684) |
| `react`, `react-dom`, `@types/react` | 19 needs peer-range bumps across the UI stack first, a types codemod, and awaited `act` in tests. A `@types/react` 17 copy can keep the global `JSX` namespace alive and hide errors (#39615) |
| `react-virtualized-auto-sizer` | 1.0.26 measures fractional sizes, which the Shell's exact scroll-to-bottom check may not tolerate. 2.x changes the API (#39609) |
| `string-width` | 8 emits `v`-flag regexes that Chrome below 112 can't parse (#39687) |
| `use-resize-observer` | 10 removed the default export and reports 0 instead of `undefined` (#39609) |
| `vite-plugin-svgr` | 4.5 and 5 transform with oxc under Vite 8. Needs `lang: "tsx"`, passed as `oxcOptions` in 5 (#39683, #40580) |
| `@visx/*` | 4 added an `exports` map and changed `ParentSize` layout. Keep all `@visx/*` on one version (#39611) |
