---
name: mz-console-review
description: >
  Review a Console (`console/`) PR or diff the way Console reviewers do: run the
  change to verify claims, check it against the patterns and helpers the app
  already has, and post findings as one GitHub review. Trigger: "review this
  console PR", "review my console changes", "does this console change look ok",
  a github.com/MaterializeInc/materialize/pull/<N> link whose files are under
  `console/`, or a request to look over a React/TypeScript diff in this repo.
  For Rust changes use mz-pr-review instead.
argument-hint: [pr-number]
allowed-tools: [Bash, Read, Grep, Glob]
---

# Console code review

`mz-pr-review` covers the Rust checklists, none of which apply under `console/`.
The canonical Console rules live in `console/CLAUDE.md` (component conventions,
import restrictions, TypeScript and React patterns, testing philosophy) and in
`console/doc/` (`guide-testing.md`, `guide-tanstack-table.md`,
`mz-backwards-compatibility.md`). Read the sections the diff touches. Do not
restate them in comments, cite them.

This skill is what Console reviewers actually flag. Section 4 is distilled from
roughly 570 reviewer comments on about 150 Console PRs from 2025 and 2026, and is
ordered by how often each kind of feedback came up. Sections 5 and 6 are not from
that tally. They cover cost and structure that reviewers rarely catch by
reading, measured on `mz_catalog_server` and in render-count probes.

## 1. Get the diff, and make it runnable

Remotes are usually `upstream` = `MaterializeInc/materialize` and `origin` = a
personal fork, so a PR branch will not resolve against `origin`:

```bash
git fetch upstream pull/<N>/head:pr-<N>-review
```

To run anything you need the files inside `console/`, because `node_modules`
lives at `console/node_modules` and a fresh worktree does not have it. Check the
files into a tree with no local edits under `<path>`, run, then clean up. The
restore removes files the PR added and reverts the ones it changed, and it also
discards any local edits under `<path>`:

```bash
git checkout pr-<N>-review -- console/src/<path>
(cd console && npx vitest run src/<path>)
git restore --source=HEAD --staged --worktree -- console/src/<path>
git branch -D pr-<N>-review
```

Confirm `git status --short` is empty before you finish.

## 2. Verify every behavioral claim before writing it

Run the suite and state the result ("30/30 pass"). Probe anything you intend to
assert. A claim about draw order, nullability, or event handling that you reasoned
out but did not execute is a claim you will get wrong.

Vitest hides `console.log` in passing tests. Assert against a sentinel to see a
value: `expect(actual).toEqual("MARKER")`.

jsdom makes graph and pointer assertions silently vacuous:

| Symptom | Cause | Fix in the probe |
|---|---|---|
| Geometric assertions pass trivially | `ParentSize` measures nothing, width is 0 | `vi.mock("@visx/responsive/lib/components/ParentSize", ...)` with a fixed size |
| Pointer handler commits `NaN` | no `createSVGPoint` / `getScreenCTM`, so visx `localPoint` fails | `vi.mock("@visx/event", () => ({ localPoint: vi.fn(() => ({ x, y })) }))` |

Measure colors instead of judging them: `d3.lab(color).l` gives perceived
lightness. `d3` is already a dependency.

For SQL claims, compile the builder and read the SQL. Kysely's `.select` appends,
it does not replace, so a "fallback" select can emit a duplicate column.

## 3. Read the PR before the code

Reviewers raise these before any line comment:

* **Motivation is traceable.** A linked issue (a Linear key such as `Closes: CNS-192`, or `Fixes database-issues#N` for a legacy GitHub issue), a Slack thread, or a design doc. If a reviewer cannot tell why the change exists, ask for context before reviewing.
* **UI changes link the Figma design** in the description.
* **Stacked PRs are linked** in each description, or the work is one PR with logical commits. Review semantic commits one at a time when the author asks.
* **Commit messages are imperative and descriptive** per `doc/developer/guide-changes.md`, not "addressed feedback".
* **A removed compatibility shim states its floor.** If the PR deletes a version map or gate, the description names the minimum supported environment version.
* **WIP code is marked** with `TODO` (spelled `TODO`, not `TO-DO`), including leftover `console.log`.
* **New pages and routes sit behind a LaunchDarkly flag.** The point is developer experience: it lets the work merge incrementally. Self-managed flags default to `true` in `src/config/flexibleDeploymentFlags.ts`, so a flag meant to stay off there must be set to `false` explicitly.

## 4. What reviewers flag, most frequent first

### 4.1 Remove and simplify

The single most common request. Look for:

* Unused variables, atoms, exports, props, and query keys. Variables that are always `true`.
* Redundant fallbacks (`?? 0` behind a value that cannot be null) and conditionals that render nothing.
* Wrapper hooks that re-expose library options as custom `enable*` props. Keep wrappers thin, since the library's own options are better documented.
* Features with no requirement behind them (column pinning, LRU layers). Ask "is there a requirement for this?"
* A mini rules engine where two `if` statements would do.
* Extra state flags (`destroyed`, `initialized`) that every method must check, and piles of refs. Stale refs are a bug source.
* A copied error state when the mutation hook already exposes `error`.
* `!!x` inside an expression that is already boolean, and several early returns that can merge into one.

### 4.2 SQL and catalog correctness

* **Nullable columns.** `disk_bytes` and friends are nullable. Coalesce, and fix the cast. When dividing, check the *denominator* for zero, not each operand.
* **`ORDER BY ... DESC` puts nulls first.** Add `nulls last` where the column is nullable.
* **User objects are `id LIKE 'u%'`**, via `isSystemId` in `~/api/materialize`. Not `name NOT LIKE 'mz_%'`, and not a bare `startsWith("u")`.
* **Statistics tables keep rows for dropped replicas** for up to a day. Aggregates like `bool_and(snapshot_committed)` must filter to extant replicas.
* **Sources:** filter to top-level sources the way `sourceList.ts` does. Subsources and progress collections re-enter through joins on `mz_hydration_statuses`. Newer sources expose subsources as tables.
* **Latest vs max.** Is the query returning the most recent sample or the largest one, and is that what the UI claims?
* **A windowed flag must not drive a point-in-time answer.** "Unreadable at some point in the window" is not "unreadable now". Check which columns and predicates a window-wide flag reaches.
* **Multi-process skew.** Summing usage over processes hides a single process about to OOM. Prefer `MAX`, and prefer existing catalog views over recomputing their math client-side.
* **Two surfaces, two answers.** When a PR touches one of several queries for the same metric (list vs detail page), check the staleness bounds and column semantics match. A column can mean different things in the old and new relation.
* Factor a repeated join or subquery into `src/api/materialize/expressionBuilders.ts` and left-join it, rather than `GROUP BY` just to pull one column.
* Aggregate with `jsonb_agg` / `list_agg` and format on the client, instead of `STRING_AGG` or `pg_size_pretty` in SQL.
* Raw SQL goes through the `sql` tagged template. Qualified names are structured (`{ databaseName, schemaName, name }` quoted with `sql.id()` or `quoteIdentifier`), never a dotted string, which breaks on names containing periods.
* Comment catalog behavior the docs do not cover (for example `object_sub_id IS NULL` meaning the relation itself), and rewrite clever predicates for intent (`BOOL_AND(x IS NOT NULL)` over count arithmetic).

### 4.3 Reuse before re-rolling

Before accepting any new helper, grep for what it replaces. Reviewers name these most:

| Re-rolled | Already exists |
|---|---|
| clamp, pluralize, capitalize, null-guard | `clamp`, `pluralize`, `capitalizeSentence`, `notNullOrUndefined` in `~/util` |
| Percent and byte text | `formatPercentage(fraction, decimals)` (takes a fraction. Call it directly and handle null inline, since a local `formatPercent` wrapper reads as a re-roll), `formatBytesShort`, `humanizeBytes`, `formatMemoryUsage` in `~/utils/format` |
| Sorting | d3 `sort(iterable, ...accessors)` (multi-key ties, returns a copy), `handleSortOrder` in `~/utils/sort`, `sortingFunctions` (`nullsLast`, `numericNullsLast`) in `components/Table/tableColumnBuilders.ts`. The same comparator written twice becomes one shared helper |
| `60_000`-style time constants | `millisecondsInMinute`, `millisecondsInHour` from `date-fns/constants` (not the `date-fns` root in v4) |
| Object type text | `kebabToTitleCase`, `kebabToSentenceCase`, `kebabToScreamingSpaceCase` (SQL keywords such as `MATERIALIZED VIEW`) in `~/util`, `formatObjectType` in `platform/clusters/format.ts` |
| Route strings built by hand | path helpers in `~/platform/routeHelpers` (`regionPath`, `useBuildWorkflowGraphPath`, ...). Add one there when it is missing |
| Logic copied from a neighboring page | extract one helper and switch both call sites to it, so the copies cannot drift |
| System-object check | `isSystemId` in `~/api/materialize` |
| Duration formatting, lag caps | `formatInterval`, `formatDurationForAxis` in `~/utils/format` (fractional units are reserved for seconds). `formatDurationForAxis` is for axes: it rounds to whole minutes and caps at 24h, so 90s reads "2m" in text. The 24h cap `MAX_LAG_MS` is module-private there and `FreshnessGraph.tsx` declares its own copy, so export one instead of adding a third |
| An anchor with custom hover/underline | `TextLink` from `~/components/TextLink` |
| Status colors copied from a pill | export `colorSchemeToStyles` from `StatusPill.tsx` |
| Layout props threaded one by one | the `containerProps` pattern, see `Gauge.tsx` |
| A new cluster-sizes query | `useAvailableClusterSizes` / `fetchAvailableClusterSizes` |
| A new "all objects" fetch | `useAllObjects` and the global subscribes (`useGlobalUpsertSubscribe`) |
| A new filter dropdown | mirror `SchemaFilter.tsx` / `DatabaseFilter.tsx` (reserved "all" id, caller sets width) |
| A new DAG layout | `dagreGraphHelpers.ts` and the workflow graph components |
| A `useEffect` + timeout debounce | `lodash.debounce` (see `ObjectExplorer.tsx`) |
| Previous-value or latest-value refs, or a callback read inside a debounce or timer | `usePrevious`, `useLatestRef` from `@chakra-ui/react` (see `useAutomaticallyConnectSocket.ts`) |
| Tab active state from `location.pathname` | `NavLink`'s `isActive`, see `PageTabStrip` |
| Set diffing by hand | `Set.prototype.difference` |
| Secret masking | export `formatAppPassword` from `~/queries/frontegg` |
| A per-tenant localStorage key | export `buildCacheKey` from `platform/shell/commandCache.ts` |
| Chart axes, grid, tooltip, legend, cursor | `buildXYGraphLayoutProps` and friends in `~/utils/graph`, `~/components/graphComponents`, `useChartLegend`, `useCursorState`, `colors.lineGraph` |
| visx plumbing | `Line` from `@visx/shape` for straight lines (as `GraphLineCursor` does), `Group` from `@visx/group` instead of repeating offset arithmetic on each child, `ParentSizeProvidedProps` to type a `ParentSize` child's props, `useDrag` from `~/hooks/useDrag` for draggable handles |
| Several small components for one feature | one `xComponents.tsx` file, like `formComponents.tsx` |

When a new component duplicates an existing one, say which pages it lands on and
what those pages have today.

### 4.4 Tests

Value over volume. Look for:

* **Tests that assert on query-builder string output, SQL snapshots, CSS attributes, or class names.** Replace with SQL tests (`*.test.sql.ts`) that create real objects and assert results, or with user-visible assertions.
* **Missing corner cases.** Ask for the harder fixture: a deeper role hierarchy, overlapping buckets, multiple inherited roles, users that can log in. For graph fixtures, an ASCII diagram in a comment helps.
* **Assert the outcome, not the call.** After a `GRANT`, query `mz_internal.mz_show_all_privileges`.
* **Fixtures that do not look like real data.** Mock only the relation under test, and make values realistic.
* **`vi.mock` / `vi.spyOn` on app modules.** Reviewers dislike them (unstable, break on circular imports). Prefer MSW via `buildSqlQueryHandlerV2`, with per-call query keys when one mutation issues several statements, or dependency injection.
* **Redundant happy-path slices.** One happy-path test plus separate corner cases beats ten tests that all break together. Multiple assertions per integration test are fine.
* Unnecessary setup (`GRANT CREATEROLE` when the test already logs in as a superuser).
* Regex over `toContain` for generated code, `!` over `?.` inside tests so they fail early, no `if` branches around expects, constants hoisted out of `describe` in `SCREAMING_SNAKE_CASE`.
* Every bug found in review gets a regression test. New helpers and classes get unit tests.
* A test that asserts a function's arity or a mock's call shape breaks when someone adds an optional parameter. Assert what the user sees instead.

### 4.5 Types and nullability

* No `any`, no casts that the types do not need (`as unknown as T`).
* Ask why each field is nullable when every caller defines it.
* Fields that are always null together become one nested object (`cluster: { id, name } | null`).
* Two booleans that can conflict (`usersOnly`, `rolesOnly`) become a discriminated union.
* Boolean props are optional (`canIssue?: boolean`) so `<X canIssue />` works. An error prop is `error?: boolean`, not `unknown`.
* Type a required key as required instead of throwing when it is missing.
* `===` over `==`. Prefer `undefined` over `null` for optional values.

### 4.6 Naming

* Units are suffixes: `durationMs`, not `ms`.
* Booleans are present tense (`hasActiveSubscription`). Components are nouns, not verbs (`LicenseKeyCtaContent`, not `GetLicenseKey`).
* `Inner` suffix only for a suspense boundary split, `Content` for a block of content.
* `build...Query` is reserved for SQL builders. Transforms are `transform...`, fetchers `fetch...`, DOM listener setup `register...`.
* Do not alias a name as an id (`parent.name as parentId`). SQL aliases are camelCase, named for the general concept (`relationComment`, not `tableComment`).
* Callback props keep parity with DOM events (`onPointMove`, `onClick`), not new verbs like `onPointSelect`.
* A variant that changes styling is a `variant` prop, not inferred from which callbacks were passed.
* Icons end in `Icon`. Version gates are named constants (`MCP_OAUTH_MIN_VERSION`), not inline strings.
* Loop variables say what they index (`bucketIndex`, not `j`).

### 4.7 Code comments

The rules are in the root `CLAUDE.md` "Code comments" section. What reviewers
flag in practice: comments with fluff, details that make no sense out of context
("avoids alphanumeric sorting issues"), and comments that no longer match the
code. They ask *for* comments on undocumented catalog behavior, on unconventional
uses (react-query used only to get loading/error state for an initializer), on
non-obvious algorithms, and on props whose meaning is subtle.

### 4.8 Loading, error, and empty states

* Every `useQuery` renders distinct loading, error (`isError`), and empty UI. A mutation failure is visible to the user, not only `console.error`.
* No state falls through to an endless spinner (a disabled region is an error, not "loading").
* Loading is a spinner inside the card, not "Loading..." text, which shifts layout on every refetch. Empty states render inside the card, not as a blank page.
* Prefer `useQuery` with explicit states over new `useSuspenseQuery`.
* **The global subscribes start empty.** `useAllClusters` and `useAllObjects` return `data: []` until `snapshotComplete`, so a page that reads empty as "none" shows "No clusters" or a false success on a cold load. Gate on `snapshotComplete`, and render the subscribe's error.
* An entity with nothing to report gets one empty state, not a green "0 of 0 within target" summary over an empty chart.

### 4.9 Data layer

* Query keys start with `buildRegionQueryKey(...)` (`~/api/buildQueryKeySchema`), so switching region or organization never serves another region's cache.
* Queries live under `src/api/materialize/...`, not in `platform/`.
* `staleTime` / `gcTime` are deliberate. If data never goes stale, use `Infinity`, and invalidate on logout or mutation.
* Refresh derived data by putting its query key in the namespace that mutations already invalidate, rather than polling (a 60s poll means 60s-stale ownership).
* No new `useSql*` hook calls. `console/CLAUDE.md` ("React Query") has the read and write pattern.
* Global subscribes are hoisted to the route that owns the section, or navigating remounts them and the view jitters.
* Validate user-supplied parameters at the `build...Query` layer (see the start-time check in `queryHistoryList.ts`), not only in the UI.
* Anything persisted to localStorage is scoped by organization and region, and cleared on region switch. An unscoped key leaks one tenant's catalog into another session.

### 4.10 React hooks and state

* No `useEffect` to sync props into state or to react to a user action. Do it in the event handler, or initialize state in the modal's `onOpen`.
* Effect dependencies hold the primitive that matters (the JWT string, not the whole query result), so work reruns only when it should.
* TanStack table `columns` are static. Dynamic data flows through `data`, or cells remount and tooltips vanish.
* One owner per lifecycle: a connection manager owns connect, disconnect, and retry, and the hook is a thin shell. Shell output changes go through the `websocketFsm.ts` state machine.
* For Jotai, a derived write-only atom with an early return, not returning the previous object and hoping it skips a render.
* A helper that mutates variables outside its scope is harder to read than the same code inline with a comment.

### 4.11 Routing and URL state

* View modes and modal flows (graph vs table, edit role) are subroutes, so a reload keeps the user's place.
* Filter and lookback state belongs in the URL, not localStorage, when it should survive reload and share.
* Do not branch on `location.pathname` when `<Route>` already branches.
* A URL-selected entity that does not exist (a dropped cluster in an old link) must not silently show a different one. Write the fallback into the URL, or say it was not found.
* Imports use `~/` unless the file shares a parent directory.

### 4.12 Versions, flags, and deployment modes

* A query that reads a new column or relation breaks older environments. Gate it with `useEnvironmentGate` and a named version constant, per `console/doc/mz-backwards-compatibility.md`. Self-managed can run a new Console against much older environments.
* Test across versions with emulator images on another port (`misc/docker/README.md` in the Console tree) or a local kind cluster.
* Cloud-only initialization goes in `CloudAppInitializer`, not `AppInitializer` (flexible deployments and impersonation).
* `forceShow` on a nav item breaks when the environment is disabled.
* Superuser checks through `usePrivileges` hit `has_table_privilege` and load environmentd. Use `useIsSuperUser`.
* An `AppConfigSwitch` whose branches render the same thing is noise.

### 4.13 Visual and UX QA

Reviewers run the branch and attach screenshots or recordings. If you can run it
(`console/CLAUDE.md` has the dev commands), do. Otherwise list these for the human
reviewer:

* Overflow, especially in modals, filters, and form footers.
* Dark mode. Any literal color (`colors.green[50]`, hex, `hsl(`) is suspect, since it does not flip.
* Spacing on the 4px grid in relative units (`3`, not `"12px"`). Valid `textStyle` values come from `src/theme/typography.ts`. Valid Chakra tokens for `zIndex`. Chakra style props over `style`.
* **Types do not catch invalid style values.** Chakra passes `alignItems="left"` straight through (Chrome computes it as `normal`, which `CSS.supports("align-items", "left")` confirms), and an undefined `textStyle` such as `text-ui-sm` falls back to the default without an error.
* Consistency with existing patterns: the app's purple color scheme, existing button hover behavior, filter chips like query history, an Apply button for unit inputs. "We don't do this anywhere else" is a common and valid objection.
* Layout shift on refetch, jitter on navigation, flicker on load.
* Thresholds that agree with each other (a value turns orange at 5s but the filter starts at a minute).
* Copy: title case in titles, correct terminology ("role with LOGIN attribute", not "privilege"), where to save a generated config, a link to the docs page.
* Semantic elements (`ul`/`li` for lists).
* **Mouse-only click targets.** A clickable `Tr`, `HStack` or legend pill with `onClick` cannot be reached from the keyboard. Make it a button (`as="button"`, `type="button"`) with `aria-pressed` for toggles (see `connectComponents.tsx`), and prefer `:focus-visible` over an always-on focus ring.
* One placeholder for a missing value across a page (`-` or `—`, not both).

### 4.14 Security and session traps

Rare but high severity, and most often caught by automated QA review after merge:

* Tenant or region state that survives a switch: caches, module-level singletons, persisted rows seeded with `snapshotComplete: true`.
* Auth flows where the client clock or cached token decides a redirect. Redirects should be server-authoritative, or users loop.
* Two concurrent sessions (password and OIDC) where queries silently run as the wrong account.
* State-changing `GET`s, and protections that rely on another system's cookie config (`SameSite`) instead of checking `Origin` / `Sec-Fetch-Site`.
* `btoa` on non-ASCII input. Service accounts where the code assumes a user email.

### 4.15 Charts and graphs

Graph PRs collect the same gotchas. Check each:

* **Reuse first.** The 4.3 rows for graph helpers and visx plumbing. A block near identical to `FreshnessGraph`, `UtilizationGraph` or `ThresholdLineGraph` should come from the same pieces.
* **Hover redraws only the cursor and tooltip.** Mark layers (lines, bars, rows) are `React.memo` with stable props: scales and layouts memoized on width and domain, series memoized. A memo only helps if callers hoist their arrays. One built inline reruns every render.
* **Volatile inputs get their own layer.** Anything derived from `useAllObjects` or `useAllClusters` gets a new identity on any change anywhere in the environment. Draw it as its own memoized layer, or key its memo by content, so an unrelated DDL does not redraw the chart.
* **Which point is hovered.** `findNearestDatum` returns the nearest bucket start, which near a bar's right edge is the next bar. For bucketed bars, find the bucket that contains the time.
* **Decorative layers keep `pointerEvents="none"`** and sit under the event overlay, or a full-width strip swallows hover.
* **Labels state only what the data says.** A max-per-bucket series is a peak, not an average. Name the unit precisely ("% of replica CPU allocation"), and do not label an inferred state as recorded.
* **Wide windows stay readable.** Bars narrower than a few pixels alias into stripes. Merge buckets client-side, keeping each metric's peak.
* **Colors.** Series take `colors.lineGraph`. A generated palette varies perceived lightness (`d3.hcl` / `d3.lab`), not HSL lightness.
* **Axis formatters are for axes.** `formatDurationForAxis` and tick formatters round. Headline and table values need the unrounded formatter.
* **Refresh cost.** A chart that polls re-reads its whole window on every refetch. See 5.1 in `cost-and-structure.md`.

### 4.16 Design docs

For `doc/design/` PRs, reviewers ask for concrete ranges and limits, error states
(an unresponsive cluster), reuse of existing global subscribes and components,
consistency when two polled queries resolve at different times (a subscribe
avoids it), client-side formatting, and a scoped first version (superuser only)
before the general case.

## 5. Check what the change costs

When the diff changes a query builder, a subscribe, a `refetchInterval` or
`staleTime`, or anything that runs at pointer or keystroke rate, read section 5
of `cost-and-structure.md` in this skill's directory and work through it. The
`mz_catalog_server` cost and the render cost it checks do not show up in a read
of the diff.

## 6. Audit hooks and component structure

When the diff adds or moves a `useEffect`, `useMemo`, `useCallback`, or
`React.memo`, or splits a component, read section 6 of `cost-and-structure.md`.

## 7. Write the comment the way a Console reviewer writes it

Findings are triaged with the person you are reviewing for first. Present the
list, let them cut and rephrase, then post. Do not post a review off your own draft.

Comments are short, hedged, and collaborative. State the claim, give at most one
clause of why, name the alternative, stop. Typical openers: `Nit:`, `I think we
should`, `Maybe we can`, `Could we reuse`, `Let's just`, `I'd recommend`,
`I wonder if`, `Kinda feels like`, `Why is X nullable here?`, `How does this look
in dark mode?`.

Real comments at the right length:

> nit: We usually like to use `<>` for react fragments

> Can most likely reuse the TextLink component

> Why is objectId nullable here? Usage of this hook shows me that it's always defined

> Kinda feels like these should be grouped into a `cluster` object, especially if both of them can be null

> Let's remove this comment.

Keep measurements and probe output out of the comment. They belong in your reply
to the person you are reviewing for. A bug claim is the exception: attach the
repro (a screenshot, a recording, or a short diff that triggers it), because that
is what makes it actionable.

Point at the existing pattern with a permalink rather than describing it. Offer
pseudocode or a suggestion block when the fix is not obvious. Say when something
is not blocking: "Doesn't need to be blocking but I suggest in a followup PR,"
"We can do this as a followup."

## 8. Post as a single review

One review, not N comments, so the author gets one notification:

```bash
gh api repos/MaterializeInc/materialize/pulls/<N>/reviews \
  --method POST --input review.json
```

`review.json` is `{commit_id, body, event: "COMMENT", comments: [...]}`. Use
`event: "COMMENT"` unless the person asked to approve or request changes. Each
inline comment is `{path, line, side: "RIGHT", body}`, and a multi-line anchor adds
`start_line` and `start_side`. General findings go in `body`.

Permalink every code reference and pin the SHA:

* Code already on main gets the PR's **base** SHA (`gh pr view <N> --json baseRefOid`).
* Code inside the PR gets the **head** SHA.
* Verify each cited line resolves before posting: `git show "<sha>:<path>" | sed -n '<line>p'`. When the SHA is in a shell variable, brace it (`"${sha}:${path}"`). zsh reads `$sha:c` as a modifier and drops the colon, even inside quotes.

Afterwards, confirm what landed:

```bash
gh api "repos/MaterializeInc/materialize/pulls/<N>/comments?per_page=100" \
  --jq '[.[] | select(.pull_request_review_id == <id>)] | length'
```

Before posting, check for a pending review. GitHub allows one pending review per
person per PR, so a stray one blocks a new review, and one pinned to an older
commit cannot take new comments because their lines resolve against that commit:

```bash
gh api "repos/MaterializeInc/materialize/pulls/<N>/reviews?per_page=100" \
  --jq '[.[] | select(.state == "PENDING")]'
```

* **Inline comments must fall inside a diff hunk.** Read the ranges with `gh api "repos/MaterializeInc/materialize/pulls/<N>/files" --jq '.[].patch' | grep '^@@'`. Anything outside goes on the file (`subjectType: FILE`) or in the body.
* **Replies and new threads in one review** need GraphQL: `addPullRequestReview` with `commitOID`, then `addPullRequestReviewThread` and `addPullRequestReviewThreadReply` with that `pullRequestReviewId`, then `submitPullRequestReview` with `event: COMMENT`.
* **Pending comments are invisible to REST**, where `pulls/comments/<id>` returns 404. Read and edit them through GraphQL (`updatePullRequestReviewComment`).
* **Link the priorities.** When a review mixes bugs and nits, list the bugs in the body with `#discussion_r<id>` links so the author fixes those first.
* **Pace the batches.** When the author has just received a large review, post the blocking items and hold structural suggestions until they respond.

## 9. Re-review after the author pushes

* **Diff from the last SHA you reviewed, not from the base:** `git fetch upstream pull/<N>/head:pr-<N>-r2 && git diff <last-reviewed-sha> pr-<N>-r2`.
* **Sort each earlier comment into fixed, partly fixed, or open by reading the code at the new head.** Replies do not count, and a fix without a reply is fine. Thread state comes from GraphQL `reviewThreads { isResolved isOutdated }`.
* **Probe every fix for the bug it might introduce.** Removing an effect left a debounce that could strand a drag handle, and scoring unreadable objects as breaching reached a "right now" column. Both passed the existing tests.
* **When the author pushes back with numbers, check they measured what you suggested,** and time a plan label rather than trusting it. `lookup` against `differential join` said nothing about which was faster. Reply in the thread with your own measurement, since numbers belong in that reply.
* **Re-run the suite on the new head,** and say so in the body.

## Output

Group findings as **Blocking**, **Strong suggestions**, **Nits**, and mark
follow-up candidates. Open the summary with one specific sentence on what the PR
does well ("Loving the tests!", "Great work making this page dynamic, and it uses
a lot of our existing patterns"), then the verdict ("Small nits but otherwise
looking really good", "Once my comments are addressed, LGTM"). Name the test
result. Flag anything you could not verify rather than asserting it.
