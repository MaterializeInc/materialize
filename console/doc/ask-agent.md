# `\ask` Agent and MCP Panels

Work in progress, dev-server only. The `\ask` shell command and the MCP-backed
panels are unreleased and ungated. Read "Before this can ship" before putting
any of it in front of users.

## What is wired

| Surface                             | Location                                            | Backing endpoint                  |
| ----------------------------------- | --------------------------------------------------- | --------------------------------- |
| `\ask <question>` in the SQL Shell  | `src/platform/shell/Shell.tsx`                      | `/api/ask` + `/api/mcp/developer` |
| `\health`, `\freshness`, `\sources` | `src/platform/shell/Shell.tsx`                      | `/api/mcp/developer`              |
| `\products`                         | `src/platform/shell/Shell.tsx`                      | `/api/mcp/agent`                  |
| Attention feed, Data products       | `src/platform/environment-overview/`                | both                              |
| MV freshness panel                  | `src/platform/object-explorer/MvFreshnessPanel.tsx` | `/api/mcp/developer`              |
| Cluster impact banner               | `src/platform/clusters/ClusterImpactBanner.tsx`     | `/api/mcp/developer`              |

The agent loop is `src/api/materialize/consoleAgent.ts`. The MCP transport is
`src/api/materialize/mcpClient.ts`.

## How the key is handled

The console is a client-only SPA, so it cannot hold an Anthropic key. The split:

- The **browser** runs the tool-use loop. When the model calls
  `query_system_catalog`, the browser runs that query itself through
  `mcpClient`, using the logged-in user's own session. Catalog reads never
  leave the user's authenticated session, so RBAC is preserved for free.
- Only the **model completion** is proxied. `vite.config.ts` registers an
  `/api/ask` dev middleware that injects `ANTHROPIC_API_KEY` server-side and
  forwards the request body to `api.anthropic.com`. The key is read via
  `loadEnv` and is never bundled or sent to the client.

The middleware lives in `configureServer`, so it exists **only under
`yarn start`**. A production build has no `/api/ask` and `\ask` will fail
there. Shipping this needs a real key-holding endpoint, see below.

## Running it locally

1. Put your key in `console/.env.local` (git-ignored):

   ```
   ANTHROPIC_API_KEY=sk-ant-...
   ```

2. Start the dev server as usual:

   ```
   yarn start
   ```

3. Open the SQL Shell and ask a question:

   ```
   \ask why is my materialized view behind?
   ```

The shell prints each query the agent ran, then its answer. `\ask` with no
argument prints usage. The other slash commands need no key, they are plain
MCP calls.

If the key is missing or still a placeholder, `/api/ask` returns a 500 whose
message names the file to fix. `/api/ask` must be registered before Vite's
internal `/api/` proxy, otherwise the request is forwarded to environmentd
instead.

## What the target environment must provide

- `enable_mcp_developer` and `enable_mcp_agent`, both default `true`. When off,
  the endpoints return 503 and `mcpClient` surfaces that as a disabled-endpoint
  error.
- The `mz_internal.mz_ontology_*` relations, which are built in. The system
  prompt tells the model to query `mz_ontology_entity_types`,
  `mz_ontology_link_types`, and `mz_ontology_properties` first so it discovers
  real table and column names instead of guessing them. This ontology-first
  step is what keeps the generated joins correct, so do not trim it from the
  prompt.
- A CORS-allowed origin for the dev host when pointing at a bare emulator.
  environmentd rejects Basic-auth requests carrying an untrusted `Origin` as a
  DNS-rebinding defense, which shows up as a browser 403 while the same request
  succeeds under `curl`. Pass `--cors-allowed-origin=http://localhost:3000` as
  an emulator CLI flag. The `CORS_ALLOWED_ORIGIN` environment variable does not
  work for this.

## Model

`claude-opus-5`, pinned in two places that must agree: `MODEL` in
`consoleAgent.ts`, and the fallback default in the `/api/ask` middleware in
`vite.config.ts`.

Opus 5 runs adaptive thinking by default, so no `thinking` parameter is sent.
Thinking shares the output budget, which is why `max_tokens` is 16000 and not
the couple of thousand a non-thinking model needs. Dropping it back down
truncates answers mid-diagnosis. Reasoning is not surfaced in the shell: the
default thinking display is `omitted`, so thinking blocks arrive with empty
text and the shell renders only text blocks. The loop replays whole assistant
`content` arrays, so those blocks are echoed back correctly across rounds.

To trade cost against depth, add `output_config: { effort: ... }`, `low`
through `max`, default `high`. Note that the middleware rebuilds the outgoing
request from a fixed set of fields, currently `model`, `max_tokens`, `system`,
`tools`, and `messages`. Anything else the client sends, `output_config` and
`thinking` included, is dropped silently, so a tuning parameter has to be added
in both files to take effect.

`MAX_TOOL_ROUNDS` in `consoleAgent.ts` caps the loop at 8 rounds. On reaching
it the agent returns a "stopped" answer rather than throwing.

## Before this can ship

- **No feature gating.** The panels render unconditionally on the environment
  overview, cluster overview, and MV detail pages. Each needs a LaunchDarkly
  flag before this merges for real.
- **No production key path.** `/api/ask` is dev middleware. Cloud console is on
  Vercel, so the shape there is a Vercel Function; self-managed console is
  served by environmentd, so it needs its own key-holding sidecar. Note that
  `vercel.json` currently carries only header rules, so an `/api/*` route is
  not shadowed.
- **No streaming.** `/api/ask` buffers the whole completion, so the shell shows
  "Investigating..." until the full answer lands. Multi-round questions take a
  while.
- **No tests.** None of the agent loop, the proxy, or the panels are covered.
- The MV freshness panel derives lag severity by string-matching the interval
  text, which is brittle and should read a typed interval instead.
