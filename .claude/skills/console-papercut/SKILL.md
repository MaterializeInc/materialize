---
name: console-papercut
description: >
  Fix a console UI papercut from Linear and create a PR. Trigger when the user says
  "console papercut", "/console-papercut", "fix papercut", or references a CNS-* issue
  from the Console Papercuts or monthly papercut Linear projects (e.g., April Papercuts).
argument-hint: <Linear issue ID, e.g. CNS-30>
---

# Console Papercut Fix Workflow

Fix a single console UI papercut end-to-end: pull issue from Linear, make a minimal code change using existing codebase patterns, verify in browser, and open a PR.

## Prerequisites

- **Linear MCP** must be connected (for pulling issue details).
- **Playwright MCP** (optional) for browser-based verification. If unavailable, skip to manual verification instructions.
- Dev server: `yarn --cwd console start` should be running or startable.

## Step 1: Pull the issue from Linear

Parse `$ARGUMENTS` for a Linear issue identifier (e.g., `CNS-30`).

If no identifier is provided, list open issues from papercut projects:

```
mcp__claude_ai_Linear__list_issues(
  project: "Console Papercuts",
  team: "Console",
  state: "backlog" OR "todo" OR "in progress",
  limit: 10
)
```

Also check the current monthly project (e.g., "April Papercuts") if the main project returns few results.

Fetch full issue details with `mcp__claude_ai_Linear__get_issue` to get description, comments, and attachments.

## Step 2: Understand the fix

1. Read the issue description and any linked Slack threads or screenshots.
2. Identify which files need to change. Start by searching `console/src/` for relevant components, hooks, or queries.
3. Read the relevant files. **Understand the existing code before making any changes.**
4. Read `console/CLAUDE.md` for code conventions — every change must follow these.

## Step 3: Make the fix

### Reuse existing patterns — do not invent new ones

Before writing any code, search for how similar problems are already solved in the codebase:

- **Need a UI element?** Check `console/src/components/` for existing components first. Use them as-is or extend them — do not create parallel components.
- **Need a hook?** Check `console/src/hooks/` for existing hooks. If a custom hook exists for the pattern you need (localStorage sync, SQL fetching, toast deduplication), use it.
- **Need a utility?** Check `console/src/utils/` and the file you're editing for local helpers. Add to existing utils if the function is genuinely reusable.
- **Need a query?** Follow the `buildXQuery()` + `InferResult` + `fetchX()` pattern used in `console/src/api/materialize/`. Look at sibling query files for the exact conventions.
- **Need error handling?** Use existing error classes (`DatabaseError`, `PermissionError`) and parser helpers from `console/src/api/materialize/parseErrors.ts`.
- **Need a form?** Follow the React Hook Form + `useSqlLazy` + `mode: "onTouched"` pattern from existing modals in `console/src/platform/`.
- **Need a page layout?** Use `AppErrorBoundary` + `React.Suspense` + `MainContentContainer` like every other page.
- **Need an overflow menu?** Use `OverflowMenu` with `{ visible, render }` item objects.
- **Need toast notifications?** Reuse toast IDs to prevent stacking (`toast.isActive(ID)` check).

If you add a new component or utility, make sure it follows the same structure, naming, and export patterns as its neighbors.

### Minimal diffs only

- Change ONLY what is needed to fix the reported issue.
- No drive-by refactors, extra comments, docstrings, or type annotations on unchanged code.
- No new abstractions or helpers for one-off fixes.
- Follow the existing patterns in the file you are editing — match its style exactly.
- Use the project's import aliases (`~/`) and restricted imports (see CLAUDE.md).
- Props interfaces named `$ComponentProps`, arrow functions for components, no `index.tsx` files.
- Prefer `undefined` over `null`, `parseInt()` over `+`, `useSqlLazy` for mutations.
- Reference theme tokens (`background.secondary`, `foreground.primary`) — never hardcode colors.

## Step 4: Validate

Run these from the repo root — all must pass:

```bash
yarn --cwd console lint:fix
yarn --cwd console typecheck
```

If the fix touches logic with existing tests, also run:

```bash
yarn --cwd console test <relevant-test-file> --run
```

Fix any failures before proceeding.

## Step 5: Browser verification

### With Playwright MCP (preferred)

If browser MCP tools are available (e.g., `mcp__playwright__browser_navigate`):

1. Ensure the dev server is running:
   ```bash
   # Check if already running
   lsof -i :3000 || yarn --cwd console start &
   ```

2. Navigate to the relevant page:
   ```
   mcp__playwright__browser_navigate(url: "http://localhost:3000/<relevant-path>")
   ```

3. **Handle authentication**: The app may redirect to a login page. If this happens, ask the user to authenticate in their browser first, then retry navigation. Playwright cannot bypass the Frontegg login wall.

4. Take a screenshot of the fixed state:
   ```
   mcp__playwright__browser_take_screenshot(type: "png")
   ```
   Save the returned screenshot file path — it will be embedded in the PR description.

5. Verify the fix visually — check that the reported issue is resolved.

6. If the fix involves light/dark mode, check both themes.

7. Share the screenshot with the user for confirmation before proceeding to commit.

### Without Playwright MCP

Tell the user:

> Browser MCP is not connected. To enable automated verification:
>
> 1. Add Playwright MCP server to `.mcp.json`:
>    ```json
>    {
>      "mcpServers": {
>        "playwright": {
>          "command": "npx",
>          "args": ["@playwright/mcp@latest"]
>        }
>      }
>    }
>    ```
> 2. Install Playwright browsers: `npx playwright install chromium`
> 3. Reconnect via `/mcp` in Claude Code
>
> For now, please verify manually:
> 1. Run `yarn --cwd console start`
> 2. Open http://localhost:3000/<relevant-path>
> 3. Confirm the fix looks correct

Wait for user confirmation before proceeding.

### Playwright artifacts

The Playwright MCP writes screenshots, snapshots, and logs to `.playwright-mcp/`. Ensure this directory is in `.gitignore` so these files are not committed.

## Step 6: Commit and PR

Once the fix is verified, invoke the `/mz-commit` skill to:

1. Create a branch named after the Linear issue (e.g., `qindeelishtiaq/cns-30-fix-description`)
2. Commit with a `console:` prefixed message in imperative mood
3. **Only stage files related to the papercut fix** (typically under `console/`). Do not include unrelated working tree changes (e.g., `.mcp.json`, skill files, `.gitignore`).
4. Open a PR linking to the Linear issue

The PR description should include:
- **Motivation**: Link to the Linear issue
- **What changed**: One sentence describing the fix
- **Verification**: Embed the screenshot from browser verification directly in the PR body using markdown image syntax (`![description](screenshot-url)`). Upload the screenshot file to the PR using `gh` or include it as a GitHub-hosted image. If browser verification was not available, include manual verification steps instead.

## Step 7: Update Linear

After the PR is created, update the Linear issue:

```
mcp__claude_ai_Linear__save_issue(
  id: "<issue-id>",
  state: "In Progress"
)
```

Add a comment with the PR link:

```
mcp__claude_ai_Linear__save_comment(
  issueId: "<issue-id>",
  body: "PR: <github-pr-url>"
)
```
