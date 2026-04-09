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

- **Linear MCP** must be connected (for pulling issue details and updating status). If not configured, tell the user to add a Linear MCP server to their `.mcp.json` or Claude.ai MCP settings and reconnect via `/mcp`.
- **Playwright MCP** (optional) for browser-based verification. If unavailable, skip to manual verification instructions.
- Dev server: `yarn --cwd console start` should be running or startable.

## Step 1: Pull the issue from Linear

Parse `$ARGUMENTS` for a Linear issue identifier (e.g., `CNS-30`).

If no identifier is provided, use the Linear MCP tools to list open issues from papercut projects. Search for issues in the "Console" team with states "backlog", "todo", or "in progress" from either the "Console Papercuts" project or the current monthly project (e.g., "April Papercuts").

Once you have an issue identifier, fetch the full issue details (description, comments, and attachments) using the Linear MCP tools.

## Step 2: Understand the fix

1. Read the issue description and any linked Slack threads or screenshots.
2. Identify which files need to change. Start by searching `console/src/` for relevant components, hooks, or queries.
3. Read the relevant files. **Understand the existing code before making any changes.**
4. Read `console/CLAUDE.md` for code conventions — every change must follow these.

## Step 3: Make the fix

### Reuse existing patterns — do not invent new ones

Follow the conventions in `console/CLAUDE.md` exactly. Before writing any code, search `console/src/` for how similar problems are already solved — reuse existing components, hooks, and utilities rather than creating new ones. If you add a new component or utility, match the structure, naming, and export patterns of its neighbors.

### Minimal diffs only

- Change ONLY what is needed to fix the reported issue.
- No drive-by refactors, extra comments, docstrings, or type annotations on unchanged code.
- No new abstractions or helpers for one-off fixes.
- Match the style of the file you are editing exactly.

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

1. Create a branch named `<your-username>/<issue-id>-short-description` (e.g., `jcurrey/cns-30-fix-tooltip-overflow`)
2. Commit with a `console:` prefixed message in imperative mood
3. **Only stage files related to the papercut fix** (typically under `console/`). Do not include unrelated working tree changes (e.g., `.mcp.json`, skill files, `.gitignore`).
4. Open a PR linking to the Linear issue

The PR description should include:
- **Motivation**: Link to the Linear issue
- **What changed**: One sentence describing the fix
- **Verification**: Embed the screenshot from browser verification directly in the PR body using markdown image syntax (`![description](screenshot-url)`). Upload the screenshot file to the PR using `gh` or include it as a GitHub-hosted image. If browser verification was not available, include manual verification steps instead.

## Step 7: Update Linear

After the PR is created, use the Linear MCP tools to:

1. Update the issue state to "In Progress".
2. Add a comment with the PR link (e.g., `PR: <github-pr-url>`).
