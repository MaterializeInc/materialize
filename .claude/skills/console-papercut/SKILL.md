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
5. If the issue is ambiguous or multiple approaches are viable, ask the user which direction to take before writing code. For clear-cut fixes (typos, missing styles, broken alignment), proceed directly.

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

If Playwright MCP is not available, tell the user to verify manually at `http://localhost:3000/<relevant-path>` and wait for their confirmation before proceeding. Point them to `references/playwright-setup.md` for setup instructions if they want automated verification in the future.

## Step 6: Commit and PR

Once the fix is verified, invoke the `/mz-commit` skill to create the branch, commit, and open a PR. Guide it with:

- **Branch**: `<your-username>/<issue-id>-short-description` (e.g., `jcurrey/cns-30-fix-tooltip-overflow`)
- **Commit message**: use a `console:` prefix in imperative mood
- **Staging**: only stage files related to the papercut fix (typically under `console/`). Do not include unrelated working tree changes (e.g., `.mcp.json`, skill files, `.gitignore`).
- **PR description**: include motivation (link to the Linear issue), what changed (one sentence), and verification (embed the screenshot from Step 5, or manual verification steps if browser verification was not available)

## Step 7: Update Linear

After the PR is created, use the Linear MCP tools to:

1. Update the issue state to "In Progress".
2. Add a comment with the PR link (e.g., `PR: <github-pr-url>`).
