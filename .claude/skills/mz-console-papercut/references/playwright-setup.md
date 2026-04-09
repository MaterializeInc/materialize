# Playwright MCP Setup

To enable automated browser verification for console papercut fixes:

1. Add Playwright MCP server to `.mcp.json`:
   ```json
   {
     "mcpServers": {
       "playwright": {
         "command": "npx",
         "args": ["@playwright/mcp@latest"]
       }
     }
   }
   ```

2. Install Playwright browsers:
   ```bash
   npx playwright install chromium
   ```

3. Reconnect via `/mcp` in Claude Code.

## Artifacts

The Playwright MCP writes screenshots, snapshots, and logs to `.playwright-mcp/`. Ensure this directory is in `.gitignore` so these files are not committed.
