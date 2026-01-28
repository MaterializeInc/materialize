# Dataflow Visualizer E2E Tests

End-to-end browser tests for the dataflow visualizer React components using Playwright.

## Overview

These tests verify that the `/memory` and `/hierarchical-memory` endpoints on port 6876 work correctly. The visualizer displays dataflow graphs using Graphviz WASM and queries `mz_introspection.*` tables.

## Running Tests

### Via mzcompose (recommended)

```bash
cd test/dataflow-visualizer
./mzcompose run default
```

This will:
1. Start a Materialized instance
2. Launch a Playwright Docker container
3. Install npm dependencies
4. Run all Playwright tests
5. Report pass/fail status

### Manual Testing (for development)

Start Materialized separately, then run tests locally:

```bash
# Terminal 1: Start Materialized
./mzcompose up -d materialized

# Terminal 2: Run tests
cd test/dataflow-visualizer
npm install
npm test

# Cleanup
./mzcompose down
```

### Running with headed browser (debugging)

```bash
npm run test:headed
```

## Test Files

- `tests/memory.spec.ts` - Tests for the `/memory` page
  - Page loads without console errors
  - Cluster replica dropdown is populated
  - Dataflow table renders with correct headers
  - Clicking expand button shows visualization
  - Graphviz renders valid SVG
  - System catalog checkbox functionality

- `tests/hierarchical-memory.spec.ts` - Tests for the `/hierarchical-memory` page
  - Page loads without console errors
  - Cluster replica dropdown is populated
  - Dataflows render after loading
  - Collapsible sections expand/collapse correctly
  - SVG visualization is rendered
  - URL updates with cluster parameters

## Configuration

The Playwright configuration (`playwright.config.ts`) is set up to:
- Run tests serially (since they share a single Materialized instance)
- Use `http://materialized:6876` in Docker or `http://localhost:6876` locally
- Capture traces on first retry for debugging
- Timeout after 30 seconds per test
