# Dataflow Visualizer E2E Tests

End-to-end browser tests for the dataflow visualizer React components using Playwright.

## Overview

These tests verify that the `/memory` and `/hierarchical-memory` endpoints on port 6876 work correctly. The visualizer displays dataflow graphs using Graphviz WASM and queries `mz_introspection.*` tables.
