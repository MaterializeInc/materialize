// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file at the root of this repository.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { test, expect } from '@playwright/test';

test.describe('/memory page', () => {
  test('page loads without errors', async ({ page }) => {
    const consoleErrors: string[] = [];
    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        consoleErrors.push(msg.text());
      }
    });

    await page.goto('/memory');

    // Wait for loading to complete
    await expect(page.locator('text=Loading...')).toBeHidden({ timeout: 10000 });

    // Check for no console errors (excluding expected debug messages)
    const unexpectedErrors = consoleErrors.filter(
      (err) => !err.includes('could not get create view')
    );
    expect(unexpectedErrors).toHaveLength(0);
  });

  test('cluster replica dropdown is populated', async ({ page }) => {
    await page.goto('/memory');

    // Wait for loading to complete
    await expect(page.locator('text=Loading...')).toBeHidden({ timeout: 10000 });

    // Check that the dropdown exists and has options
    const dropdown = page.locator('#cluster_replica');
    await expect(dropdown).toBeVisible();

    const options = dropdown.locator('option');
    const optionCount = await options.count();
    expect(optionCount).toBeGreaterThan(0);
  });

  test('dataflow table renders', async ({ page }) => {
    await page.goto('/memory');

    // Wait for loading to complete
    await expect(page.locator('text=Loading...')).toBeHidden({ timeout: 10000 });

    // Check that the table exists with expected headers
    const table = page.locator('table.dataflows');
    await expect(table).toBeVisible();

    // Verify table headers
    await expect(table.locator('th:has-text("dataflow id")')).toBeVisible();
    await expect(table.locator('th:has-text("index name")')).toBeVisible();
    await expect(table.locator('th:has-text("records")')).toBeVisible();
  });

  test('clicking dataflow expand button shows visualization', async ({ page }) => {
    await page.goto('/memory');

    // Wait for loading to complete
    await expect(page.locator('text=Loading...')).toBeHidden({ timeout: 10000 });

    // Find the first expand button in the table
    const expandButton = page.locator('table.dataflows tbody button').first();

    // Skip test if no dataflows are present
    if ((await expandButton.count()) === 0) {
      test.skip();
      return;
    }

    await expandButton.click();

    // Wait for the visualization section to appear
    const vizSection = page.locator('div:has(> h3)').filter({ hasText: 'Name:' });
    await expect(vizSection).toBeVisible({ timeout: 15000 });
  });

  test('graphviz renders SVG when dataflow is expanded', async ({ page }) => {
    await page.goto('/memory');

    // Wait for loading to complete
    await expect(page.locator('text=Loading...')).toBeHidden({ timeout: 10000 });

    // Find the first expand button
    const expandButton = page.locator('table.dataflows tbody button').first();

    // Skip test if no dataflows are present
    if ((await expandButton.count()) === 0) {
      test.skip();
      return;
    }

    await expandButton.click();

    // Wait for SVG to be rendered by graphviz
    const svg = page.locator('svg').first();
    await expect(svg).toBeVisible({ timeout: 15000 });

    // Verify it's a valid graphviz SVG (has graph elements)
    const graphElement = svg.locator('g.graph, g.node, g.edge').first();
    await expect(graphElement).toBeVisible();
  });

  test('include system catalog checkbox works', async ({ page }) => {
    await page.goto('/memory');

    // Wait for loading to complete
    await expect(page.locator('text=Loading...')).toBeHidden({ timeout: 10000 });

    const checkbox = page.locator('#include_system_catalog');
    await expect(checkbox).toBeVisible();

    // Checkbox should be unchecked by default
    await expect(checkbox).not.toBeChecked();

    // Click to enable
    await checkbox.click();
    await expect(checkbox).toBeChecked();

    // URL should update with system_catalog parameter
    await expect(page).toHaveURL(/system_catalog=true/);
  });
});
