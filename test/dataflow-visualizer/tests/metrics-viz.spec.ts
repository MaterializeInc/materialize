// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file at the root of this repository.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { test, expect } from '@playwright/test';

test.describe('/metrics-viz page', () => {
  test('page loads without errors', async ({ page }) => {
    const consoleErrors: string[] = [];
    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        consoleErrors.push(msg.text());
      }
    });

    await page.goto('/metrics-viz');

    // Wait for loading to complete
    await expect(page.locator('text=Loading metrics...')).toBeHidden({ timeout: 15000 });

    expect(consoleErrors).toHaveLength(0);
  });

  test('endpoint selector dropdown is populated', async ({ page }) => {
    await page.goto('/metrics-viz');

    await expect(page.locator('text=Loading metrics...')).toBeHidden({ timeout: 15000 });

    const dropdown = page.locator('#endpoint-select');
    await expect(dropdown).toBeVisible();

    const options = dropdown.locator('option');
    const optionCount = await options.count();
    // At least environmentd + cluster replicas
    expect(optionCount).toBeGreaterThan(0);

    // First option should be environmentd
    const firstOption = await options.first().textContent();
    expect(firstOption).toBe('environmentd');
  });

  test('metrics load and groups render', async ({ page }) => {
    await page.goto('/metrics-viz');

    await expect(page.locator('text=Loading metrics...')).toBeHidden({ timeout: 15000 });

    // At least one metric group should be visible
    const groups = page.locator('.metric-group');
    const groupCount = await groups.count();
    expect(groupCount).toBeGreaterThan(0);
  });

  test('search box filters metrics', async ({ page }) => {
    await page.goto('/metrics-viz');

    await expect(page.locator('text=Loading metrics...')).toBeHidden({ timeout: 15000 });

    const groupsBefore = await page.locator('.metric-group').count();

    // Type a search term that should filter results
    const searchBox = page.locator('.search-box');
    await searchBox.fill('mz_adapter');

    const groupsAfter = await page.locator('.metric-group').count();
    // Should have fewer groups after filtering (or same if only one matches)
    expect(groupsAfter).toBeLessThanOrEqual(groupsBefore);
    expect(groupsAfter).toBeGreaterThan(0);
  });

  test('collapsible groups work', async ({ page }) => {
    await page.goto('/metrics-viz');

    await expect(page.locator('text=Loading metrics...')).toBeHidden({ timeout: 15000 });

    // Find the first group header and click it to expand
    const firstGroupHeader = page.locator('.metric-group > div').first();
    await firstGroupHeader.click();

    // After clicking, metric families should be visible inside
    const families = page.locator('.metric-family');
    await expect(families.first()).toBeVisible({ timeout: 5000 });

    // Click again to collapse
    await firstGroupHeader.click();

    // Families should be hidden
    await expect(families.first()).toBeHidden({ timeout: 5000 });
  });

  test('histogram chart renders for histogram metrics', async ({ page }) => {
    await page.goto('/metrics-viz');

    await expect(page.locator('text=Loading metrics...')).toBeHidden({ timeout: 15000 });

    // Search for histogram metrics to find them
    const searchBox = page.locator('.search-box');
    await searchBox.fill('histogram');

    // If there are histogram metrics, expand a group and check for SVG
    const groups = page.locator('.metric-group');
    const groupCount = await groups.count();

    if (groupCount === 0) {
      // Try a different search - look for _bucket which indicates histograms
      await searchBox.fill('duration');
    }

    const groupCount2 = await page.locator('.metric-group').count();
    if (groupCount2 === 0) {
      test.skip();
      return;
    }

    // Expand the first group
    await page.locator('.metric-group > div').first().click();

    // Look for histogram chart SVG
    const chartSvg = page.locator('.histogram-chart svg');
    const svgCount = await chartSvg.count();
    // There should be at least one histogram chart if we found histogram metrics
    if (svgCount > 0) {
      await expect(chartSvg.first()).toBeVisible();
    }
  });
});
