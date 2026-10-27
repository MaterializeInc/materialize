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

    // Wait for async endpoint discovery to populate the dropdown
    // Note: <option> elements inside <select> are not "visible" in Playwright's sense,
    // so we wait for at least one option to exist using a count check.
    const options = dropdown.locator('option');
    await expect(options).not.toHaveCount(0, { timeout: 10000 });

    // First option should be environmentd
    const firstOption = await options.first().textContent();
    expect(firstOption).toBe('environmentd');
  });

  test('metrics load and groups render', async ({ page }) => {
    await page.goto('/metrics-viz');

    await expect(page.locator('text=Loading metrics...')).toBeHidden({ timeout: 15000 });

    // At least one metric group should be visible
    const groups = page.locator('[aria-label="metric-group"]');
    await expect(groups.first()).toBeVisible({ timeout: 10000 });
    const groupCount = await groups.count();
    expect(groupCount).toBeGreaterThan(0);
  });

  test('search box filters metrics', async ({ page }) => {
    await page.goto('/metrics-viz');

    await expect(page.locator('text=Loading metrics...')).toBeHidden({ timeout: 15000 });

    // Wait for groups to render
    await expect(page.locator('[aria-label="metric-group"]').first()).toBeVisible({ timeout: 10000 });
    const groupsBefore = await page.locator('[aria-label="metric-group"]').count();

    // Type a search term that should filter results
    const searchBox = page.locator('[aria-label="search-box"]');
    await searchBox.fill('mz_adapter');

    // Wait a moment for the filter to take effect
    const groupsAfter = await page.locator('[aria-label="metric-group"]').count();
    // Should have fewer groups after filtering (or same if only one matches)
    expect(groupsAfter).toBeLessThanOrEqual(groupsBefore);
    expect(groupsAfter).toBeGreaterThan(0);
  });

  test('collapsible groups work', async ({ page }) => {
    await page.goto('/metrics-viz');

    await expect(page.locator('text=Loading metrics...')).toBeHidden({ timeout: 15000 });

    // Wait for groups to render
    await expect(page.locator('[aria-label="metric-group"]').first()).toBeVisible({ timeout: 10000 });

    // Find the first group header and click it to expand
    const firstGroupHeader = page.locator('[aria-label="metric-group"] > div').first();
    await firstGroupHeader.click();

    // After clicking, metric families should be visible inside
    const families = page.locator('[aria-label="metric-family"]');
    await expect(families.first()).toBeVisible({ timeout: 5000 });

    // Click again to collapse
    await firstGroupHeader.click();

    // Families should be hidden
    await expect(families.first()).toBeHidden({ timeout: 5000 });
  });

  test('histogram chart renders for histogram metrics', async ({ page }) => {
    await page.goto('/metrics-viz');

    await expect(page.locator('text=Loading metrics...')).toBeHidden({ timeout: 15000 });

    // Wait for groups to render first
    await expect(page.locator('[aria-label="metric-group"]').first()).toBeVisible({ timeout: 10000 });

    // Search for histogram metrics to find them
    const searchBox = page.locator('[aria-label="search-box"]');
    await searchBox.fill('histogram');

    // If there are histogram metrics, expand a group and check for SVG
    const groups = page.locator('[aria-label="metric-group"]');
    const groupCount = await groups.count();

    if (groupCount === 0) {
      // Try a different search - look for duration which often has histograms
      await searchBox.fill('duration');
    }

    const groupCount2 = await page.locator('[aria-label="metric-group"]').count();
    if (groupCount2 === 0) {
      test.skip();
      return;
    }

    // Expand the first group
    await page.locator('[aria-label="metric-group"] > div').first().click();

    // Look for histogram chart SVG
    const chartSvg = page.locator('[aria-label="histogram-chart"] svg');
    const svgCount = await chartSvg.count();
    // There should be at least one histogram chart if we found histogram metrics
    if (svgCount > 0) {
      await expect(chartSvg.first()).toBeVisible();
    }
  });

  test('poll button toggles polling state', async ({ page }) => {
    await page.goto('/metrics-viz');

    await expect(page.locator('text=Loading metrics...')).toBeHidden({ timeout: 15000 });

    // Find the Poll button
    const pollButton = page.locator('button', { hasText: 'Poll' });
    await expect(pollButton).toBeVisible();

    // Click to start polling
    await pollButton.click();

    // Button should now say "Stop"
    const stopButton = page.locator('button', { hasText: 'Stop' });
    await expect(stopButton).toBeVisible({ timeout: 5000 });

    // Poll interval selector should be visible
    const intervalSelect = page.locator('select').nth(1); // second select after endpoint
    await expect(intervalSelect).toBeVisible();

    // Click to stop polling
    await stopButton.click();

    // Button should revert to "Poll"
    await expect(pollButton).toBeVisible({ timeout: 5000 });
  });

  test('save button is present', async ({ page }) => {
    await page.goto('/metrics-viz');

    await expect(page.locator('text=Loading metrics...')).toBeHidden({ timeout: 15000 });

    const saveButton = page.locator('button', { hasText: 'Save' });
    await expect(saveButton).toBeVisible();
  });
});
