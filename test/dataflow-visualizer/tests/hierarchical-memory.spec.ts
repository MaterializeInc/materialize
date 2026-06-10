// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file at the root of this repository.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { test, expect } from '@playwright/test';

test.describe('/hierarchical-memory page', () => {
  test('page loads without crashing', async ({ page }) => {
    // Capture any page errors
    const pageErrors: Error[] = [];
    page.on('pageerror', (error) => pageErrors.push(error));

    const response = await page.goto('/hierarchical-memory');

    // Page should return 200 OK
    expect(response?.status()).toBe(200);

    // No uncaught JS exceptions
    expect(pageErrors).toHaveLength(0);
  });

  test('initial cluster replica dropdown renders', async ({ page }) => {
    await page.goto('/hierarchical-memory');

    // The dropdown should appear after initial data loads
    // Use a longer timeout as the page queries the database
    const dropdown = page.locator('#cluster_replica');
    await expect(dropdown).toBeVisible({ timeout: 20000 });

    // Should have at least one option (check count, not visibility since options are inside select)
    const options = dropdown.locator('option');
    const count = await options.count();
    expect(count).toBeGreaterThan(0);
  });

  test('page renders without crashing after data load', async ({ page }) => {
    await page.goto('/hierarchical-memory');

    // Wait for dropdown to appear (indicates initial load complete)
    const dropdown = page.locator('#cluster_replica');
    await expect(dropdown).toBeVisible({ timeout: 20000 });

    // The page queries data and renders - give it time
    // Then verify the page hasn't completely crashed (body still exists)
    await page.waitForTimeout(3000);
    await expect(page.locator('body')).toBeVisible();
  });

  test('URL updates with cluster parameters after selection', async ({ page }) => {
    await page.goto('/hierarchical-memory');

    // Wait for dropdown
    const dropdown = page.locator('#cluster_replica');
    await expect(dropdown).toBeVisible({ timeout: 20000 });

    // URL should have been updated with cluster params
    await expect(page).toHaveURL(/cluster_name=/);
    await expect(page).toHaveURL(/replica_name=/);
  });

  test('can switch cluster replicas', async ({ page }) => {
    await page.goto('/hierarchical-memory');

    const dropdown = page.locator('#cluster_replica');
    await expect(dropdown).toBeVisible({ timeout: 20000 });

    const options = dropdown.locator('option');
    const optionCount = await options.count();

    // Skip if only one replica
    if (optionCount <= 1) {
      test.skip();
      return;
    }

    // Get initial URL
    const initialUrl = page.url();

    // Select a different option
    const secondOption = options.nth(1);
    const newValue = await secondOption.getAttribute('value');
    if (newValue) {
      await dropdown.selectOption(newValue);

      // URL should update
      await page.waitForTimeout(1000);
      expect(page.url()).not.toBe(initialUrl);
    }
  });
});
