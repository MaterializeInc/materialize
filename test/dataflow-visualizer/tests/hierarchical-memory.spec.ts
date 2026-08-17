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
    // A render-time throw unmounts the React tree while leaving the document
    // intact, so assert on the rendered content rather than on <body>.
    const pageErrors: Error[] = [];
    page.on('pageerror', (error) => pageErrors.push(error));

    await page.goto('/hierarchical-memory');

    // Wait for dropdown to appear (indicates initial load complete)
    const dropdown = page.locator('#cluster_replica');
    await expect(dropdown).toBeVisible({ timeout: 20000 });

    // The page queries data and renders - give it time
    await page.waitForTimeout(3000);

    await expect(dropdown).toBeVisible();
    await expect(page.locator('#content2')).not.toContainText('error:');
    expect(pageErrors.map(String)).toEqual([]);
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

  test('renders graphs for operator names containing double quotes', async ({
    page,
    request,
  }) => {
    // An arrangement operator embeds the debug formatting of its key in its
    // name, so an index on a named column is called something like
    // `ArrangeBy[[Column(0, "id")]]`. Those double quotes have to be escaped
    // before they reach a DOT label, otherwise they terminate the label early
    // and GraphViz rejects the whole graph.
    for (const query of [
      'CREATE TABLE IF NOT EXISTS quoted_name_regression (id int, other int)',
      'CREATE INDEX IF NOT EXISTS quoted_name_regression_idx ON quoted_name_regression (id)',
    ]) {
      const response = await request.post('/api/sql', { data: { query } });
      // /api/sql reports SQL failures in the body, not the status code.
      const body = response.ok() ? await response.json() : null;
      const failure = !body
        ? `HTTP ${response.status()}`
        : body.results?.find((result) => result.error)?.error?.message;
      // The endpoint runs as an unprivileged role, so DDL may be refused
      // depending on how the environment is configured. Skip visibly rather
      // than reporting a visualizer bug that isn't one.
      test.skip(!!failure, `could not set up test index: ${failure}`);
    }

    const pageErrors: Error[] = [];
    page.on('pageerror', (error) => pageErrors.push(error));

    await page.goto(
      '/hierarchical-memory?cluster_name=quickstart&replica_name=r1'
    );

    const content = page.locator('#content2');

    // Scope graphs live inside collapsed `.content` divs, so assert on
    // presence rather than visibility.
    await expect
      .poll(() => content.locator('svg').count(), { timeout: 20000 })
      .toBeGreaterThan(0);

    // The arrangement label rendered, quotes and all.
    await expect(content).toContainText('ArrangeBy');
    await expect(content).not.toContainText('error:');
    expect(pageErrors.map(String)).toEqual([]);
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
