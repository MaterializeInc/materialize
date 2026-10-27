// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file at the root of this repository.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { test, expect, Page } from '@playwright/test';
import { FIXTURE_INDEX, fixturePage } from './fixture';

/**
 * Wait for the page to render its dataflow graphs.
 *
 * Scope graphs live inside collapsed `.content` divs, so assert on presence
 * rather than visibility. The graphs are the page's actual output: the dropdown
 * appearing only means the replica list came back, and asserting on that alone
 * passes while the rest of the page still says `Loading...`.
 */
async function expectGraphs(page: Page) {
  await expect
    .poll(() => page.locator('#content2 svg').count())
    .toBeGreaterThan(0);
}

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
    const dropdown = page.locator('#cluster_replica');
    await expect(dropdown).toBeVisible();

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

    await expectGraphs(page);
    await expect(page.locator('#cluster_replica')).toBeVisible();
    await expect(page.locator('#content2')).not.toContainText('error:');
    expect(pageErrors.map(String)).toEqual([]);
  });

  test('URL updates with cluster parameters after selection', async ({
    page,
  }) => {
    await page.goto('/hierarchical-memory');

    // Wait for dropdown
    const dropdown = page.locator('#cluster_replica');
    await expect(dropdown).toBeVisible();

    // URL should have been updated with cluster params
    await expect(page).toHaveURL(/cluster_name=/);
    await expect(page).toHaveURL(/replica_name=/);
  });

  test('renders graphs for operator names containing double quotes', async ({
    page,
  }) => {
    // An arrangement operator embeds the debug formatting of its key in its
    // name, so the fixture index on a named column is called something like
    // `ArrangeBy[[Column(0, "id")]]`. Those double quotes have to be escaped
    // before they reach a DOT label, otherwise they terminate the label early
    // and GraphViz rejects the whole graph.
    const pageErrors: Error[] = [];
    page.on('pageerror', (error) => pageErrors.push(error));

    await page.goto(fixturePage('/hierarchical-memory'));

    await expectGraphs(page);

    // The fixture's dataflow rendered, and with it the arrangement label,
    // quotes and all.
    const content = page.locator('#content2');
    await expect(content).toContainText(FIXTURE_INDEX);
    await expect(content).toContainText('ArrangeBy');
    await expect(content).not.toContainText('error:');
    expect(pageErrors.map(String)).toEqual([]);
  });

  test('can switch cluster replicas', async ({ page }) => {
    await page.goto('/hierarchical-memory');

    const dropdown = page.locator('#cluster_replica');
    await expect(dropdown).toBeVisible();

    const options = dropdown.locator('option');
    const optionCount = await options.count();

    // Skip if only one replica
    if (optionCount <= 1) {
      test.skip();
      return;
    }

    // Pick an option other than the one the page settled on, so the URL has to
    // change rather than being rewritten to the same value.
    const selected = await dropdown.inputValue();
    const values = await options.evaluateAll((els) =>
      els.map((el) => (el as HTMLOptionElement).value)
    );
    const newValue = values.find((value) => value !== selected)!;
    const [clusterName, replicaName] = JSON.parse(newValue);

    await dropdown.selectOption(newValue);

    await expect(page).toHaveURL(new RegExp(`cluster_name=${clusterName}`));
    await expect(page).toHaveURL(new RegExp(`replica_name=${replicaName}`));
  });
});
