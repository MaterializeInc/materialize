// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file at the root of this repository.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { test, expect, Page } from '@playwright/test';
import {
  FIXTURE_CLUSTER,
  FIXTURE_DATAFLOW,
  FIXTURE_QUOTED_DATAFLOW,
  FIXTURE_QUOTED_VIEW,
  FIXTURE_REPLICA,
  FIXTURE_VIEW,
  fixturePage,
} from './fixture';

/** Open a /memory page and wait for its dataflow table to render. */
async function openMemoryPage(page: Page, url: string) {
  await page.goto(url);
  await expect(page.locator('text=Loading...')).toBeHidden();
}

/**
 * Expand a fixture dataflow by its name in the dataflow table.
 *
 * Addressing the row by name matters: the table is ordered by record count
 * descending, a dataflow that arranges nothing reports NULL rather than zero
 * there, and NULLs sort first. Those are the transient
 * `introspection-subscribe-*` dataflows and the storage `command_sequencer`.
 * Expanding a transient one races its teardown, and the page then reports an
 * unknown dataflow id instead of a graph.
 */
async function expandDataflow(page: Page, dataflow = FIXTURE_DATAFLOW) {
  const row = page
    .locator('table.dataflows tbody tr')
    .filter({ hasText: dataflow });
  await expect(row).toBeVisible();
  await row.locator('button').click();
}

/** The panel one expanded dataflow renders into. */
function vizSection(page: Page) {
  return page.locator('div:has(> h3)').filter({ hasText: 'Name:' });
}

test.describe('/memory page', () => {
  test('page loads without errors', async ({ page }) => {
    const consoleErrors: string[] = [];
    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        consoleErrors.push(msg.text());
      }
    });

    await openMemoryPage(page, '/memory');

    expect(consoleErrors).toHaveLength(0);
  });

  test('cluster replica dropdown is populated', async ({ page }) => {
    // Deliberately not pinned to a replica: this covers the page picking one
    // for itself when the URL names none.
    await openMemoryPage(page, '/memory');

    // Check that the dropdown exists and has options
    const dropdown = page.locator('#cluster_replica');
    await expect(dropdown).toBeVisible();

    const options = dropdown.locator('option');
    const optionCount = await options.count();
    expect(optionCount).toBeGreaterThan(0);
  });

  test('an unpinned page selects a user cluster', async ({ page }) => {
    // Replicas are listed in cluster then replica name order, so a page that
    // cannot identify a user cluster silently settles on the first of them,
    // which is a builtin one. Those dataflows are not what a user opening this
    // page came to look at, and their introspection relations are an order of
    // magnitude more expensive to query.
    await openMemoryPage(page, '/memory');

    await expect(page.locator('#cluster_replica')).toHaveValue(
      JSON.stringify([FIXTURE_CLUSTER, FIXTURE_REPLICA])
    );
    await expect(page).toHaveURL(new RegExp(`cluster_name=${FIXTURE_CLUSTER}`));
  });

  test('dataflow table renders', async ({ page }) => {
    await openMemoryPage(page, fixturePage('/memory'));

    // Check that the table exists with expected headers
    const table = page.locator('table.dataflows');
    await expect(table).toBeVisible();

    // Verify table headers
    await expect(table.locator('th:has-text("dataflow id")')).toBeVisible();
    await expect(table.locator('th:has-text("index name")')).toBeVisible();
    await expect(table.locator('th:has-text("records")')).toBeVisible();

    // The fixture index is what the tests below expand.
    await expect(
      table.locator('tbody tr').filter({ hasText: FIXTURE_DATAFLOW })
    ).toBeVisible();
  });

  test('clicking dataflow expand button shows visualization', async ({
    page,
  }) => {
    await openMemoryPage(page, fixturePage('/memory'));
    await expandDataflow(page);

    const viz = vizSection(page);
    await expect(viz).toBeVisible();
    await expect(viz).toContainText(FIXTURE_DATAFLOW);
  });

  test('expanded dataflow shows the SQL that created it', async ({ page }) => {
    // The page resolves a dataflow back to the view its index was built on and
    // renders that view's SHOW CREATE VIEW. It is the only part of the page
    // that reaches outside `mz_introspection`, so nothing else catches it
    // breaking.
    await openMemoryPage(page, fixturePage('/memory'));
    await expandDataflow(page);

    const viz = vizSection(page);
    await expect(viz).toContainText(`View: materialize.public.${FIXTURE_VIEW}`);
    await expect(viz).toContainText('CREATE VIEW');
  });

  test('SQL quoting escapes every quote, not just the first', async ({
    page,
  }) => {
    // The pages build SQL by interpolating URL parameters and catalog names,
    // and run it as whoever opened the page, so an escaper that stops after
    // the first quote of a name is no better than none.
    await openMemoryPage(page, '/memory');

    const quoted = await page.evaluate(() => {
      const w = window as any;
      return { literal: w.sqlLiteral(`a'b'c`), ident: w.sqlIdent(`a"b"c`) };
    });

    expect(quoted.literal).toBe(`'a''b''c'`);
    expect(quoted.ident).toBe(`"a""b""c"`);
  });

  test('a view name that needs quoting still resolves to its SQL', async ({
    page,
  }) => {
    // The fixture view's name carries both quote kinds. Interpolated raw into
    // SHOW CREATE VIEW, the statement fails to parse and the page quietly
    // drops the SQL panel (console.debug only), so the panel being present is
    // what pins the quoting at its call sites.
    await openMemoryPage(page, fixturePage('/memory'));
    await expandDataflow(page, FIXTURE_QUOTED_DATAFLOW);

    const viz = vizSection(page);
    await expect(viz).toContainText(
      `View: materialize.public.${FIXTURE_QUOTED_VIEW}`
    );
    await expect(viz).toContainText('CREATE VIEW');
  });

  test('graphviz renders SVG when dataflow is expanded', async ({ page }) => {
    await openMemoryPage(page, fixturePage('/memory'));
    await expandDataflow(page);

    // Wait for SVG to be rendered by graphviz
    const svg = page.locator('svg').first();
    await expect(svg).toBeVisible();

    // Verify it's a valid graphviz SVG (has graph elements)
    const graphElement = svg.locator('g.graph, g.node, g.edge').first();
    await expect(graphElement).toBeVisible();
  });

  test('include system catalog checkbox works', async ({ page }) => {
    await openMemoryPage(page, fixturePage('/memory'));

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
