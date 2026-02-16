// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import assert from "node:assert";

import {
  APIRequestContext,
  expect,
  Page,
  test as base,
} from "@playwright/test";
import CacheableLookup from "cacheable-lookup";
import retry from "p-retry";
import { Client } from "pg";

import { Region } from "~/api/cloudGlobalApi";

import { CONSOLE_ADDR, EMAIL, REGIONS, STATE_NAME, TestContext } from "./util";

const test = base.extend<{
  testContext: TestContext;
}>({
  testContext: async ({ page, request }, use) => {
    const ctx = await TestContext.start(page, request);
    // eslint-disable-next-line react-hooks/rules-of-hooks
    await use(ctx);
  },
});

test.beforeEach(async ({ page }) => {
  // Block any network requests to Intercom's domains
  await page.route(/.*widget\.intercom\.io.*/, (route) => route.abort());
  await page.route(/.*api-iam\.intercom\.io.*/, (route) => route.abort());

  // Intercept SQL API requests to port 6876 and redirect them to port 32016 in staging tests.
  // This is needed because the region API returns the default DB port (6876) but we need to
  // route requests to the kind cluster's load balancer on port 32016.
  await page.route(
    /.*\.lb\.testing\.materialize\.cloud:6876\/api\/sql/,
    (route, request) => {
      const url = new URL(request.url());
      url.port = "32016";
      route.continue({ url: url.toString() });
    },
  );
});

/**
 * Setup state storage
 */
test.afterEach(async ({ page, testContext }) => {
  // Disable all regions to avoid conflicts with other tests
  await testContext.disableAllRegions();
  // Update the refresh token for future tests.
  await page.context().storageState({ path: STATE_NAME });
});

enum DashboardState {
  NoRegions,
  SomeRegionsActive,
  ThisRegionActive,
}

async function reactSelectOption(page: Page, elementId: string, value: string) {
  await page.click(`#${elementId}`);
  await page.waitForSelector(`#${elementId} .custom-option`);
  await page.click(`#${elementId} .custom-option :has-text("${value}")`);
}

const TEST_TIMEOUT = 30 * 60 * 1000;
const ENABLE_REGION_TIMEOUT = 60 * 1000;

for (const region of REGIONS) {
  test(`use region ${region.id}`, async ({ page, request, testContext }) => {
    // Region creation is slow, override the per test timeout
    test.setTimeout(TEST_TIMEOUT);

    const context = testContext;
    const { tenantId } = await context.getCurrentUser();
    const now = new Date().getTime();
    const apiKeyName = `Integration test token ${now}`;
    await context.deleteAllKeysOlderThan(2);

    // Create api key
    await context.goto(`${CONSOLE_ADDR}/access`);
    console.log("Creating app password", apiKeyName);
    await page.getByRole("button", { name: "Create new" }).click();
    await page.getByRole("link", { name: "App Password", exact: true }).click();
    await page.getByRole("dialog", { name: "New app password" }).waitFor();
    await page.getByRole("textbox", { name: "Name" }).fill(apiKeyName);
    await page.getByRole("button", { name: "Create password" }).click();
    await page.getByText(`New password "${apiKeyName}"`).waitFor();
    await page.getByRole("button", { name: "visibility" }).click();
    const passwordField = page.getByLabel("clientId");
    const password = await passwordField.evaluate((e) => e.textContent);
    assert(!!password, "Expected a password to be created");
    const appPasswords = await context.listAllKeys();
    console.log("app passwords now", appPasswords);
    // Go to home page
    await page
      .getByRole("link", {
        name: /materialize logo/i,
      })
      .click();

    // TODO: Re-enable once fixed on cloud side
    // if (context.fronteggAPIEnabled) {
    //   // Validate that blocked accounts cannot spin up environments. We only
    //   // run this on staging for parity with the Console repo. Our API tests
    //   // will ensure there's test coverage on both staging and prod.
    //   await testAccountBlocking(page, context, region);
    // }

    // Wait for the onboarding survey to load then skip it
    await page.getByTestId("onboarding-survey").waitFor();
    await context.goto(`${CONSOLE_ADDR}/environment-not-ready/enable-region`);

    await retry(
      async () => {
        // Activate the region in the onboarding table if we have no regions
        // active, otherwise pick one in the selector and use it.
        const regionState = await Promise.race([
          (async () => {
            await page
              .getByText(
                "Where would you like to run your Materialize environment?",
              )
              .waitFor();
            return DashboardState.NoRegions;
          })(),
          (async () => {
            await reactSelectOption(page, "environment-select", region.id);
            return await Promise.race([
              (async () => {
                await page.waitForSelector("data-test-id=shell");

                return DashboardState.ThisRegionActive;
              })(),
              (async () => {
                await page
                  .getByText("Your ${region.id} region is currently disabled")
                  .waitFor();
                return DashboardState.SomeRegionsActive;
              })(),
            ]);
          })(),
        ]);
        switch (regionState) {
          case DashboardState.NoRegions:
            console.log("No regions yet activated, activating ours.");
            await page
              .getByTestId("region-options")
              .getByLabel(region.id)
              .click();
            await page
              .getByRole("button", { name: "Get started with Materialize" })
              .click();
            break;

          case DashboardState.ThisRegionActive:
            throw new Error(
              `Region ${region.id} already active, which is unexpected`,
            );

          case DashboardState.SomeRegionsActive:
            console.log("Activating yet-inactive region");
            await page.click(`Enable ${region.id}`);
            break;

          default:
            regionState satisfies never;
        }
        console.log(`Enabling ${region.id} tenant ${tenantId}`);

        // Expect to see the pending region state fairly quickly
        // This helps prevent tests from hanging for a long time if we get an error
        await page.getByText("We’re creating your environment").waitFor({
          // Console will retry this 5 times, and the region api has a 60 second timeout
          timeout: ENABLE_REGION_TIMEOUT,
        });
      },
      { retries: 5 },
    );

    // Step through the onboarding guide
    await page
      .getByRole("link", { name: /get to know materialize →/i })
      .click();
    await page
      .getByRole("link", { name: /the materialize ecosystem →/i })
      .click();
    await page
      .getByRole("link", { name: /learn about incremental updates →/i })
      .click();
    await page
      .getByRole("link", { name: /integrate with your data stack →/i })
      .click();
    await page.getByRole("link", { name: /open console →/i }).click({
      // This button will be disabled until the region is ready, which can take some time
      timeout: ENABLE_REGION_TIMEOUT,
    });
    await expect(page.getByTestId("shell")).toBeVisible();

    // Close welcome dialog
    await page.getByTestId("welcome-dialog-close-button").click();

    await testPlatformEnvironment(page, request, password);

    //// Delete api key
    await context.goto(`${CONSOLE_ADDR}/access`);
    await page.click(
      `[aria-label='${apiKeyName}'] [aria-label='Delete app password']`,
    );
    await page.fill("[aria-modal] input", apiKeyName);
    await Promise.all([
      page.waitForSelector("[aria-modal]", { state: "detached" }),
      page.click("[aria-modal] button:text('Delete')"),
    ]);
    await page.waitForSelector(`text=${apiKeyName}`, { state: "detached" });
  });
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
async function testAccountBlocking(
  page: Page,
  context: TestContext,
  region: Region,
) {
  try {
    // Set the blocked state
    console.info("Marking tenant as blocked");
    await context.setFronteggTenantBlockedStatus(true);

    // Validate the alert banner is visible, and the region button is disabled
    await context.goto(CONSOLE_ADDR);
    await page.getByTestId("blocked-status-alert").isVisible();

    try {
      // Attempt to create an environment...
      console.info("Attempting to create an environment");
      await context.apiRequest(`${region.url}/api/region`, {
        method: "PATCH",
        data: {},
      });
      throw new Error("Tenant was not blocked from creating an environment");
    } catch (err: unknown) {
      if (err instanceof Error) {
        // We anticipate getting a 403 error for blocked accounts. If that's not what we got, rethrow the error
        if (!err.message.includes("API Error 403")) {
          throw err;
        }
        console.info("Environment creation successfully blocked");
      } else {
        // Unrecognized error. Rethrow.
        throw err;
      }
    }
  } finally {
    // Clean up after ourselves
    console.info("Unblocking tenant");
    await context.setFronteggTenantBlockedStatus(false);
    // Reload the page so the unblocked state is registered in the UI
    await context.goto(CONSOLE_ADDR);
  }
}

async function testPlatformEnvironment(
  page: Page,
  _request: APIRequestContext,
  password: string,
) {
  // TODO (SangJunBak): Re-enable this test once we fix the flakiness.
  // At the time of writing this, the postgres client isn't able to connect.
  return;
  const client = await connectRegionPostgres(page, password);
  console.log("SELECT 1");
  const result = await client.query("SELECT 1 as success");
  assert.equal(result.rows[0].success, 1);
  return;
}

async function connectRegionPostgres(
  page: Page,
  password: string,
): Promise<Client> {
  await page.getByTestId("connect-menu-button").click();
  await page.getByRole("button", { name: "External tools" }).click();

  const hostAddress = (await page.getByLabel("Host").innerText()).trim();
  const port = (await page.getByLabel("Port").innerText()).trim();
  const database = (await page.getByLabel("Database").innerText()).trim();

  assert(hostAddress && port && database);

  const url = new URL(
    hostAddress.startsWith("http") ? hostAddress : `http://${hostAddress}`,
  );
  const dns = new CacheableLookup({
    maxTtl: 0, // always re-lookup
    errorTtl: 0,
  });

  for (let i = 0; i < 60; i++) {
    try {
      const entry = await dns.lookupAsync(url.hostname);
      console.info("connecting to environmentd");
      console.info(`  USER=${EMAIL}`);
      console.info(`  HOST=${entry.address}`);
      console.info(`  PORT=${port}`);
      console.info(`  DATABASE=${database}`);
      const pgParams = {
        user: EMAIL,
        host: entry.address,
        port: parseInt(port, 10),
        database: database,
        password,
        ssl: { rejectUnauthorized: false },
        // 5 second connection timeout, because Frontegg authentication can be slow.
        connectionTimeoutMillis: 50000,
      };

      const client = new Client(pgParams);
      await client.connect();
      return client;
    } catch (error) {
      console.error(error, { EMAIL, database, password });
      await page.waitForTimeout(1000);
    }
  }

  throw new Error("unable to connect to region");
}
