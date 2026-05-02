// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import assert from "node:assert";

// We can ignore the no-restricted-imports rule here because we don't want to export these test utilities
// in our wrapper and don't need to mock these in our e2e tests either.
// eslint-disable-next-line no-restricted-imports
import { FronteggAuthenticator, HttpClient } from "@frontegg/client";
import { APIRequestContext, expect, Page } from "@playwright/test";
import retry, { AbortError } from "p-retry";

import { Region } from "~/api/cloudGlobalApi";
import { buildFronteggUrl } from "~/api/frontegg/index";
import { appConfig } from "~/config/AppConfig";

function getEnvVarOrFail(varName: string, errorMessage: string): string {
  const value = process.env[varName];
  if (!value) {
    throw new Error(errorMessage);
  }
  return value;
}

export const CONSOLE_ADDR = `${appConfig.consoleUrl.protocol}//${appConfig.consoleUrl.hostname}${
  appConfig.consoleUrl.port ? ":" + appConfig.consoleUrl.port : ""
}`;

export const IS_LOCAL_STACK =
  appConfig.mode === "cloud" && appConfig.currentStack === "local";

function buildRegions(stack: string): Region[] {
  if (stack === "local") {
    return [
      {
        id: "local/kind",
        cloudProvider: "local",
        name: "kind",
        // When pointing at a local kind cluster, the region-api server lives on port 32001
        url: "http://127.0.0.1:32001",
      },
    ];
  }
  const stackString = stack === "production" ? "" : `.${stack}`;
  return [
    {
      id: "aws/us-east-1",
      cloudProvider: "aws",
      name: "us-east-1",
      url: `https://api.us-east-1.aws${stackString}.cloud.materialize.com`,
    },
    {
      id: "aws/eu-west-1",
      cloudProvider: "aws",
      name: "eu-west-1",
      url: `https://api.eu-west-1.aws${stackString}.cloud.materialize.com`,
    },
  ];
}

// If you change this, also update the value of `config.workers` in `playwright.config.ts`
export const NUM_PLAYWRIGHT_WORKERS = 5;
// currentStack is only null in flexible deployment mode and our e2e tests
// only run against mz cloud deployments.
export const STACK =
  appConfig.mode === "cloud" ? appConfig.currentStack : "local";
export const REGIONS = buildRegions(STACK);
const e2eTenantStack = STACK === "local" ? "staging" : STACK;

export const PASSWORD = getEnvVarOrFail(
  "E2E_TEST_PASSWORD",
  `Please set $E2E_TEST_PASSWORD on the environment; from the cloud repo, use 'pulumi stack output --stack materialize/${e2eTenantStack} --show-secrets console_e2e_test_password' to retrieve the value.`,
);

/**
 * To prevent tests from trampling over each other, we need to ensure they
 * receive a unique Frontegg tenant.
 *
 * We accomplish this by figuring out how many accounts we have to work with,
 * bucketing them based on our Playwright concurrency settings, and then
 * assigning offsets into the collection based on a combination GitHub Actions
 * Run ID and the Playwright worker index.
 */
function getE2EIndex(stack: string): number {
  const totalWorkers = NUM_PLAYWRIGHT_WORKERS ?? 1;
  const totalAccounts = stack === "staging" ? 50 : 5;
  const workerOffset = parseInt(process.env.TEST_PARALLEL_INDEX ?? "0");
  const runId = parseInt(process.env.GITHUB_RUN_ID ?? "0");
  const totalE2EGroups = Math.floor(totalAccounts / totalWorkers);
  const e2eStartOffset = runId % totalE2EGroups;
  return e2eStartOffset + workerOffset;
}

export const EMAIL = `infra+cloud-integration-tests-${e2eTenantStack}-console-${getE2EIndex(
  e2eTenantStack,
)}@materialize.io`;

export const STATE_NAME = `e2e-tests/state-${process.env.TEST_PARALLEL_INDEX}.json`;

export const FRONTEGG_CLIENT_ID = process.env["E2E_FRONTEGG_CLIENT_ID"];

export const FRONTEGG_SECRET_KEY = process.env["E2E_FRONTEGG_SECRET_KEY"];

// No idea why this is so slow sometimes
export const FRONTEGG_LOADING_TIMEOUT = 60_000;

interface FronteggAuthResponse {
  /** Short-lived access token. */
  accessToken: string;
  /** Longer-lived refresh token, usable only once. */
  refreshToken: string;
  /** Time after which the access token has expired. */
  expires: string;
  /** Seconds until expiration */
  expiresIn: number;
}

export type Options = Parameters<APIRequestContext["fetch"]>[1];

/** Manages an end-to-end test against Materialize Console. */
export class TestContext {
  page: Page;
  request: APIRequestContext;
  accessToken: string;
  refreshToken: string;
  refreshDeadline: Date;
  private fronteggClient: HttpClient | undefined = undefined;

  public get fronteggAPIEnabled(): boolean {
    return this.fronteggClient !== undefined;
  }

  constructor(page: Page, request: APIRequestContext) {
    this.page = page;
    this.request = request;
    this.accessToken = "";
    this.refreshToken = "";
    this.refreshDeadline = new Date(0);
    if (FRONTEGG_CLIENT_ID && FRONTEGG_SECRET_KEY) {
      const authenticator = new FronteggAuthenticator();
      authenticator.init(FRONTEGG_CLIENT_ID, FRONTEGG_SECRET_KEY);
      this.fronteggClient = new HttpClient(authenticator, {
        baseURL: "https://api.frontegg.com",
      });
    } else {
      console.info(
        "No Frontegg API credentials found. Not initializing admin API client.",
      );
    }
  }

  /** Start a new test. */
  static async start(page: Page, request: APIRequestContext) {
    const context = new TestContext(page, request);
    console.info("EMAIL=", EMAIL);

    // Provide a clean slate for the test.
    if (context.fronteggAPIEnabled) {
      await context.setFronteggTenantBlockedStatus(false);
    }
    await context.disableAllRegions();

    // Navigate to the home page && wait for that to load.
    await context.goto(CONSOLE_ADDR);
    // We assume the first page is the onboarding survey
    await page.waitForSelector("[data-testid=onboarding-survey]");
    return context;
  }

  async signIn() {
    await this.page.waitForSelector("[data-test-id=input-identifier]", {
      timeout: FRONTEGG_LOADING_TIMEOUT,
    });
    await this.page.fill("[name=identifier]", EMAIL);
    await this.page.press("[name=identifier]", "Enter");
    await this.page.waitForSelector("[name=password]"); // wait for animation
    await this.page.fill("[name=password]", PASSWORD);
    this.page.press("[name=password]", "Enter");
    await this.waitForFronteggToLoad();
    await expect(
      this.page.getByRole("link", { name: "Logo Materialize" }),
    ).toBeVisible();
    await this.page.context().storageState({ path: STATE_NAME });
  }

  async ensureAuthenticated() {
    if (new Date().getTime() < this.refreshDeadline.getTime()) {
      return;
    }

    const authUrl = buildFronteggUrl("/identity/resources/auth/v1/user");
    const response = await retry(
      () =>
        this.request.post(authUrl, {
          data: {
            email: EMAIL,
            password: PASSWORD,
          },
          timeout: 10 * 1000,
        }),
      // Sometimes we get ECONNRESET or requests hang on these frontegg calls
      { retries: 4 },
    );

    const text = await response.text();
    let auth: FronteggAuthResponse;
    try {
      auth = JSON.parse(text);
    } catch (e: unknown) {
      console.error(`Invalid json from ${authUrl}:\n${text}`);
      throw e as SyntaxError;
    }

    this.accessToken = auth.accessToken;
    this.refreshToken = auth.refreshToken;
    // Use the expiresIn instead of expires, since expires is a hard to work
    // with string.
    this.refreshDeadline = new Date();
    this.refreshDeadline.setUTCSeconds(
      this.refreshDeadline.getUTCSeconds() + auth.expiresIn / 2,
    );
  }

  /**
   * Because frontegg is really slow, we explicitly wait to see our page layout, which
   * shows up once the frontegg full page loading state is complete.
   */
  async waitForFronteggToLoad() {
    await this.page.waitForSelector("[data-testid=page-layout]", {
      timeout: FRONTEGG_LOADING_TIMEOUT,
    });
  }

  /**
   * Visits a given url, signs in if necessary.
   * Sometimes frontegg just seems to hang, so we also retry on all failures.
   */
  async goto(url: string, options?: Parameters<Page["goto"]>[1]) {
    return retry(
      async () => {
        await this.page.goto(url, options);
        const result = await Promise.race([
          (async () => {
            await this.page.waitForSelector("[data-test-id=input-identifier]", {
              timeout: FRONTEGG_LOADING_TIMEOUT,
            });
            return "login";
          })(),
          (async () => {
            await this.waitForFronteggToLoad();
            return "success";
          })(),
        ]);
        if (result === "login") {
          await this.signIn();
        }
      },
      // 3 tries total, no backoff
      { retries: 2, minTimeout: 0, factor: 1 },
    );
  }

  /**
   * Make an authenticated Frontegg API request.
   */
  async fronteggRequest(path: string, request?: Partial<Options>) {
    return this.apiRequest(buildFronteggUrl(path), request);
  }

  /**
   * Make an authenticated API request.
   */
  async apiRequest(url: string, request?: Partial<Options>) {
    await this.ensureAuthenticated();
    request = {
      ...request,
      headers: {
        authorization: `Bearer ${this.accessToken}`,
        "content-type": "application/json",
        ...(request || {}).headers,
      },
    };
    return retry<Record<string, any> | null>(
      // Automatically retry network errors
      async () => {
        const response = await this.request.fetch(url, request);

        if (!response.ok()) {
          const responsePayload = await response.text();
          throw new Error(
            `API Error ${response.status()}  ${url}, req: ${
              JSON.stringify(request.data) ?? "No request body"
            }, res: ${JSON.stringify(responsePayload) ?? "No response body"}`,
          );
        }
        return response;
      },
      // No exponential backoff
      { retries: 2, minTimeout: 1000, factor: 1 },
    );
  }

  /** Block or unblock an organization **/
  async setFronteggTenantBlockedStatus(blocked: boolean) {
    if (!this.fronteggClient) {
      throw new Error("No available Frontegg client");
    }
    const { tenantId } = await this.getCurrentUser();
    await this.fronteggClient.post(
      `tenants/resources/tenants/v1/${tenantId}/metadata`,
      {
        metadata: { blocked },
      },
    );
  }

  /** Disable any existing regions. */
  async disableAllRegions() {
    await Promise.all(REGIONS.map((region) => this.disableRegion(region)));
  }

  async disableRegion(region: Region): Promise<unknown> {
    console.log(`Disabling ${region.id}, this may take up to 5min...`);
    return retry(
      async (attempts) => {
        try {
          await this.ensureAuthenticated();
          await this.request.fetch(
            `${region.url}/api/region`,
            // The timeout on the ALB is 60 seconds, so this timeout doesn't matter much
            {
              method: "DELETE",
              params: { hardDelete: "true" },
              timeout: 5 * 60000,
              headers: {
                authorization: `Bearer ${this.accessToken}`,
                "content-type": "application/json",
              },
            },
          );
        } catch (e: unknown) {
          console.error(e);
          if (e instanceof Error) {
            if (e.message.includes("API Error 504")) {
              // If we get a 504, the ALB most likely timed out
              console.log(
                `Retrying disable region for ${region.id}, attempt ${attempts}`,
              );
              return;
            }
            // If we get any other error, we should not retry
            throw new AbortError(e.message);
          }
          throw new AbortError("Unknown error occurred");
        }
      },
      // Because the ALB connection timeout is 60 seconds, we want to retry right away
      { retries: 4, minTimeout: 0, factor: 1 },
    );
  }

  async getCurrentUser(): Promise<{ id: string; tenantId: string }> {
    const response = await this.fronteggRequest(
      `/identity/resources/users/v2/me`,
    );
    assert(response);
    const { id, tenantId } = await response.json();
    return { id, tenantId };
  }

  async listAllKeys() {
    const { id, tenantId } = await this.getCurrentUser();
    const response = await this.fronteggRequest(
      `/identity/resources/users/api-tokens/v1`,
      {
        headers: {
          "frontegg-tenant-id": tenantId,
          "frontegg-user-id": id,
        },
      },
    );
    assert(response);
    return response.json();
  }

  async deleteAllKeysOlderThan(hours: number) {
    const { id, tenantId } = await this.getCurrentUser();
    const userKeys = await this.listAllKeys();
    for (const k of userKeys) {
      const age = new Date().getTime() - Date.parse(k.createdAt);
      if (age < hours * 60 * 60 * 1000) {
        continue;
      }
      try {
        await this.fronteggRequest(
          `/identity/resources/users/api-tokens/v1/${k.clientId}`,
          {
            method: "DELETE",
            headers: {
              "frontegg-tenant-id": tenantId,
              "frontegg-user-id": id,
            },
          },
        );
      } catch (e: unknown) {
        // if the deployment does not exist, it's okay to ignore the error.
        const keyDoesNotExist =
          e instanceof Error && e.message.includes("API Error 404");
        if (!keyDoesNotExist) {
          throw e;
        }
      }
    }
  }
}
