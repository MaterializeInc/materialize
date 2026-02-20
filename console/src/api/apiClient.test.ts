// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { http, HttpResponse } from "msw";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { CloudAppConfig, SelfManagedAppConfig } from "~/config/AppConfig";
import { MOCK_ACCESS_TOKEN } from "~/external-library-wrappers/__mocks__/frontegg";

import {
  CloudApiClient,
  FLEXIBLE_DEPLOYMENT_USER,
  ImpersonationApiClient,
  type Middleware,
  SelfManagedApiClient,
  withMiddleware,
} from "./apiClient";
import server from "./mocks/server";

describe("withMiddleware", () => {
  let mockFetch: any;

  beforeEach(() => {
    mockFetch = vi.fn().mockResolvedValue(new Response("OK"));
  });

  it("should apply middleware to fetch function", async () => {
    // Middleware to insert header "test-header"
    const testMiddleware: Middleware =
      (next) =>
      async (...args) => {
        const [input, init = {}] = args;
        const headers = new Headers(init.headers);
        headers.set("test-header", "test-value");
        return next(input, { ...init, headers });
      };

    const enhancedFetch = withMiddleware(mockFetch, testMiddleware);
    await enhancedFetch("https://api.example.com");

    expect(mockFetch).toHaveBeenCalledWith(
      "https://api.example.com",
      expect.objectContaining({
        headers: expect.any(Headers),
      }),
    );

    const [, options] = mockFetch.mock.calls[0];
    expect(options.headers.get("test-header")).toBe("test-value");
  });

  it("should handle multiple middleware functions", async () => {
    // Middleware to insert header "header1"
    const middleware1: Middleware =
      (next) =>
      async (...args) => {
        const [input, init = {}] = args;
        const headers = new Headers(init.headers);
        headers.set("header1", "value1");
        return next(input, { ...init, headers });
      };

    // Middleware to insert header "header2"
    const middleware2: Middleware =
      (next) =>
      async (...args) => {
        const [input, init = {}] = args;
        const headers = new Headers(init.headers);
        headers.set("header2", "value2");
        return next(input, { ...init, headers });
      };

    const enhancedFetch = withMiddleware(mockFetch, middleware1, middleware2);
    await enhancedFetch("https://api.example.com");

    const [, options] = mockFetch.mock.calls[0];
    expect(options.headers.get("header1")).toBe("value1");
    expect(options.headers.get("header2")).toBe("value2");
  });
});

const MOCK_CLOUD_APP_CONFIG = {
  mode: "cloud",
  isImpersonating: false,
  cloudGlobalApiUrl: "https://api.cloud.materialize.com",
  fronteggUrl: "https://app.frontegg.com",
  environmentdScheme: "https",
  environmentdWebsocketScheme: "wss",
  impersonation: null,
} as CloudAppConfig;

describe("CloudApiClient", () => {
  it("should initialize with correct properties", () => {
    const cloudApiClient = new CloudApiClient({
      appConfig: MOCK_CLOUD_APP_CONFIG,
    });
    expect(cloudApiClient.type).toBe("cloud");
    expect(cloudApiClient.isImpersonating).toBe(false);
    expect(cloudApiClient.cloudGlobalApiBasePath).toBe(
      "https://api.cloud.materialize.com",
    );
    expect(cloudApiClient.fronteggApiBasePath).toBe("https://app.frontegg.com");
    expect(cloudApiClient.mzHttpUrlScheme).toBe("https");
    expect(cloudApiClient.mzWebsocketUrlScheme).toBe("wss");
  });

  it("should add bearer token to headers of mzApiFetch function", async () => {
    const cloudApiClient = new CloudApiClient({
      appConfig: MOCK_CLOUD_APP_CONFIG,
    });
    const TEST_URL = "https://cloud.client.com";
    let headers: Headers | null = null;
    // Spy on the fetch function using MSW and save the headers to assert on later
    const handler = http.get(TEST_URL, ({ request }) => {
      headers = request.headers;
      return HttpResponse.json({});
    });
    server.use(handler);

    await cloudApiClient.mzApiFetch(TEST_URL);
    expect(headers!.get("Authorization")).toBe(`Bearer ${MOCK_ACCESS_TOKEN}`);
    server.resetHandlers();
  });

  it("should add bearer token to headers of cloudApiFetch function", async () => {
    const cloudApiClient = new CloudApiClient({
      appConfig: MOCK_CLOUD_APP_CONFIG,
    });

    const TEST_URL = "https://cloud.client.com";
    let headers: Headers | null = null;
    // Spy on the fetch function using MSW and save the headers to assert on later
    const handler = http.get(TEST_URL, ({ request }) => {
      headers = request.headers;
      return HttpResponse.json({});
    });

    server.use(handler);

    await cloudApiClient.cloudApiFetch(TEST_URL);
    expect(headers!.get("Authorization")).toBe(`Bearer ${MOCK_ACCESS_TOKEN}`);
    server.resetHandlers();
  });

  it("should return token auth config for websocket", () => {
    const cloudApiClient = new CloudApiClient({
      appConfig: MOCK_CLOUD_APP_CONFIG,
    });
    const authConfig = cloudApiClient.getWsAuthConfig();
    expect(authConfig).toEqual({
      token: MOCK_ACCESS_TOKEN,
    });
  });
});

const MOCK_IMPERSONATION_CONFIG = {
  mode: "cloud",
  isImpersonating: true,
  cloudGlobalApiUrl: "https://api.cloud.materialize.com",
  fronteggUrl: "https://app.frontegg.com",
  environmentdScheme: "https",
  environmentdWebsocketScheme: "wss",
  impersonation: {
    organizationId: "test-org-id",
    provider: "aws",
    region: "us-east-1",
    regionId: "aws/us-east-1",
    environmentId: "env-123",
    environmentdHttpAddress: "test.materialize.com",
  },
} as CloudAppConfig;

describe("ImpersonationApiClient", () => {
  it("should initialize with correct properties", () => {
    const impersonationApiClient = new ImpersonationApiClient({
      appConfig: MOCK_IMPERSONATION_CONFIG,
    });
    expect(impersonationApiClient.type).toBe("cloud");
    expect(impersonationApiClient.isImpersonating).toBe(true);
    expect(impersonationApiClient.cloudGlobalApiBasePath).toBe(
      "https://api.cloud.materialize.com",
    );
    expect(impersonationApiClient.mzHttpUrlScheme).toBe("https");
    expect(impersonationApiClient.mzWebsocketUrlScheme).toBe("wss");
  });

  it("should return null for websocket auth config", () => {
    const impersonationApiClient = new ImpersonationApiClient({
      appConfig: MOCK_IMPERSONATION_CONFIG,
    });
    const authConfig = impersonationApiClient.getWsAuthConfig();
    expect(authConfig).toBeNull();
  });

  it("should apply impersonation middleware to cloudApiFetch", async () => {
    const impersonationApiClient = new ImpersonationApiClient({
      appConfig: MOCK_IMPERSONATION_CONFIG,
    });

    const TEST_URL = "https://impersonation.example.com";
    let headers: Headers | null = null;
    // Spy on the fetch function using MSW and save the headers to assert on later
    const handler = http.get(TEST_URL, ({ request }) => {
      headers = request.headers;
      return HttpResponse.json({});
    });

    server.use(handler);

    await impersonationApiClient.cloudApiFetch(TEST_URL);
    // Expect the "accept" header to be the organization ID initialized from the mock app config.
    expect(headers!.get("accept")).toBe(
      MOCK_IMPERSONATION_CONFIG.impersonation?.organizationId,
    );
    server.resetHandlers();
  });
});

const MOCK_SELF_MANAGED_CONFIG = {
  mode: "self-managed",
  environmentdScheme: "http",
  environmentdWebsocketScheme: "ws",
  consoleUrl: new URL("http://localhost:9400"),
  regionsStub: [
    {
      provider: "local",
      region: "flexible-deployment",
      regionApiUrl: "",
    },
  ],
  environmentdConfig: {
    environmentdHttpAddress: "localhost:6875",
    regionId: "local/flexible-deployment",
  },
  authMode: "None" as const,
} as SelfManagedAppConfig;

describe("SelfManagedApiClient", () => {
  it("should initialize with correct properties", () => {
    const selfManagedApiClient = new SelfManagedApiClient({
      appConfig: MOCK_SELF_MANAGED_CONFIG,
    });
    expect(selfManagedApiClient.type).toBe("self-managed");
    expect(selfManagedApiClient.mzHttpUrlScheme).toBe("http");
    expect(selfManagedApiClient.mzWebsocketUrlScheme).toBe("ws");
  });

  it("should return password auth config for websocket", () => {
    const selfManagedApiClient = new SelfManagedApiClient({
      appConfig: {
        ...MOCK_SELF_MANAGED_CONFIG,
      },
    });
    const authConfig = selfManagedApiClient.getWsAuthConfig();
    expect(authConfig).toEqual(FLEXIBLE_DEPLOYMENT_USER);
  });

  it("should redirect when authMode is Password and 401 is returned", async () => {
    const config = {
      ...MOCK_SELF_MANAGED_CONFIG,
      authMode: "Password" as const,
    };
    const selfManagedApiClient = new SelfManagedApiClient({
      appConfig: config,
    });

    // Set up a mock handler that returns a 401 unauthenticated for an mzApi fetch.
    const MZ_API_URL = "https://www.weewoo.com";
    const mzApiHandler = http.get(MZ_API_URL, () => {
      return new HttpResponse(null, { status: 401 });
    });

    // Set up a mock handler when the logout API is called.
    const LOGOUT_URL = `${selfManagedApiClient.authApiBasePath}/api/logout`;
    const logoutApiHandlerSpy = vi.fn();
    const logoutApiHandler = http.post(LOGOUT_URL, () => {
      logoutApiHandlerSpy();
      return new HttpResponse(null, { status: 200 });
    });

    server.use(mzApiHandler, logoutApiHandler);

    await selfManagedApiClient.mzApiFetch(MZ_API_URL);

    // Assert that the logout API was called after the mzApi fetch returns a 401.
    await vi.waitFor(() => {
      expect(logoutApiHandlerSpy).toHaveBeenCalled();
    });

    server.resetHandlers();
  });
});
