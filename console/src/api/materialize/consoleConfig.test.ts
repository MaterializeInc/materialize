// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { fetchConsoleConfig, resetConsoleConfig } from "./consoleConfig";

const respondWith = (body: unknown) =>
  new Response(JSON.stringify(body), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  });

describe("fetchConsoleConfig", () => {
  beforeEach(() => {
    resetConsoleConfig();
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("reads one request for the whole page load", async () => {
    const fetchMock = vi.fn(async () =>
      respondWith({
        oidc_issuer: "https://issuer.example.com",
        console_oidc_client_id: "client-id",
        console_oidc_scopes: "openid email",
        console_ory_sdk_url: "https://ory.example.com",
      }),
    );
    vi.stubGlobal("fetch", fetchMock);

    const [first, second] = await Promise.all([
      fetchConsoleConfig(),
      fetchConsoleConfig(),
    ]);

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(first).toEqual(second);
    expect(first.oryUrl).toBe("https://ory.example.com");
  });

  // environmentd serves this endpoint only once it is up, so a console loaded a
  // moment too early must not be stuck on that failure until someone reloads.
  it("does not remember a failure", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response("", { status: 503 }))
      .mockResolvedValue(
        respondWith({ console_ory_sdk_url: "https://ory.example.com" }),
      );
    vi.stubGlobal("fetch", fetchMock);

    await expect(fetchConsoleConfig()).rejects.toThrow(/still be starting up/);

    await expect(fetchConsoleConfig()).resolves.toMatchObject({
      oryUrl: "https://ory.example.com",
    });
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  // A deployment that does not run Ory leaves the parameter unset, and the
  // console has to read that as "plain OIDC" rather than as missing data.
  it("reports an absent parameter as empty rather than undefined", async () => {
    vi.stubGlobal("fetch", async () =>
      respondWith({ console_oidc_client_id: "client-id" }),
    );

    await expect(fetchConsoleConfig()).resolves.toEqual({
      issuer: "",
      oidcClientId: "client-id",
      oidcScopes: "",
      oryUrl: "",
    });
  });
});
