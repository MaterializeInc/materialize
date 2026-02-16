// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { act, renderHook, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";

import NetworkPolicyError from "~/api/materialize/NetworkPolicyError";
import { buildCloudRegionsReponse } from "~/api/mocks/cloudGlobalApiHandlers";
import {
  buildEnabledRegionReponse,
  buildNetworkErrorRegionResponse,
  buildPendingRegionResponse,
  buildServerErrorRegionResponse,
} from "~/api/mocks/cloudRegionApiHandlers";
import {
  badRequestHandler,
  successfulHealthCheckResponse,
} from "~/api/mocks/materializeHandlers";
import { getStore } from "~/jotai";
import { createProviderWrapper } from "~/test/utils";

import server from "../api/mocks/server";
import {
  EnabledEnvironment,
  EnvironmentError,
  EnvironmentsWithHealth,
  environmentsWithHealth,
  fetchEnvironmentHealth,
  usePollEnvironmentHealth,
} from "./environments";
import { isFocusedState } from "./focus";

let Wrapper: React.FunctionComponent;

const enabledEnvironment: EnabledEnvironment = {
  errors: [],
  state: "enabled",
  status: { health: "pending", errors: [] },
  httpAddress: "8zpze6ltqnsjok9vvf2i99st5.us-east-1.aws.example.com:443",
  sqlAddress: "8zpze6ltqnsjok9vvf2i99st5.us-east-1.aws.example.com:6875",
  resolvable: true,
  enabledAt: new Date().toISOString(),
};

const enabledStillBootingEnvironment: EnabledEnvironment = {
  ...enabledEnvironment,
  resolvable: false,
};

describe("store/environments", () => {
  beforeEach(() => {
    server.use(buildCloudRegionsReponse());
  });

  describe("fetchEnvironmentHealth", () => {
    it("return healthy for a successful response", async () => {
      server.use(successfulHealthCheckResponse);
      const result = await fetchEnvironmentHealth(enabledEnvironment);
      expect(result.health).toEqual("healthy");
    });

    it("should return crashed when there is an error", async () => {
      server.use(badRequestHandler);
      const result = await fetchEnvironmentHealth(enabledEnvironment);
      expect(result.health).toEqual("crashed");
      const { errors } = result as { errors?: EnvironmentError[] };
      expect(errors).toEqual([
        {
          message: "Environmentd health check failed",
        },
        {
          message: "bad request",
        },
      ]);
    });

    it("should return crashed when max boot time has elapsed and a network error occurs", async () => {
      server.use(
        http.post("*/api/sql", () => {
          return HttpResponse.error();
        }),
      );
      const result = await fetchEnvironmentHealth(
        enabledEnvironment,
        10_000,
        { seconds: 0.001 }, // 1ms max boot time (already exceeded)
      );
      expect(result.health).toEqual("crashed");
      const { errors } = result as { errors?: EnvironmentError[] };
      expect(errors).toEqual([
        {
          message:
            "Environment not resolvable for more than 0.001 seconds after creation",
          details: expect.any(Error),
        },
      ]);
    });

    it("should return crashed when not resolvable for longer than max boot time", async () => {
      const result = await fetchEnvironmentHealth(
        enabledStillBootingEnvironment,
        10_000, // 10 second timeout
        { seconds: 0.001 }, // 1ms max boot time
      );
      expect(result.health).toEqual("crashed");
      const { errors } = result as { errors?: EnvironmentError[] };
      expect(errors).toEqual([
        {
          message:
            "Environment not resolvable for more than 0.001 seconds after creation",
          details: expect.objectContaining({
            name: "Error",
            message: "Environment unresolvable",
          }),
        },
      ]);
    });

    it("should return crashed when not responsive for longer than max boot time", async () => {
      server.use(
        http.post("*/api/sql", () => {
          return HttpResponse.error();
        }),
      );
      const result = await fetchEnvironmentHealth(
        enabledEnvironment,
        10_000,
        { seconds: 0.001 }, // 1ms max boot time (already exceeded)
      );
      expect(result.health).toEqual("crashed");
      const { errors } = result as { errors?: EnvironmentError[] };
      expect(errors).toEqual([
        {
          message: expect.stringMatching(
            "Environment not resolvable for more than 0.001 seconds after creation",
          ),
          details: expect.any(Error),
        },
      ]);
    });

    it("should return booting if the environment is not resolvable, and we have not reached max boot time", async () => {
      const result = await fetchEnvironmentHealth(
        enabledStillBootingEnvironment,
      );
      expect(result.health).toEqual("booting");
    });

    it("should return blocked if the environment returns an organization blocked response", async () => {
      server.use(
        http.post("*/api/sql", () => {
          return new Promise<HttpResponse>((resolve) => {
            return resolve(
              HttpResponse.json(
                {
                  message: "login blocked",
                  code: "MZ010",
                  detail:
                    "Your organization has been blocked. Please contact support.",
                },
                { status: 403 },
              ),
            );
          });
        }),
      );
      const result = await fetchEnvironmentHealth(enabledEnvironment);
      expect(result.health).toEqual("blocked");
      const { errors } = result as { errors?: EnvironmentError[] };
      expect(errors).toEqual([]);
    });

    it("should return blocked if the environment returns a network policy blocked response", async () => {
      server.use(
        http.post("*/api/sql", () => {
          return new Promise<HttpResponse>((resolve) => {
            return resolve(
              HttpResponse.json(
                {
                  message: "session denied",
                  code: "MZ011",
                  detail: "Access denied for address 127.0.0.1",
                },
                { status: 403 },
              ),
            );
          });
        }),
      );
      const result = await fetchEnvironmentHealth(enabledEnvironment);
      expect(result.health).toEqual("blocked");
      const { errors } = result as { errors?: EnvironmentError[] };
      expect(errors).toHaveLength(1);
      const error = errors![0].details;
      expect(error).toBeInstanceOf(NetworkPolicyError);
    });
  });

  describe("usePollEnvironmentHealth", () => {
    beforeEach(async () => {
      Wrapper = await createProviderWrapper();
      const store = getStore();
      store.set(environmentsWithHealth, undefined);
    });

    it("correctly reports healthy environments", async () => {
      server.use(buildEnabledRegionReponse(), successfulHealthCheckResponse);
      const store = getStore();
      renderHook(() => usePollEnvironmentHealth({ intervalMs: 5000 }), {
        wrapper: Wrapper,
      });
      // wait for health check to run
      let environments: EnvironmentsWithHealth | undefined;
      await waitFor(
        () => {
          environments = store.get(environmentsWithHealth);
          expect(environments).toBeTruthy();
        },
        { timeout: 5_000 },
      );
      const usEast = environments?.get("aws/us-east-1");
      const euWest = environments?.get("aws/eu-west-1");
      expect(usEast).toEqual(
        expect.objectContaining({
          state: "enabled",
          resolvable: true,
          status: expect.objectContaining({
            health: "healthy",
          }),
        }),
      );
      expect(euWest).toEqual(
        expect.objectContaining({
          state: "enabled",
          resolvable: true,
          status: expect.objectContaining({
            health: "healthy",
          }),
        }),
      );
    });

    it("correctly reports environments that are pending enablement as creating", async () => {
      server.use(buildPendingRegionResponse("enablement-pending"));
      const store = getStore();
      renderHook(() => usePollEnvironmentHealth({ intervalMs: 5000 }), {
        wrapper: Wrapper,
      });
      // wait for health check to run
      let environments: EnvironmentsWithHealth | undefined;
      await waitFor(() => {
        environments = store.get(environmentsWithHealth);
        expect(environments).toBeTruthy();
      });
      const usEast = environments?.get("aws/us-east-1");
      const euWest = environments?.get("aws/eu-west-1");
      expect(usEast).toEqual({
        state: "creating",
        errors: [],
      });
      expect(euWest).toEqual({
        state: "creating",
        errors: [],
      });
    });

    it("correctly reports environments that are pending deletion as disabled", async () => {
      server.use(buildPendingRegionResponse("deletion-pending"));
      const store = getStore();
      renderHook(() => usePollEnvironmentHealth({ intervalMs: 5000 }), {
        wrapper: await createProviderWrapper({ store }),
      });
      // wait for health check to run
      let environments: EnvironmentsWithHealth | undefined;
      await waitFor(() => {
        environments = store.get(environmentsWithHealth);
        expect(environments).toBeTruthy();
      });
      const usEast = environments?.get("aws/us-east-1");
      const euWest = environments?.get("aws/eu-west-1");
      expect(usEast).toEqual({
        state: "disabled",
        errors: [],
      });
      expect(euWest).toEqual({
        state: "disabled",
        errors: [],
      });
    });

    it("correctly reports soft deleted environments as disabled", async () => {
      server.use(buildPendingRegionResponse("soft-deleted"));
      const store = getStore();
      renderHook(() => usePollEnvironmentHealth({ intervalMs: 5000 }), {
        wrapper: Wrapper,
      });
      // wait for health check to run
      let environments: EnvironmentsWithHealth | undefined;
      await waitFor(() => {
        environments = store.get(environmentsWithHealth);
        expect(environments).toBeTruthy();
      });
      const usEast = environments?.get("aws/us-east-1");
      const euWest = environments?.get("aws/eu-west-1");
      expect(usEast).toEqual({
        state: "disabled",
        errors: [],
      });
      expect(euWest).toEqual({
        state: "disabled",
        errors: [],
      });
    });

    it("correctly reports disabled environments", async () => {
      const store = getStore();
      renderHook(() => usePollEnvironmentHealth({ intervalMs: 5000 }), {
        wrapper: Wrapper,
      });
      // wait for health check to run
      let environments: EnvironmentsWithHealth | undefined;
      await waitFor(() => {
        environments = store.get(environmentsWithHealth);
        expect(environments).toBeTruthy();
      });
      const usEast = environments?.get("aws/us-east-1");
      const euWest = environments?.get("aws/eu-west-1");
      expect(usEast).toEqual({
        state: "disabled",
        errors: [],
      });
      expect(euWest).toEqual({
        state: "disabled",
        errors: [],
      });
    });

    it("correctly reports region errors", async () => {
      server.use(buildServerErrorRegionResponse());
      const store = getStore();
      renderHook(() => usePollEnvironmentHealth({ intervalMs: 5000 }), {
        wrapper: await createProviderWrapper({ store }),
      });
      // wait for health check to run
      let environments: EnvironmentsWithHealth | undefined;
      await waitFor(() => {
        environments = store.get(environmentsWithHealth);
        expect(environments).toBeTruthy();
      });
      const usEast = environments?.get("aws/us-east-1");
      const euWest = environments?.get("aws/eu-west-1");
      expect(usEast).toEqual({
        state: "unknown",
        errors: [expect.objectContaining({ message: "Get region failed" })],
      });
      expect(euWest).toEqual({
        state: "unknown",
        errors: [expect.objectContaining({ message: "Get region failed" })],
      });
    });

    it("correctly reports crashed environments", async () => {
      server.use(
        buildEnabledRegionReponse(),
        // simulates a environment that's in a bad state
        http.post("*/api/sql", () => {
          return new HttpResponse(null, { status: 500 });
        }),
      );
      const store = getStore();
      renderHook(() => usePollEnvironmentHealth({ intervalMs: 5000 }), {
        wrapper: await createProviderWrapper({ store }),
      });
      // wait for health check to run
      await act(() => new Promise((resolve) => setTimeout(resolve, 1)));
      let environments: EnvironmentsWithHealth | undefined;
      await waitFor(() => {
        environments = store.get(environmentsWithHealth);
        expect(environments).toBeTruthy();
      });
      const usEast = environments?.get("aws/us-east-1");
      const euWest = environments?.get("aws/eu-west-1");
      expect(usEast).toEqual(
        expect.objectContaining({
          state: "enabled",
          errors: [],
          status: expect.objectContaining({
            health: "crashed",
            errors: [
              expect.objectContaining({
                message: "Environmentd health check failed",
              }),
              expect.objectContaining({
                message: "Error running query.",
              }),
            ],
          }),
        }),
      );
      expect(euWest).toEqual(
        expect.objectContaining({
          state: "enabled",
          errors: [],
          status: expect.objectContaining({
            health: "crashed",
            errors: [
              expect.objectContaining({
                message: "Environmentd health check failed",
              }),
              expect.objectContaining({
                message: "Error running query.",
              }),
            ],
          }),
        }),
      );
    });

    it("failed region api requests don't disable the environment", async () => {
      server.use(buildEnabledRegionReponse(), successfulHealthCheckResponse);
      const store = getStore();
      renderHook(() => usePollEnvironmentHealth({ intervalMs: 100 }), {
        wrapper: await createProviderWrapper({ store }),
      });
      // set focused, otherwise we won't poll
      act(() => store.set(isFocusedState, true));
      let environments: EnvironmentsWithHealth | undefined;
      // wait for initial health check to run
      await waitFor(() => {
        environments = store.get(environmentsWithHealth);
        expect(environments).toBeTruthy();
      });
      server.use(buildNetworkErrorRegionResponse());
      // wait for polling
      await waitFor(() => {
        environments = store.get(environmentsWithHealth);
        const usEast = environments?.get("aws/us-east-1");
        expect(usEast).toEqual(
          expect.objectContaining({
            state: "enabled",
            errors: [
              expect.objectContaining({
                message: "Get region failed",
              }),
            ],
            status: expect.objectContaining({
              health: "healthy",
              errors: [],
            }),
          }),
        );

        const euWest = environments?.get("aws/eu-west-1");
        expect(euWest).toEqual(
          expect.objectContaining({
            state: "enabled",
            errors: [
              expect.objectContaining({
                message: "Get region failed",
              }),
            ],
            status: expect.objectContaining({
              health: "healthy",
              errors: [],
            }),
          }),
        );
      });
    });
  });
});
