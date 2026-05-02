// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { EnvironmentProvider, ToastProvider } from "@chakra-ui/react";
import { ThemeProvider } from "@chakra-ui/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { createStore, Provider as JotaiProvider, Setter } from "jotai";
import { DelayMode } from "msw";
import React, { ReactElement } from "react";
import {
  BrowserRouter,
  MemoryRouter,
  Route,
  useLocation,
} from "react-router-dom";
import { gte as semverGte, parse as semverParse } from "semver";

import {
  ClusterWithOwnership,
  Replica,
} from "~/api/materialize/cluster/clusterList";
import { getStore } from "~/jotai";
import { getQueryClient } from "~/queryClient";
import { SentryRoutes } from "~/sentry";
import {
  CreatingEnvironment,
  currentEnvironmentState,
  currentRegionIdAtom,
  DisabledEnvironment,
  EnabledEnvironment,
  environmentsWithHealth,
  LoadedEnvironment,
} from "~/store/environments";
import { lightTheme } from "~/theme";
import { assert } from "~/util";
import { parseDbVersion } from "~/version/api";

import { getMaterializeClient } from "./sql/materializeSqlClient";

type Store = ReturnType<typeof createStore>;

/** Number of milliseconds to wait before returning a response for testing loading
 * states.
 */
export const MSW_HANDLER_LOADING_WAIT_TIME: DelayMode = "infinite";

export const disabledEnvironment: DisabledEnvironment = {
  state: "disabled",
  errors: [],
};

export const creatingEnvironment: CreatingEnvironment = {
  state: "creating",
  errors: [],
};

export const healthyEnvironment: EnabledEnvironment = {
  errors: [],
  httpAddress: "8zpze6ltqnsjok9vvf2i99st5.us-east-1.aws.example.com:443",
  sqlAddress: "8zpze6ltqnsjok9vvf2i99st5.us-east-1.aws.example.com:6875",
  resolvable: true,
  enabledAt: "2023-01-10T01:59:37Z",
  state: "enabled",
  status: {
    health: "healthy",
    version: parseDbVersion("v0.99.0 (ea0d129f)"),
    errors: [],
  },
};

/**
 * Queries materialize and checks if the version is greater than or equal to the supplied
 * version.
 *
 * NOTE: Only for use in SQL tests.
 */
export async function mzVersionIsGte(targetVersion: string) {
  const suppliedVersion = semverParse(targetVersion);
  assert(suppliedVersion);
  const client = await getMaterializeClient();
  const {
    rows: [{ mz_version }],
  } = await client.query("select mz_version()");
  const actualVersion = parseDbVersion(mz_version);
  return semverGte(actualVersion.crateVersion, suppliedVersion);
}

export const defaultRegionId = "aws/us-east-1";

export const RenderWithPathname = ({
  children,
}: {
  children: React.ReactNode;
}) => {
  const current = useLocation();
  return (
    <>
      {children}
      <div data-testid="pathname">{current.pathname}</div>
    </>
  );
};

export const setFakeEnvironment = async (
  set: Setter,
  regionId: string,
  environment: LoadedEnvironment,
) => {
  const environments = new Map<string, LoadedEnvironment>([
    [regionId, environment],
  ] as const);
  set(currentRegionIdAtom, regionId);
  set(environmentsWithHealth, environments);
  // This prevents components from suspending when the get the environment, unfortunately
  // it's not clear why this works.
  await getStore().get(currentEnvironmentState);
};

export type InitializeStateFn = (store: Store) => Promise<void> | void;

/**
 * Renders a component with all our app providers
 */
export const renderComponent = async (
  element: ReactElement,
  options: {
    initializeState?: InitializeStateFn;
    initialRouterEntries?: string[];
    queryClient?: QueryClient;
  } = {},
) => {
  const initializeState: InitializeStateFn = ({ set }) =>
    setFakeEnvironment(set, defaultRegionId, healthyEnvironment);
  const ProviderWrapper = await createProviderWrapper({
    initializeState: options.initializeState ?? initializeState,
    router: {
      type: "MEMORY_ROUTER",
      initialRouterEntries: options.initialRouterEntries,
    },
    queryClient: options.queryClient,
  });

  return render(<ProviderWrapper>{element}</ProviderWrapper>);
};

export interface ProviderWrapperProps {
  initializeState?: InitializeStateFn;
  store?: Store;
  router?:
    | {
        type: "BROWSER_ROUTER";
      }
    | {
        type: "MEMORY_ROUTER";
        initialRouterEntries?: string[];
      };

  queryClient?: QueryClient;
}

/**
 * Factory function to create a ProviderWrapper with initial Jotai state and router entries for use with renderHook.
 * By default, it include a react-router BrowserRouter, but can be configured to use
 * MemoryRouter.
 */
export const createProviderWrapper = async ({
  initializeState,
  router,
  store,
  queryClient: queryClientOverride,
}: ProviderWrapperProps = {}) => {
  router = router ?? { type: "BROWSER_ROUTER" };

  let Router = BrowserRouter;
  let routerProps = {};

  if (router.type === "MEMORY_ROUTER") {
    Router = MemoryRouter;
    routerProps = {
      initialEntries: router.initialRouterEntries,
    };
  }

  const queryClient = queryClientOverride ?? getQueryClient();

  store ||= getStore();
  await initializeState?.(store);

  return function ({ children }: React.PropsWithChildren) {
    return (
      <JotaiProvider store={store}>
        <ThemeProvider theme={lightTheme}>
          <EnvironmentProvider>
            <React.Suspense fallback="provider-wrapper-suspense-fallback">
              <QueryClientProvider client={queryClient}>
                <Router {...routerProps}>
                  <SentryRoutes>
                    <Route index path="*" element={children} />
                  </SentryRoutes>
                </Router>
              </QueryClientProvider>
            </React.Suspense>
            <ToastProvider />
          </EnvironmentProvider>
        </ThemeProvider>
      </JotaiProvider>
    );
  };
};

export function buildCluster(
  overrides: Partial<ClusterWithOwnership> = {},
  replicas: Array<Replica> = [],
): ClusterWithOwnership {
  return {
    id: "s1",
    name: "mz_system",
    size: "25cc",
    managed: true,
    disk: true,
    isOwner: true,
    replicas: replicas,
    latestStatusUpdate: "2024-01-01T00:00:00.000Z",
    ...overrides,
  };
}

function buildStringifiedOptions(options: Element[]): string {
  if (options.length === 0) return "<No options>";
  return options.map((el) => `'${el.textContent}'`).join(", ");
}

/**
 * Select a react-select option.
 *
 * @param container - The outermost div of the react-select component.
 * @param optionText - The text of the option to select.
 */
export async function selectReactSelectOption(
  container: HTMLElement,
  optionText: string,
) {
  const input = container.querySelector("input");
  assert(input !== null, "Could not find react-select input");
  await userEvent.click(input);
  const optionEls = Array.from(container.querySelectorAll('[role="option"]'));
  const option = optionEls.find((el) => el.textContent === optionText);
  assert(
    option !== undefined,
    `Could not find react-select option '${optionText}'. Found: ${buildStringifiedOptions(optionEls)}`,
  );
  await userEvent.click(option);
}

/**
 * Determine whether there are invalid form fields in the document.
 */
export function isValidForm() {
  const invalidFields = Array.from(document.querySelectorAll("[data-invalid]"));
  return invalidFields.length === 0;
}
