// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { ColorModeProvider, ColorModeScript } from "@chakra-ui/react";
import { QueryClientProvider } from "@tanstack/react-query";
import React from "react";
import { BrowserRouter } from "react-router-dom";

import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import { ChakraProviderWrapper } from "~/components/ChakraProviderWrapper";
import { DevtoolsContainer } from "~/components/DevtoolsContainer";
import { FronteggProviderWrapper } from "~/components/FronteggProviderWrapper";
import { IntercomAnonymousContainer } from "~/components/IntercomAnonymousContainer";
import LoadingScreen from "~/components/LoadingScreen";
import { useAppConfig } from "~/config/useAppConfig";
import { JotaiProviderWrapper } from "~/layouts/JotaiProviderWrapper";
import { getQueryClient } from "~/queryClient";
import { config as themeConfig, initialColorMode } from "~/theme";

import { UnauthenticatedRoutes } from "./UnauthenticatedRoutes";

const BrowserRouterWrapper = ({ children }: React.PropsWithChildren) => {
  const appConfig = useAppConfig();

  return (
    <BrowserRouter
      basename={
        appConfig.mode === "cloud" ? appConfig.impersonationBasePath : ""
      }
      future={{
        v7_relativeSplatPath: true,
      }}
    >
      {children}
    </BrowserRouter>
  );
};

export const App = () => {
  return (
    <>
      <ColorModeScript initialColorMode={initialColorMode} />
      <ColorModeProvider options={themeConfig}>
        <JotaiProviderWrapper>
          <BrowserRouterWrapper>
            <ChakraProviderWrapper>
              <AppErrorBoundary containerProps={{ height: "100vh" }}>
                <QueryClientProvider client={getQueryClient()}>
                  <DevtoolsContainer />
                  <IntercomAnonymousContainer />
                  <FronteggProviderWrapper>
                    <AppErrorBoundary containerProps={{ height: "100vh" }}>
                      <React.Suspense fallback={<LoadingScreen />}>
                        <UnauthenticatedRoutes />
                      </React.Suspense>
                    </AppErrorBoundary>
                  </FronteggProviderWrapper>
                </QueryClientProvider>
              </AppErrorBoundary>
            </ChakraProviderWrapper>
          </BrowserRouterWrapper>
        </JotaiProviderWrapper>
      </ColorModeProvider>
    </>
  );
};
