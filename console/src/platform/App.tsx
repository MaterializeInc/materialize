// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import "../platform/shell/aprilFools.css";

import { ColorModeProvider, ColorModeScript } from "@chakra-ui/react";
import { QueryClientProvider } from "@tanstack/react-query";
import React from "react";
import { BrowserRouter } from "react-router-dom";

import markSrc from "~/img/materialize-mark-grayscale.svg";
import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import { ChakraProviderWrapper } from "~/components/ChakraProviderWrapper";
import { DevtoolsContainer } from "~/components/DevtoolsContainer";
import { FronteggProviderWrapper } from "~/components/FronteggProviderWrapper";
import { IntercomAnonymousContainer } from "~/components/IntercomAnonymousContainer";
import LoadingScreen from "~/components/LoadingScreen";
import { OidcProviderWrapper } from "~/components/OidcProviderWrapper";
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
        v7_startTransition: true,
      }}
    >
      {children}
    </BrowserRouter>
  );
};

// April Fools: Konami code → full-screen MZ logo explosion
const KONAMI_SEQUENCE = [
  "ArrowUp", "ArrowUp", "ArrowDown", "ArrowDown",
  "ArrowLeft", "ArrowRight", "ArrowLeft", "ArrowRight",
  "b", "a",
];

export function triggerLogoExplosion() {
  const container = document.createElement("div");
  container.style.cssText =
    "position:fixed;top:0;left:0;width:100%;height:100%;pointer-events:none;z-index:999999;overflow:hidden";
  document.body.appendChild(container);

  // Flash overlay
  const flash = document.createElement("div");
  flash.style.cssText =
    "position:absolute;top:0;left:0;width:100%;height:100%;background:white;opacity:1;transition:opacity 0.5s;";
  container.appendChild(flash);
  requestAnimationFrame(() => {
    flash.style.opacity = "0";
  });

  // Spawn 40 logos flying outward from center
  for (let i = 0; i < 40; i++) {
    const img = document.createElement("img");
    img.src = markSrc;
    const angle = (i / 40) * Math.PI * 2;
    const distance = 300 + Math.random() * 500;
    const size = 30 + Math.random() * 60;
    const duration = 1.5 + Math.random() * 1;
    const dx = Math.cos(angle) * distance;
    const dy = Math.sin(angle) * distance;
    const rotation = Math.random() * 720 - 360;

    img.style.cssText = `
      position:absolute;
      left:50%;top:50%;
      width:${size}px;height:${size}px;
      margin-left:-${size / 2}px;margin-top:-${size / 2}px;
      opacity:1;
      transition:all ${duration}s cubic-bezier(0.34, 1.56, 0.64, 1);
      z-index:999999;
    `;
    container.appendChild(img);
    requestAnimationFrame(() => {
      img.style.transform = `translate(${dx}px, ${dy}px) rotate(${rotation}deg) scale(0.2)`;
      img.style.opacity = "0";
    });
  }

  // Big center logo that scales up then fades
  const bigLogo = document.createElement("img");
  bigLogo.src = markSrc;
  bigLogo.style.cssText = `
    position:absolute;left:50%;top:50%;
    width:200px;height:200px;
    margin-left:-100px;margin-top:-100px;
    transform:scale(0);opacity:1;
    transition:all 0.8s cubic-bezier(0.34, 1.56, 0.64, 1);
    z-index:9999999;
  `;
  container.appendChild(bigLogo);
  requestAnimationFrame(() => {
    bigLogo.style.transform = "scale(1.5) rotate(180deg)";
  });
  setTimeout(() => {
    bigLogo.style.opacity = "0";
    bigLogo.style.transform = "scale(3) rotate(360deg)";
  }, 1200);

  setTimeout(() => container.remove(), 4000);
}

export const App = () => {
  React.useEffect(() => {
    document.body.classList.add("april-fools-active");
    return () => {
      document.body.classList.remove("april-fools-active");
    };
  }, []);

  // April Fools: Konami code listener
  React.useEffect(() => {
    let pos = 0;
    const handler = (e: KeyboardEvent) => {
      if (e.key === KONAMI_SEQUENCE[pos]) {
        pos++;
        if (pos === KONAMI_SEQUENCE.length) {
          pos = 0;
          triggerLogoExplosion();
        }
      } else {
        pos = 0;
      }
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, []);

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
                    <OidcProviderWrapper>
                      <AppErrorBoundary containerProps={{ height: "100vh" }}>
                        <React.Suspense fallback={<LoadingScreen />}>
                          <UnauthenticatedRoutes />
                        </React.Suspense>
                      </AppErrorBoundary>
                    </OidcProviderWrapper>
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
