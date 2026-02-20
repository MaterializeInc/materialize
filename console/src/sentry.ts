// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import * as Sentry from "@sentry/react";
import { useEffect } from "react";
import {
  createRoutesFromChildren,
  matchRoutes,
  Routes,
  useLocation,
  useNavigationType,
} from "react-router-dom";

import { User } from "~/external-library-wrappers/frontegg";

import { appConfig } from "./config/AppConfig";
import { isMzInternalEmail } from "./util";

export type SentryConfig = {
  dsn: Sentry.BrowserOptions["dsn"];
  environment: Sentry.BrowserOptions["environment"];
  release: Sentry.BrowserOptions["release"];
};

// We have to initialize Sentry before calling withSentryReactRouterV6Routing
// which prevents us from putting this in a function.

if (appConfig.mode === "cloud") {
  const { sentryConfig } = appConfig;
  Sentry.init({
    dsn: sentryConfig.dsn,
    enabled: true,
    environment: sentryConfig.environment,
    release: sentryConfig.release,
    beforeSend: (event) => {
      if (sentryConfig !== null) return event;

      // If sentry is disabled, log events to the console
      if (event.exception?.values) {
        for (const e of event.exception.values) {
          console.error(e);
        }
      } else {
        console.debug("Sentry event", event);
      }

      return event;
    },
    beforeSendTransaction: (txEvent) => {
      // If an e2e user, do not log any transactions. We've seen our e2e tests
      // spam Sentry, bleeding quota.
      if (
        isMzInternalEmail(txEvent.user?.email) &&
        txEvent.user?.email?.startsWith("infra+cloud-integration-tests")
      ) {
        return null;
      }
      return txEvent;
    },
    integrations: [
      Sentry.reactRouterV6BrowserTracingIntegration({
        useEffect,
        useLocation,
        useNavigationType,
        createRoutesFromChildren,
        matchRoutes,
      }),
    ],
    normalizeDepth: 8,
    tracesSampler: (samplingContext) => {
      if (samplingContext.attributes?.polled) {
        return 0.01;
      } else {
        return 0.3;
      }
    },
  });
}

export const useSentryIdentifyOrganization = ({ user }: { user: User }) => {
  useEffect(() => {
    Sentry.setUser({
      id: user.id,
      email: user.email,
    });
  }, [user]);
};

/** React router <Routes /> component wrapped with Sentry tracing */
export const SentryRoutes =
  appConfig.mode === "cloud"
    ? Sentry.withSentryReactRouterV6Routing(Routes)
    : Routes;
