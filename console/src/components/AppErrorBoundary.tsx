// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { StackProps, VStack } from "@chakra-ui/react";
import { ErrorBoundary, ErrorBoundaryProps } from "@sentry/react";
import { useQueryErrorResetBoundary } from "@tanstack/react-query";
import React from "react";

import { AppConfig } from "~/config/AppConfig";
import { useAppConfig } from "~/config/useAppConfig";

import Alert from "./Alert";

/**
 * Detects errors caused by older browsers that do not support the `wasm-unsafe-eval`
 * content security policy.
 */
function isWasmCSPError(error: unknown): boolean {
  if (typeof error !== "object") return false;
  if (error === null) return false;
  if (!("message" in error) || typeof error.message !== "string") return false;

  if (error.message.includes("WebAssembly")) {
    if (error.message.includes("blocked by CSP")) {
      // Firefox
      return true;
    } else if (error.message.includes("Content Security Policy")) {
      // Safari, Chromium
      return true;
    }
  }
  return false;
}

export interface AppErrorBoundaryProps extends ErrorBoundaryProps {
  message?: string;
  renderFallback?: (errorData: {
    error: unknown;
    componentStack: string;
    eventId: string;
    resetError(): void;
  }) => React.ReactNode;
  containerProps?: StackProps;
}

export const AppErrorBoundary = (props: AppErrorBoundaryProps) => {
  const { reset } = useQueryErrorResetBoundary();
  const appConfig = useAppConfig();

  // The Sentry ErrorBoundary incorrectly types `error` as `Error`, even though it could be
  // anything: https://github.com/getsentry/sentry-javascript/issues/11728.
  // We should always cast to `unknown` before trying to do anything with these errors.
  return (
    <ErrorBoundary
      fallback={(fallbackProps) => (
        <VStack
          width="100%"
          height="100%"
          alignItems="center"
          justifyContent="center"
          overflow="auto"
          {...props.containerProps}
        >
          {isWasmCSPError(fallbackProps.error) ? (
            <Alert
              variant="error"
              message="Your browser is not supported. Please consider updating to the latest version of Chrome, Firefox, or Safari."
            />
          ) : props.renderFallback ? (
            props.renderFallback(fallbackProps)
          ) : (
            <>
              <Alert
                variant="error"
                message={buildErrorMessage(
                  props.message ?? "An unexpected error has occurred",
                  fallbackProps.error,
                  fallbackProps.componentStack,
                  appConfig,
                )}
                showButton={true}
                buttonText="Try again"
                buttonProps={{ onClick: () => fallbackProps.resetError() }}
              />
            </>
          )}
        </VStack>
      )}
      onReset={reset}
      {...props}
    />
  );
};

/**
 * For React Testing Library tests, we append the error message and stack trace
 * to the DOM for more information since React Testing Library prints out the DOM
 * on test failure.
 */
function buildErrorMessage(
  message: string,
  error: unknown,
  componentStack: string,
  appConfig: AppConfig,
) {
  if (appConfig.mode === "cloud" && appConfig.currentStack !== "test")
    return message;

  let innerMessage: string | null = null;
  if (typeof error === "object" && error != null) {
    if ("message" in error && typeof error.message === "string") {
      innerMessage = error.message;
    }
  }
  return (
    <>
      <p>Details</p>
      <p>{message}</p>
      {innerMessage && <p>{innerMessage}</p>}
      <pre>{componentStack}</pre>
    </>
  );
}
