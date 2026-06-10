// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useAtom } from "jotai";
import React from "react";

import { currentEnvironmentState } from "~/store/environments";

import {
  DEFAULT_QUERY_ERROR,
  executeSql,
  ExecuteSqlError,
  NEW_QUERY_ABORT_MESSAGE,
  OnError,
  OnSettled,
  OnSuccess,
  Results,
  SqlRequest,
  TIMEOUT_ABORT_MESSAGE,
} from ".";
import { DEFAULT_REQUEST_TIMEOUT } from "./executeSqlV2";

export type UseSqlApiRequestOptions = {
  /** The timeout in ms for the connection to environmentd. Defaults to 10_000 */
  timeout?: number;
  /** Whether the request will run immediately when the hook is called, or be executed later. Defaults to `true`. */
  lazy?: boolean;
};

/**
 * A React hook that connects SQL API requests to React's lifecycle.
 * It keeps track of the state of the request and exposes a handler to execute a query.
 */
export function useSqlApiRequest(options?: UseSqlApiRequestOptions) {
  const [environment] = useAtom(currentEnvironmentState);
  const [loading, setLoading] = React.useState<boolean>(
    options?.lazy ? false : true,
  );
  const [hasLoadedOnce, setHasLoadedOnce] = React.useState<boolean>(false);
  const requestIdRef = React.useRef(1);
  const controllerRef = React.useRef<AbortController>(new AbortController());
  const requestInFlightRef = React.useRef(false);
  const [results, setResults] = React.useState<Results[] | null>(null);
  const [error, setError] = React.useState<string | null>(null);
  const [databaseError, setDatabaseError] =
    React.useState<ExecuteSqlError | null>(null);
  const { timeout = DEFAULT_REQUEST_TIMEOUT } = options ?? {};
  const runSql = React.useCallback(
    async (
      request?: SqlRequest,
      onSuccess?: OnSuccess,
      onError?: OnError,
      onSettled?: OnSettled,
    ) => {
      if (environment?.state !== "enabled" || !request) {
        return;
      }
      if (requestInFlightRef.current === true) {
        return;
      }
      controllerRef.current = new AbortController();
      const controller = controllerRef.current;
      requestInFlightRef.current = true;
      const timeoutId = setTimeout(
        () => controller.abort(TIMEOUT_ABORT_MESSAGE),
        timeout,
      );
      const signal = controller.signal;
      const requestId = requestIdRef.current;
      try {
        setLoading(true);
        const result = await executeSql(environment.httpAddress, request, {
          signal: controller.signal,
        });
        if (requestIdRef.current > requestId) {
          // a new query has been kicked off, ignore these results
          return;
        }
        if ("errorMessage" in result) {
          onError?.(result.errorMessage);
          onSettled?.(undefined, result.errorMessage);
          setError(result.errorMessage);
          setDatabaseError(result);
        } else {
          onSuccess?.(result.results);
          onSettled?.(result.results);
          setResults(result.results);
          setError(null);
          setDatabaseError(null);
          return result.results;
        }
      } catch (err) {
        if (signal.aborted) {
          if (signal.reason === NEW_QUERY_ABORT_MESSAGE) {
            // Don't propagate an error when a new query aborts a previous query
            return;
          } else if (signal.reason === TIMEOUT_ABORT_MESSAGE) {
            onError?.(TIMEOUT_ABORT_MESSAGE);
            onSettled?.(undefined, TIMEOUT_ABORT_MESSAGE);
            setError(TIMEOUT_ABORT_MESSAGE);
          }
        } else {
          onSettled?.(undefined, DEFAULT_QUERY_ERROR);
          onError?.(DEFAULT_QUERY_ERROR);
          setError(DEFAULT_QUERY_ERROR);
        }
      } finally {
        requestInFlightRef.current = false;
        clearTimeout(timeoutId);
        setLoading(false);
        setHasLoadedOnce(true);
      }
    },
    [environment, timeout],
  );

  const abortRequest = React.useCallback((reason?: unknown) => {
    requestInFlightRef.current = false;
    requestIdRef.current += 1;
    controllerRef.current.abort(reason);
  }, []);

  return {
    data: results,
    databaseError,
    error,
    failedToLoad: Boolean(!results && error),
    isInitiallyLoading: !hasLoadedOnce && loading,
    loading,
    runSql,
    abortRequest,
  };
}

export type SqlApiResponse = ReturnType<typeof useSqlApiRequest>;
