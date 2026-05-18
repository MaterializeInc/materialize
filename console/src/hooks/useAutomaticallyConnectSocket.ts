// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useLatestRef, usePrevious } from "@chakra-ui/react";
import { atom, useAtomValue, useStore } from "jotai";
import React, { useRef } from "react";

import { SubscribeManager } from "~/api/materialize/SubscribeManager";
import { SessionVariables, SqlRequest } from "~/api/materialize/types";
import {
  Connectable,
  ConnectionInfo,
  ReconnectionState,
  WebsocketConnectionManager,
} from "~/api/materialize/WebsocketConnectionManager";
import { currentEnvironmentState } from "~/store/environments";

// Atom for reconnection state - can be shared across components if needed
export const reconnectionStateAtom = atom<ReconnectionState>({
  status: "disconnected",
  attempt: 0,
  maxAttempts: 5,
  nextRetryMs: null,
});

/**
 * Connects the socket on mount once the environment is healthy.
 * Handles automatic reconnection via WebsocketConnectionManager.
 *
 * Responsibilities:
 * - Creates the WebsocketConnectionManager on mount, which handles:
 *   - Listening to environment health changes
 *   - Reconnecting with exponential backoff when environment is healthy
 * - Handles request changes for subscribe queries (separate from reconnection)
 */
export const useAutomaticallyConnectSocket = <T extends object, R>({
  target,
  subscribe,
  request,
  getSessionVariables,
}: {
  target: Connectable;
  subscribe?: SubscribeManager<T, R>;
  request?: SqlRequest;
  getSessionVariables?: (info: ConnectionInfo) => SessionVariables | undefined;
}): {
  reconnectionState: ReconnectionState;
} => {
  const store = useStore();
  const reconnectionState = useAtomValue(reconnectionStateAtom);
  const managerRef = useRef<WebsocketConnectionManager | null>(null);
  const getSessionVariablesRef = useLatestRef(getSessionVariables);
  const debugLabel = subscribe?.debugLabel;

  // Manager lifecycle - create on mount/target change, destroy on cleanup
  React.useEffect(() => {
    if (debugLabel) {
      // eslint-disable-next-line no-console
      console.log(`[uacs:${debugLabel}] effect-mgr: creating WebsocketConnectionManager`);
    }
    managerRef.current = new WebsocketConnectionManager(
      target,
      store,
      reconnectionStateAtom,
      {
        getSessionVariables: (info) => getSessionVariablesRef.current?.(info),
      },
    );

    return () => {
      if (debugLabel) {
        // eslint-disable-next-line no-console
        console.log(`[uacs:${debugLabel}] effect-mgr: destroying WebsocketConnectionManager`);
      }
      managerRef.current?.destroy();
      managerRef.current = null;
    };
  }, [target, store, getSessionVariablesRef, debugLabel]);

  // Handle request changes for subscribe queries
  const currentEnvironment = useAtomValue(currentEnvironmentState);
  const previousRequest = usePrevious(request);

  React.useEffect(() => {
    if (!subscribe || !request) {
      if (debugLabel) {
        // eslint-disable-next-line no-console
        console.log(
          `[uacs:${debugLabel}] effect-req: skip (subscribe=${!!subscribe} request=${!!request})`,
        );
      }
      return;
    }
    if (previousRequest === request) {
      if (debugLabel) {
        // eslint-disable-next-line no-console
        console.log(`[uacs:${debugLabel}] effect-req: skip (request unchanged)`);
      }
      return;
    }
    // On first mount previousRequest is undefined; the manager's constructor
    // already initiates a connect using the request captured at construction
    // time. Re-connecting here would race with that and tear the still-
    // CONNECTING socket down.
    if (previousRequest === undefined) {
      if (debugLabel) {
        // eslint-disable-next-line no-console
        console.log(
          `[uacs:${debugLabel}] effect-req: skip (first mount; manager handles initial connect)`,
        );
      }
      return;
    }
    if (currentEnvironment?.state !== "enabled") {
      if (debugLabel) {
        // eslint-disable-next-line no-console
        console.log(
          `[uacs:${debugLabel}] effect-req: skip (env state=${currentEnvironment?.state})`,
        );
      }
      return;
    }

    if (debugLabel) {
      // eslint-disable-next-line no-console
      console.log(`[uacs:${debugLabel}] effect-req: calling subscribe.connect()`);
    }
    subscribe.connect(
      request,
      currentEnvironment.httpAddress,
      getSessionVariablesRef.current?.({ hasEverConnected: false }),
    );
  }, [
    subscribe,
    request,
    previousRequest,
    currentEnvironment,
    getSessionVariablesRef,
    debugLabel,
  ]);

  return { reconnectionState };
};
