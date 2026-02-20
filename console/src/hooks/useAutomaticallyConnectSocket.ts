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
  sessionVariables,
}: {
  target: Connectable;
  subscribe?: SubscribeManager<T, R>;
  request?: SqlRequest;
  sessionVariables?: SessionVariables;
}): { reconnectionState: ReconnectionState } => {
  const store = useStore();
  const reconnectionState = useAtomValue(reconnectionStateAtom);
  const managerRef = useRef<WebsocketConnectionManager | null>(null);
  const sessionVariablesRef = useLatestRef(sessionVariables);

  // Manager lifecycle - create on mount/target change, destroy on cleanup
  React.useEffect(() => {
    managerRef.current = new WebsocketConnectionManager(
      target,
      store,
      reconnectionStateAtom,
      { sessionVariables: sessionVariablesRef.current },
    );

    return () => {
      managerRef.current?.destroy();
      managerRef.current = null;
    };
  }, [target, store, sessionVariablesRef]);

  // Handle request changes for subscribe queries
  const currentEnvironment = useAtomValue(currentEnvironmentState);
  const previousRequest = usePrevious(request);

  React.useEffect(() => {
    if (!subscribe || !request) return;
    if (previousRequest === request) return;
    if (currentEnvironment?.state !== "enabled") return;

    subscribe.connect(
      request,
      currentEnvironment.httpAddress,
      sessionVariablesRef.current,
    );
  }, [
    subscribe,
    request,
    previousRequest,
    currentEnvironment,
    sessionVariablesRef,
  ]);

  return { reconnectionState };
};
