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

  // Manager lifecycle - create on mount/target change, destroy on cleanup
  React.useEffect(() => {
    managerRef.current = new WebsocketConnectionManager(
      target,
      store,
      reconnectionStateAtom,
      {
        getSessionVariables: (info) => getSessionVariablesRef.current?.(info),
      },
    );

    return () => {
      managerRef.current?.destroy();
      managerRef.current = null;
    };
  }, [target, store, getSessionVariablesRef]);

  const previousRequest = usePrevious(request);

  React.useEffect(() => {
    if (!subscribe || !request) return;
    if (previousRequest === request) return;
    subscribe.setRequest(request);
    // The manager owns the initial connect, so a request appearing on a
    // pristine connection needs no reconnect. If a query already ran on this
    // connection (the request was cleared and set again), reconnect to
    // replace it, since a connection runs at most one subscribe.
    if (previousRequest === undefined && !subscribe.hasActiveQuery()) return;
    managerRef.current?.reconnect();
  }, [subscribe, request, previousRequest]);

  return { reconnectionState };
};
