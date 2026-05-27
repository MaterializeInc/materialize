// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { usePrevious } from "@chakra-ui/react";
import { useAtomValue } from "jotai";
import { useEffect } from "react";

import { reconnectionStateAtom } from "~/hooks/useAutomaticallyConnectSocket";
import { useToast } from "~/hooks/useToast";

const SHELL_CONNECTION_TOAST_ID = "shell-connection-status";

/**
 * Reactive toast hook that shows connection status toasts
 * based on ReconnectionState transitions from WebsocketConnectionManager.
 */
export function useShellConnectionToasts() {
  const reconnectionState = useAtomValue(reconnectionStateAtom);
  const toast = useToast({ duration: 10000 });

  const { status } = reconnectionState;
  const prevStatus = usePrevious(status);

  useEffect(() => {
    if (prevStatus === status) return;

    if (
      prevStatus === "connected" &&
      (status === "reconnecting" || status === "disconnected")
    ) {
      const opts = {
        id: SHELL_CONNECTION_TOAST_ID,
        description: "Connection interrupted. Reconnecting...",
        status: "error" as const,
        duration: 5000,
        isClosable: true,
      };
      if (toast.isActive(SHELL_CONNECTION_TOAST_ID)) {
        toast.update(SHELL_CONNECTION_TOAST_ID, opts);
      } else {
        toast(opts);
      }
    }

    if (status === "connected" && prevStatus === "reconnecting") {
      const opts = {
        id: SHELL_CONNECTION_TOAST_ID,
        description: "Reconnected to Materialize.",
        status: "success" as const,
        duration: 2000,
        isClosable: true,
      };
      if (toast.isActive(SHELL_CONNECTION_TOAST_ID)) {
        toast.update(SHELL_CONNECTION_TOAST_ID, opts);
      } else {
        toast(opts);
      }
    }
  }, [prevStatus, status, toast]);
}
