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

    // Show disconnect toast when transitioning away from "connected".
    // On first render prevStatus is undefined (from usePrevious), so this
    // naturally skips the initial connection.
    if (
      prevStatus === "connected" &&
      (status === "reconnecting" || status === "disconnected")
    ) {
      toast({
        description: "Connection interrupted. Reconnecting...",
        status: "error",
        duration: 5000,
        isClosable: true,
      });
    }

    // Show reconnect toast only after a real reconnection attempt.
    // On first connection prevStatus is "disconnected", not "reconnecting",
    // so this naturally skips the initial connection.
    if (status === "connected" && prevStatus === "reconnecting") {
      toast({
        description: "Reconnected to Materialize.",
        status: "success",
        duration: 2000,
        isClosable: true,
      });
    }
  }, [prevStatus, status, toast]);
}
