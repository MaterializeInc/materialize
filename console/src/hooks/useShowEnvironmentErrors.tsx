// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useToast } from "@chakra-ui/toast";
import * as Sentry from "@sentry/react";
import { useAtom } from "jotai";
import throttle from "lodash.debounce";
import React, { useMemo } from "react";

import { useFlags } from "~/hooks/useFlags";
import { currentEnvironmentState } from "~/store/environments";

const ENVIRONMENT_ERROR_TOAST_ID = "environment-error";

/**
 * The time to wait before showing the toast after an environment check
 * fails. 20 seconds is a reasonable amount of time for reducing false negatives.
 */
const ENV_ERROR_TOAST_TOLERANCE_MS = 20_000;

export const useShowEnvironmentErrors = () => {
  const flags = useFlags();
  const logDetails = flags["layout-environment-health-details"];
  const [currentEnvironment] = useAtom(currentEnvironmentState);
  const toast = useToast();

  const displayToast = useMemo(
    () =>
      throttle(
        () => {
          if (!toast.isActive(ENVIRONMENT_ERROR_TOAST_ID)) {
            toast({
              id: ENVIRONMENT_ERROR_TOAST_ID,
              position: "bottom-right",
              duration: null, // never dismiss
              title: "Error",
              description: "We're having trouble reaching your environment",
              status: "warning",
              isClosable: true,
            });
          }
        },
        ENV_ERROR_TOAST_TOLERANCE_MS,
        {
          // We set leading to false to not show the toast immediately
          leading: false,
        },
      ),
    [toast],
  );

  // Listens to errors in the environment
  React.useEffect(() => {
    if (!currentEnvironment) return;

    const regionErrors = currentEnvironment.errors;
    const environmentErrors =
      currentEnvironment.state === "enabled"
        ? currentEnvironment.status.errors
        : [];

    const isBlocked =
      currentEnvironment.state === "enabled" &&
      currentEnvironment.status.health === "blocked";

    if (logDetails) {
      for (const error of regionErrors) {
        console.error(error.message, error.details);
      }
      for (const error of environmentErrors) {
        console.error(error.message, error.details);
      }
    }
    if (!isBlocked) {
      if (environmentErrors.length > 0) {
        Sentry.captureException(new Error("Health check failed"), {
          extra: {
            regionErrors,
            environmentErrors,
          },
        });
        displayToast();
      } else {
        displayToast.cancel();
        if (toast.isActive(ENVIRONMENT_ERROR_TOAST_ID)) {
          toast.close(ENVIRONMENT_ERROR_TOAST_ID);
        }
      }
    }
  }, [
    displayToast, // Should not change
    currentEnvironment,
    logDetails,
    toast,
  ]);
};
