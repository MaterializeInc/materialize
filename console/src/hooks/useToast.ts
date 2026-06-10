// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useToast as useChakraToast, UseToastOptions } from "@chakra-ui/react";
import { useMemo } from "react";

import { ToastComponent } from "~/components/Toast";

const TOAST_DURATION = 2_000;

/**
 * Wraps Chakra's useToast to use a custom render function and expose our own API
 */
export const useToast = (options?: UseToastOptions) => {
  const toast = useChakraToast(
    useMemo(
      () => ({
        position: "bottom-right" as const,
        duration: TOAST_DURATION,
        render: ToastComponent,
        status: "success",
        ...options,
      }),
      [options],
    ),
  );

  return toast;
};
