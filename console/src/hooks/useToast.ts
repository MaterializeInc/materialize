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

/** How the app's toasts look and behave unless a call says otherwise. */
export const TOAST_DEFAULTS: UseToastOptions = {
  position: "bottom-right",
  duration: TOAST_DURATION,
  render: ToastComponent,
  status: "success",
};

/**
 * Wraps Chakra's useToast to use a custom render function and expose our own API
 */
export const useToast = (options?: UseToastOptions) => {
  const toast = useChakraToast(
    useMemo(() => ({ ...TOAST_DEFAULTS, ...options }), [options]),
  );

  return toast;
};
