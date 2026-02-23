// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { atom, useAtom, useSetAtom } from "jotai";
import React from "react";

import { isPollingDisabled } from "~/util";

export const useTrackFocus = () => {
  const setValue = useSetAtom(isFocusedState);

  const handleBlur = React.useCallback(() => {
    setValue(false);
  }, [setValue]);
  const handleFocus = React.useCallback(() => {
    setValue(true);
  }, [setValue]);

  React.useEffect(() => {
    window.addEventListener("blur", handleBlur);
    window.addEventListener("focus", handleFocus);
  });
};

export const isFocusedState = atom<boolean>(document.hasFocus());

/**
 * Checks if polling should be disabled because of the noPoll query param or because the document is not currently focused
 */
export const useIsPollingDisabled = () => {
  const [isFocused] = useAtom(isFocusedState);
  return isPollingDisabled() || !isFocused;
};
