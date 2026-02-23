// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useCallback, useRef } from "react";

export const useReactSelectForceFocus = () => {
  const selectRef = useRef<{ focus: () => void } | undefined>(null);

  const onMenuOpen = useCallback(() => {
    /**
     *
     * When a dropdown using Chakra's Popover component is open and someone opens
     * this dropdown, if you click out of this dropdown, it doesn't close.
     *
     * This is because the focus is locked in the other dropdown component, causing this dropdown
     * not to focus properly.
     *
     * This call forces this component to be focused, which allows the dropdown to close properly.
     *
     */
    selectRef.current?.focus();
  }, []);

  return {
    ref: selectRef,
    onMenuOpen,
  };
};
