// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { MenuItem } from "@chakra-ui/react";
import React from "react";
import { Link as RouterLink } from "react-router-dom";

import {
  useModernConsoleEnabled,
  useSetModernConsoleEnabled,
} from "~/store/modernConsole";

const ModernConsoleSwitcher = () => {
  const enabled = useModernConsoleEnabled();
  const setEnabled = useSetModernConsoleEnabled();
  const label = enabled
    ? "Switch to classic console"
    : "Switch to modern console";
  return (
    <MenuItem
      as={RouterLink}
      fontWeight="medium"
      onClick={() => {
        setEnabled(!enabled);
        window.location.reload();
      }}
    >
      {label}
    </MenuItem>
  );
};

export default ModernConsoleSwitcher;
