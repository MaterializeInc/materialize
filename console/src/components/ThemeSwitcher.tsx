// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { MenuItem, useColorMode, useColorModeValue } from "@chakra-ui/react";
import React from "react";
import { Link as RouterLink } from "react-router-dom";

const ThemeSwitcher = () => {
  const { toggleColorMode } = useColorMode();
  const text = useColorModeValue("dark", "light");
  const [isMaxDark, setIsMaxDark] = React.useState(false);

  const toggleMaxDark = () => {
    const next = !isMaxDark;
    setIsMaxDark(next);
    document.documentElement.style.filter = next ? "brightness(0.05)" : "";
  };

  return (
    <>
      <MenuItem
        as={RouterLink}
        fontWeight="medium"
        onClick={() => toggleColorMode()}
      >
        Switch to {text} theme
      </MenuItem>
      <MenuItem fontWeight="medium" onClick={toggleMaxDark}>
        {isMaxDark ? "Disable" : "Enable"} maximum dark mode
      </MenuItem>
    </>
  );
};

export default ThemeSwitcher;
