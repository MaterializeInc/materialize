// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  CSSReset,
  EnvironmentProvider,
  GlobalStyle,
  ThemeProvider,
  ToastProvider,
  useColorMode,
} from "@chakra-ui/react";
import React from "react";

import { darkTheme, lightTheme } from "~/theme";

export const ChakraProviderWrapper = ({
  children,
}: {
  children: React.ReactNode;
}) => {
  const mode = useColorMode();
  const theme = mode.colorMode === "dark" ? darkTheme : lightTheme;

  return (
    <ThemeProvider theme={theme}>
      <GlobalStyle />
      <CSSReset />
      <EnvironmentProvider>
        {children}
        <ToastProvider />
      </EnvironmentProvider>
    </ThemeProvider>
  );
};
