// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { chakra, ChakraProps, useColorMode } from "@chakra-ui/react";
import React from "react";

import grayscaleLogo from "~/img/materialize-logo-grayscale.svg";
import whiteLogo from "~/img/materialize-logo-white.svg";
import grayscaleMark from "~/img/materialize-mark-grayscale.svg";
import whiteMark from "~/img/materialize-mark-white.svg";

export const MaterializeLogo = ({
  markOnly = false,
  ...props
}: { markOnly?: boolean } & ChakraProps) => {
  const { colorMode } = useColorMode();

  return (
    <chakra.img
      src={logoImage(colorMode, markOnly)}
      height="6"
      width={markOnly ? "6" : "auto"}
      aria-label="Materialize logo"
      {...props}
    />
  );
};

function logoImage(colorMode: "light" | "dark", markOnly: boolean) {
  if (colorMode === "light") {
    return markOnly ? grayscaleMark : grayscaleLogo;
  } else {
    return markOnly ? whiteMark : whiteLogo;
  }
}
