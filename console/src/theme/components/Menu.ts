// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { mode, StyleFunctionProps } from "@chakra-ui/theme-tools";

const menuListBase = (props: StyleFunctionProps) => {
  const { colorScheme: c } = props;
  return {
    bg: mode("white", `${c}.700`)(props),
    color: mode(`${c}.600`, `${c}.100`)(props),
    rounded: "md",
    boxShadow: "lg",
    fontSize: "sm",
    py: "1",
  };
};

export const Menu = {
  baseStyle: (props: StyleFunctionProps) => ({
    list: menuListBase(props),
    groupTitle: {
      textTransform: "uppercase",
      fontSize: "0.9em",
      marginInlineStart: "0.8rem",
      marginInlineEnd: "0.8rem",
    },
  }),
  defaultProps: {
    colorScheme: "gray",
  },
};
