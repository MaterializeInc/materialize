// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { forwardRef, Select, SelectProps, useTheme } from "@chakra-ui/react";
import * as React from "react";

import { MaterializeTheme } from "~/theme";

export type SimpleSelectProps = SelectProps;

const SimpleSelect = forwardRef<SimpleSelectProps, "select">((props, ref) => {
  const { colors, shadows } = useTheme<MaterializeTheme>();

  return (
    <Select
      ref={ref}
      size="sm"
      fontSize="14px"
      lineHeight="16px"
      width="auto"
      rounded="8px"
      borderWidth="1px"
      boxShadow="
        0px 1px 3px 0px hsla(0, 0%, 0%, 0.06),
        0px 1px 1px 0px hsla(0, 0%, 0%, 0.04),
        0px 0px 0px 0px hsla(0, 0%, 0%, 0)"
      transition="box-shadow 50ms ease-out"
      sx={{
        _hover: {
          cursor: "pointer",
        },
        _focus: {
          borderColor: colors.accent.brightPurple,
          boxShadow:
            "0px 0px 0px 0px hsla(0, 0%, 0%, 0), 0px 0px 0px 0px hsla(0, 0%, 0%, 0), 0px 0px 0px 2px hsla(257, 100%, 65%, 0.24)", // accent.brightPurple,
        },
        _invalid: {
          borderColor: colors.accent.red,
          boxShadow: shadows.input.error,
        },
      }}
      borderColor={colors.border.secondary}
      {...props}
    />
  );
});

export default SimpleSelect;
