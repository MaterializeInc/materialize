// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { SearchIcon } from "@chakra-ui/icons";
import { Box, BoxProps, Input, InputProps, useTheme } from "@chakra-ui/react";
import React from "react";

import { MaterializeTheme } from "~/theme";

export interface SearchInputProps extends InputProps {
  containerProps?: BoxProps;
}

const SearchInput = ({ containerProps, ...props }: SearchInputProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Box as="div" position="relative" {...containerProps}>
      <Input pl="32px" minWidth="256px" placeholder="Search" {...props} />
      <Box
        position="absolute"
        left="4px"
        top="0"
        bottom="0"
        display="flex"
        alignItems="center"
        pl="2"
      >
        <SearchIcon color={colors.foreground.secondary} />
      </Box>
    </Box>
  );
};

export default SearchInput;
