// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { SearchIcon } from "@chakra-ui/icons";
import {
  Box,
  IconButton,
  Input,
  InputGroup,
  InputLeftElement,
  InputRightElement,
  useTheme,
} from "@chakra-ui/react";
import debounce from "lodash.debounce";
import React from "react";

import { MaterializeTheme } from "~/theme";

import { TableSearchProps } from "./tableTypes";

const SEARCH_DEBOUNCE_MS = 250;

export const TableSearch = ({
  initialValue = "",
  onValueChange,
  placeholder = "Search",
  debounceMs = SEARCH_DEBOUNCE_MS,
}: TableSearchProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const [value, setValue] = React.useState(initialValue);

  const debouncedOnChange = React.useMemo(
    () => debounce(onValueChange, debounceMs),
    [onValueChange, debounceMs],
  );

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setValue(e.target.value);
    debouncedOnChange(e.target.value);
  };

  const handleClear = () => {
    setValue("");
    debouncedOnChange.cancel();
    onValueChange("");
  };

  return (
    <Box maxW="md">
      <InputGroup>
        <InputLeftElement pointerEvents="none" height="100%">
          <SearchIcon color={colors.foreground.secondary} />
        </InputLeftElement>
        <Input
          value={value}
          onChange={handleChange}
          placeholder={placeholder}
          aria-label={placeholder}
          pl={10}
        />
        {value && (
          <InputRightElement height="100%">
            <IconButton
              aria-label="Clear search"
              variant="ghost"
              size="xs"
              onClick={handleClear}
              sx={{
                fontSize: "sm",
                color: "foreground.secondary",
                _hover: { color: "foreground.primary" },
              }}
            >
              &times;
            </IconButton>
          </InputRightElement>
        )}
      </InputGroup>
    </Box>
  );
};
