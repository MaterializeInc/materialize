// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, ColorMode, HStack, Text, useColorMode } from "@chakra-ui/react";
import { useAtom } from "jotai";
import React from "react";
import { useNavigate } from "react-router-dom";
import ReactSelect, {
  MultiValue,
  OptionProps,
  SingleValue as SingleValueType,
  SingleValueProps,
  StylesConfig,
} from "react-select";

import { hasEnvironmentReadPermission } from "~/api/auth";
import { User } from "~/external-library-wrappers/frontegg";
import { regionIdToSlug } from "~/store/cloudRegions";
import {
  currentRegionIdAtom,
  Environment,
  useEnvironmentsWithHealth,
} from "~/store/environments";
import colors from "~/theme/colors";

const environmentSlugRegex = /^\/regions\/([\w-]*)\/?/;

const EnvironmentSelectField = ({ user }: { user: User }) => {
  const colorModeContext = useColorMode();
  const canReadEnvironments = hasEnvironmentReadPermission(user);
  const environments = useEnvironmentsWithHealth();
  const navigate = useNavigate();
  const [currentRegionId, setCurrentRegionId] = useAtom(currentRegionIdAtom);

  const selectHandler = React.useCallback(
    (
      option: SingleValueType<EnvOptionType> | MultiValue<EnvOptionType> | null,
    ) => {
      const regionSlug = regionIdToSlug((option as EnvOptionType).id);
      setCurrentRegionId((option as EnvOptionType).id);
      const matches = environmentSlugRegex.exec(location.pathname);
      if (matches) {
        const newPath = location.pathname.replace(matches[1], `${regionSlug}`);
        navigate(newPath + location.search + location.hash);
      }
    },
    [navigate, setCurrentRegionId],
  );

  const colorStyles = React.useMemo(
    () => getColorStyles(colorModeContext.colorMode),
    [colorModeContext],
  );

  if (
    Array.from(environments.values()).every((e) => e.state === "disabled") ||
    !canReadEnvironments
  ) {
    return null;
  }

  const options = Array.from(environments, ([id, environment]) => ({
    id,
    environment,
  }));

  const currentOption = options.find((o) => o.id === currentRegionId)!;

  return (
    <ReactSelect
      id="environment-select"
      aria-label="Environment"
      name="environment-select"
      components={{ Option: EnvOption, SingleValue }}
      options={options}
      value={currentOption}
      onChange={selectHandler}
      styles={colorStyles}
      isMulti={false}
      isSearchable={false}
    />
  );
};

type EnvOptionType = {
  id: string;
  environment: Environment;
};

type DotProps = {
  environment: Environment;
};

function getDotProperties(environment: Environment): {
  color: string;
  health: string;
} {
  switch (environment.state) {
    case "creating": {
      // This case means the region api has acknowledged the request
      return { color: "yellow.400", health: "creating" };
    }
    case "enabled": {
      switch (environment.status.health) {
        case "pending":
          // This state never shows, we don't render this until the data is loaded
          return { color: "yellow.400", health: "pending" };
        case "booting":
          // This case means the region is running, but not yet resolvable via DNS
          return { color: "yellow.400", health: "booting" };
        case "healthy":
          return { color: "green.500", health: "healthy" };
        case "crashed":
          return { color: "red.400", health: "crashed" };
        case "blocked":
          return { color: "gray.300", health: "blocked" };
      }
    }
    // Because all cases above return, the break would be unreachable
    // eslint-disable-next-line no-fallthrough
    case "disabled": {
      return { color: "gray.300", health: "disabled" };
    }
    case "unknown": {
      // This will only happen if our very fist request to the region api fails
      return { color: "red.400", health: "unknown" };
    }
  }
}
const Dot = ({ environment }: DotProps) => {
  const { color, health } = getDotProperties(environment);
  return (
    <Box
      data-testid={`health-${health}`}
      height="10px"
      width="10px"
      mr={2}
      backgroundColor={color}
      borderRadius="10px"
      flexShrink="0"
    />
  );
};

const SingleValue: React.FunctionComponent<
  React.PropsWithChildren<SingleValueProps<EnvOptionType>>
> = ({ innerProps, data }) => {
  return (
    <HStack {...innerProps} minWidth="0" spacing={0}>
      <Dot environment={data.environment} />
      <Text noOfLines={1}>{data.id}</Text>
    </HStack>
  );
};

const EnvOption: React.FunctionComponent<
  React.PropsWithChildren<OptionProps<EnvOptionType>>
> = ({ innerProps, innerRef, data, ...props }) => {
  const { colorMode } = useColorMode();
  const isDarkMode = colorMode === "dark";
  const textColor = isDarkMode ? "white" : "black";
  const bg = isDarkMode ? "transparent" : "white";
  const selectedBg = isDarkMode ? `#FFFFFF18` : "transparent";
  const hoverBg = isDarkMode ? `#FFFFFF24` : "gray.100";
  const activeBg = isDarkMode ? `#FFFFFF36` : "gray.200";
  return (
    <HStack
      ref={innerRef}
      {...innerProps}
      className="custom-option"
      color={props.isDisabled ? "gray.400" : textColor}
      cursor={props.isDisabled ? "not-allowed" : "pointer"}
      backgroundColor={props.isSelected ? selectedBg : bg}
      _hover={{
        backgroundColor: hoverBg,
      }}
      _active={{
        backgroundColor: activeBg,
      }}
      px="9px"
      py={2}
      spacing={0}
    >
      <Dot environment={data.environment} />
      <Box>{data.id}</Box>
    </HStack>
  );
};

const getColorStyles = (mode: ColorMode): StylesConfig<EnvOptionType> => {
  const isDarkMode = mode === "dark";
  const unfocusedBorderColor = isDarkMode ? colors.gray[600] : colors.gray[300];
  const unfocusedIconHoverColor = isDarkMode
    ? colors.gray[300]
    : colors.gray[500];
  return {
    container: (styles) => ({
      ...styles,
      width: "100%",
    }),
    control: (styles, state) => ({
      ...styles,
      backgroundColor: "transparent",
      width: "100%",
      borderRadius: "8px",
      borderColor: state.isFocused ? colors.purple[400] : unfocusedBorderColor,
    }),
    indicatorSeparator: () => ({}),
    dropdownIndicator: (styles) => ({
      ...styles,
      color: isDarkMode ? colors.gray[300] : colors.gray[500],
      ":active": {
        ...styles[":active"],
      },
      ":hover": {
        ...styles[":hover"],
        color: unfocusedIconHoverColor,
      },
    }),
    menu: (styles) => ({
      ...styles,
      borderRadius: "0.375rem",
      backgroundColor: isDarkMode ? colors.gray[700] : styles.backgroundColor,
    }),
    valueContainer: (styles) => ({
      ...styles,
      display: "flex",
      input: {
        // prevent this visually hidden input from causing layout issues on small viewports
        position: "absolute",
      },
    }),
    input: (styles) => ({
      ...styles,
    }),
  };
};

export default EnvironmentSelectField;
