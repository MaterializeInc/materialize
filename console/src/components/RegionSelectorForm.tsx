// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Box,
  forwardRef,
  HStack,
  RadioProps,
  Text,
  useRadio,
  useRadioGroup,
  useTheme,
} from "@chakra-ui/react";
import { useAtom } from "jotai";
import React from "react";

import {
  cloudRegionsSelector,
  getStylizedCloudRegionProvider,
} from "~/store/cloudRegions";
import { MaterializeTheme } from "~/theme";

type RegionType = string;

export const RegionSelectorForm = (props: {
  onChange: (value: string) => void;
  options: RegionType[];
}) => {
  const { getRadioProps, getRootProps } = useRadioGroup({
    name: "region-selector",
    onChange: (val) => {
      props.onChange(val);
    },
  });
  const group = getRootProps();

  return (
    <Box
      as="form"
      action="/region"
      method="post"
      width="100%"
      data-testid="region-options"
    >
      <HStack {...group} align="start" gap={6}>
        {props.options.map((option, index) => {
          const radio = getRadioProps({ value: option });
          return (
            <RegionRadioCard
              tabIndex={index}
              value={option}
              key={`${option}-${index}`}
              {...radio}
            />
          );
        })}
      </HStack>
    </Box>
  );
};

export type RegionSelectorProps = RadioProps & {
  value: string;
};

export const RegionRadioCard = forwardRef<RadioProps, "input">(
  ({ ...props }, ref) => {
    const { colors, shadows } = useTheme<MaterializeTheme>();
    const { getInputProps, getRadioProps } = useRadio(props);
    const [cloudRegions] = useAtom(cloudRegionsSelector);

    const input = getInputProps();
    const checkbox = getRadioProps();

    if (!props.value) return null;
    const cloudRegion = cloudRegions.get(props.value);

    if (!cloudRegion) return null;

    return (
      <Box as="label" width="100%" aria-label={props.value}>
        <input ref={ref} {...input} />
        <Box
          {...checkbox}
          bg={colors.background.secondary}
          cursor="pointer"
          borderWidth="1px"
          borderRadius="md"
          borderColor={colors.border.secondary}
          _checked={{
            bg: colors.background.primary,
            borderColor: colors.accent.brightPurple,
            boxShadow: shadows.input.focus,
          }}
          _focus={{
            borderColor: colors.accent.brightPurple,
            boxShadow: shadows.input.focus,
          }}
          py={4}
          px={6}
        >
          <Text
            textStyle="text-base"
            fontWeight="500"
            color={colors.foreground.secondary}
          >
            {getStylizedCloudRegionProvider(cloudRegion)}
          </Text>
          <Text textStyle="heading-sm">{cloudRegion.region}</Text>
        </Box>
      </Box>
    );
  },
);
