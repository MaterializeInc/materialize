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
  BoxProps,
  ChakraTheme,
  IconButton,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { useAtomCallback } from "jotai/utils";
import React, { forwardRef, PropsWithChildren } from "react";

import { TabbedCodeBlock } from "~/components/copyableComponents";
import CommandIcon from "~/svg/CommandIcon";
import { MaterializeTheme, ThemeColors } from "~/theme";
import ColorsType from "~/theme/colors";

import { saveClearPrompt, setPromptValue } from "./store/prompt";

export type RunnableProps = {
  runCommand: (value: string) => void;
  title: string;
  value: string;
};

export const Runnable = ({ runCommand, value, title }: RunnableProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const setPrompt = useAtomCallback(setPromptValue);
  const clearPrompt = useAtomCallback(saveClearPrompt);

  return (
    <TabbedCodeBlock
      width="100%"
      flexShrink="0"
      tabs={[{ title, contents: value }]}
      lineNumbers
      headingIcon={
        <IconButton
          aria-label="Run command button"
          title="Run command"
          onClick={() => {
            setPrompt(value);
            runCommand(value);
            clearPrompt();
          }}
          icon={<CommandIcon color={colors.foreground.secondary} />}
          variant="unstyled"
          rounded={0}
          sx={{
            _hover: {
              background: "rgba(255, 255, 255, 0.06)",
            },
          }}
        />
      }
    />
  );
};

export const TextContainer = ({ children }: PropsWithChildren) => (
  <VStack spacing="4" alignItems="flex-start">
    {children}
  </VStack>
);

export const RunnableContainer = ({
  children,
  ...rest
}: PropsWithChildren & BoxProps) => (
  <VStack spacing="6" {...rest}>
    {children}
  </VStack>
);

export const StepLayout = forwardRef(
  ({ children, ...rest }: PropsWithChildren & BoxProps, ref) => (
    <VStack
      spacing="6"
      alignItems="stretch"
      overflow="auto"
      flexGrow="1"
      minHeight="0"
      paddingBottom="6"
      paddingX="10"
      ref={ref}
      {...rest}
    >
      {children}
    </VStack>
  ),
);

export type StepRenderProps = {
  runCommand: (value: string) => void;
  title: string;
  colors: ChakraTheme["colors"] & ThemeColors & typeof ColorsType;
};

export type StepData = {
  title: string;
  render: (props: StepRenderProps) => JSX.Element;
};

// Re-export Box so step-data files don't have to import from chakra directly
// for the common case of wrapping a widget in a Box.
export { Box };
