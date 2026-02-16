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
  Center,
  FlexProps,
  Grid,
  Hide,
  HStack,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React from "react";

import { CodeBlock } from "~/components/copyableComponents";
import { MaterializeTheme } from "~/theme";

import { UnauthenticatedPageFooter } from "./PageFooter";

export const AuthLayout = ({
  children,
  ...props
}: React.PropsWithChildren<FlexProps>) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <Center background={colors.background.primary} height="100vh" {...props}>
      <VStack alignItems="stretch" flexGrow="1" spacing="0" height="100%">
        <Grid
          gridTemplateColumns={{ base: "1fr", lg: "1fr 1fr" }}
          gridTemplateRows={{ base: "1fr 3fr", lg: "1fr" }}
          alignItems={{ base: "start", lg: "center" }}
          height="100%"
          width="100%"
        >
          <Hide below="lg">
            <HStack my="8" justifyContent="center" mx="12">
              <CodeBlock
                lineNumbers
                title="Create a user"
                contents={`CREATE ROLE mat_admin WITH LOGIN PASSWORD 'password';`}
                width="600px"
              >
                {`CREATE ROLE mat_admin WITH LOGIN PASSWORD 'password';`}
              </CodeBlock>
            </HStack>
          </Hide>
          <Center justifyContent={{ base: "center", lg: "flex-start" }}>
            {children}
          </Center>
        </Grid>
        <UnauthenticatedPageFooter />
      </VStack>
    </Center>
  );
};

export const AuthContentContainer = ({
  children,
  ...containerProps
}: React.PropsWithChildren<BoxProps>) => {
  return (
    <Box width="420px" {...containerProps}>
      {children}
    </Box>
  );
};
