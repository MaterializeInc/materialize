// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Alert,
  AlertIcon,
  Flex,
  FlexProps,
  Text,
  VStack,
} from "@chakra-ui/react";
import React from "react";

import SupportLink from "./SupportLink";

export interface ErrorBoxProps extends FlexProps {
  message?: string;
}

/** Displays a simple error message that takes up its full container */
const ErrorBox = ({ message, children, ...flexProps }: ErrorBoxProps) => {
  return (
    <Flex
      h="100%"
      w="100%"
      alignItems="center"
      justifyContent="center"
      {...flexProps}
    >
      <VStack spacing={2}>
        <Alert status="error" rounded="md" p={4} marginTop={2}>
          <AlertIcon />
          {message ?? "An unexpected error has occurred"}
        </Alert>
        <Text>
          Visit our <SupportLink>help center</SupportLink> if the issue
          persists.
        </Text>
        {children}
      </VStack>
    </Flex>
  );
};

export default ErrorBox;
