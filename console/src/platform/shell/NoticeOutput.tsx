// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Code, useTheme, VStack } from "@chakra-ui/react";
import React from "react";

import { Notice } from "~/api/materialize/types";
import { MaterializeTheme } from "~/theme";

const NoticeOutput = ({ notice }: { notice: Notice }) => {
  const { colors } = useTheme<MaterializeTheme>();
  const messages = [`${notice.severity.toUpperCase()}: ${notice.message}`];
  if (notice.detail) {
    messages.push(`DETAIL: ${notice.detail}`);
  }
  if (notice.hint) {
    messages.push(`HINT: ${notice.hint}`);
  }
  return (
    <VStack alignItems="flex-start" px="3">
      {messages.map((message, index) => (
        <Code
          key={index}
          color={colors.foreground.secondary}
          whiteSpace="pre-wrap"
        >
          {message}
        </Code>
      ))}
    </VStack>
  );
};

export default NoticeOutput;
