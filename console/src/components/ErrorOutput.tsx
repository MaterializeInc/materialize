// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Code, StackProps, useTheme, VStack } from "@chakra-ui/react";
import React from "react";

import { Error as MaterializeError } from "~/api/materialize/types";
import { MaterializeTheme } from "~/theme";

import { ErrorHighlighter } from "./ErrorHighlighter";
import {
  calculateErrorHighlightSegments,
  findErrorLine,
  getSortedErrorContextLines,
} from "./errorHighlighting";

const ErrorOutput = ({
  error,
  errorMessageOverride,
  command,
  ...props
}: {
  error: MaterializeError;
  errorMessageOverride?: string;
  command?: string;
} & StackProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const messages: React.ReactNode[] = [
    errorMessageOverride ?? `Error: ${error.message}`,
  ];
  if (error.detail) {
    messages.push(`Detail: ${error.detail}`);
  }
  if (error.hint) {
    messages.push(`Hint: ${error.hint}`);
  }

  if (error.position !== undefined && command) {
    messages.push("");

    // Single line query
    if (!command.includes("\n")) {
      const segments = calculateErrorHighlightSegments(command, error.position);
      if (segments) {
        messages.push(
          <ErrorHighlighter
            key="error-line"
            text={command}
            segments={segments}
            lineNumber={1}
          />,
        );
      }
    } else {
      const contextLines = getSortedErrorContextLines(command, error.position);
      for (const line of contextLines) {
        if (line.type === "current") {
          const errorLineDetails = findErrorLine(command, error.position);
          const segments = calculateErrorHighlightSegments(
            line.lineText,
            errorLineDetails?.charOffset ?? 0,
          );
          if (segments) {
            messages.push(
              <ErrorHighlighter
                key={`error-line-${line.lineNumber}`}
                text={line.lineText}
                segments={segments}
                lineNumber={line.lineNumber + 1}
              />,
            );
          }
        } else {
          messages.push(
            <Code key={`line-${line.lineNumber}`} fontFamily="monospace">
              {`LINE ${line.lineNumber + 1}: ${line.lineText}`}
            </Code>,
          );
        }
      }
    }
  }

  return (
    <VStack
      alignItems="flex-start"
      borderRadius="lg"
      borderWidth="1px"
      borderColor={colors.border.secondary}
      p="4"
      {...props}
    >
      {messages.map((messageContent, index) => (
        <Code
          key={index}
          whiteSpace="pre-wrap"
          fontFamily="monospace"
          color={messageContent === "" ? "transparent" : undefined}
        >
          {messageContent}
        </Code>
      ))}
    </VStack>
  );
};

export default ErrorOutput;
