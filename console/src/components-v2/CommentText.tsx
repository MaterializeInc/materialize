// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Code, Text, TextProps } from "@chakra-ui/react";
import React from "react";

export interface CommentTextProps extends TextProps {
  children: string;
}

/**
 * Renders a comment string with backtick-enclosed segments styled as inline code.
 */
export const CommentText = ({ children, ...textProps }: CommentTextProps) => {
  const parts = parseBackticks(children);

  return (
    <Text {...textProps}>
      {parts.map((part, i) =>
        part.isCode ? (
          <Code key={i} variant="inline-syntax" fontSize="inherit">
            {part.text}
          </Code>
        ) : (
          <React.Fragment key={i}>{part.text}</React.Fragment>
        ),
      )}
    </Text>
  );
};

interface TextPart {
  text: string;
  isCode: boolean;
}

function parseBackticks(input: string): TextPart[] {
  const parts: TextPart[] = [];
  const regex = /`([^`]+)`/g;
  let lastIndex = 0;
  let match;

  while ((match = regex.exec(input)) !== null) {
    if (match.index > lastIndex) {
      parts.push({ text: input.slice(lastIndex, match.index), isCode: false });
    }
    parts.push({ text: match[1], isCode: true });
    lastIndex = regex.lastIndex;
  }

  if (lastIndex < input.length) {
    parts.push({ text: input.slice(lastIndex), isCode: false });
  }

  return parts;
}
