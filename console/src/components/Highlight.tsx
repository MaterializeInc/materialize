// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// A fork of the Chakra `Highlight` component that allows case sensitive matching.
// https://github.com/chakra-ui/chakra-ui/blob/f65586674cb9241feeacfce7c5672c9129d1704e/packages/components/src/highlight/highlight.tsx
import {
  HighlightProps as ChakraHighlightProps,
  Mark,
  UseHighlightProps as ChakraUseHighlightProps,
} from "@chakra-ui/react";
import React, { Fragment, useMemo } from "react";

import { escapeRegExp } from "~/util";

interface HighlightProps extends ChakraHighlightProps {
  caseSensitive?: boolean;
}

interface UseHighlightProps extends ChakraUseHighlightProps {
  caseSensitive?: boolean;
}
export interface Chunk {
  text: string;
  match: boolean;
}

function buildRegex(query: string[], caseSensitive: boolean) {
  const _query = query
    .filter((text) => text.length !== 0)
    .map((text) => escapeRegExp(text.trim()));
  if (!_query.length) {
    return null;
  }

  return new RegExp(`(${_query.join("|")})`, `${caseSensitive ? "" : "i"}g`);
}

interface HighlightOptions {
  text: string;
  query: string | string[];
  caseSensitive?: boolean;
}

function highlightWords({
  text,
  query,
  caseSensitive = false,
}: HighlightOptions): Chunk[] {
  const regex = buildRegex(
    Array.isArray(query) ? query : [query],
    caseSensitive,
  );
  if (!regex) {
    return [{ text, match: false }];
  }
  const result = text.split(regex).filter(Boolean);
  return result.map((str) => ({ text: str, match: regex.test(str) }));
}
function useHighlight({ text, query, caseSensitive }: UseHighlightProps) {
  return useMemo(
    () => highlightWords({ text, query, caseSensitive }),
    [text, query, caseSensitive],
  );
}

const Highlight = (props: HighlightProps) => {
  const { children, query, styles, caseSensitive } = props;

  if (typeof children !== "string") {
    throw new Error("The children prop of Highlight must be a string");
  }

  const chunks = useHighlight({ query, text: children, caseSensitive });

  return (
    <>
      {chunks.map((chunk, index) =>
        chunk.match ? (
          <Mark key={index} sx={styles}>
            {chunk.text}
          </Mark>
        ) : (
          <Fragment key={index}>{chunk.text}</Fragment>
        ),
      )}
    </>
  );
};

export default Highlight;
