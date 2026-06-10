// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, chakra, Code, useColorMode } from "@chakra-ui/react";
import { HighlightStyle, LanguageSupport } from "@codemirror/language";
import { Tree } from "@lezer/common";
import { Highlighter, highlightTree } from "@lezer/highlight";
import React, { useEffect, useRef, useState } from "react";
import { StyleModule } from "style-mod";

import type { EditorProps } from "~/components/CommandBlock";
import languageExt from "~/components/CommandBlock/mzDialect";
import {
  baseThemeSettings,
  darkHighlightStyle,
  lightHighlightStyle,
  lineHeight,
  ThemeSettings,
} from "~/components/CommandBlock/theme";

if (darkHighlightStyle.module) {
  StyleModule.mount(document, darkHighlightStyle.module);
}
if (lightHighlightStyle.module) {
  StyleModule.mount(document, lightHighlightStyle.module);
}

// Borrowed from @lezer/highlight@1.2.0. Syntax highlighting currently breaks
// since CodeMirror doesn't yet support Lezer 1.2, so we're vendoring the
// helper function for now.
// https://github.com/lezer-parser/highlight/blob/88ff939d5585514b96cdca5c8eaca6deea6ad03c/src/highlight.ts#L293-L316
function highlightCode(
  code: string,
  tree: Tree,
  highlighter: Highlighter | readonly Highlighter[],
  putText: (code: string, classes: string) => void,
  putBreak: () => void,
  from = 0,
  to = code.length,
) {
  let pos = from;
  function writeTo(p: number, classes: string) {
    if (p <= pos) return;
    for (let text = code.slice(pos, p), i = 0; ; ) {
      const nextBreak = text.indexOf("\n", i);
      const upto = nextBreak < 0 ? text.length : nextBreak;
      if (upto > i) putText(text.slice(i, upto), classes);
      if (nextBreak < 0) break;
      putBreak();
      i = nextBreak + 1;
    }
    pos = p;
  }

  highlightTree(
    tree,
    highlighter,
    (fromPos, toPos, classes) => {
      writeTo(fromPos, "");
      writeTo(toPos, classes);
    },
    from,
    to,
  );
  writeTo(to, "");
}

function getHighlighted(
  value: string,
  ext: LanguageSupport,
  highlightStyle: HighlightStyle,
  themeSettings: ThemeSettings,
): JSX.Element[] {
  const tree = ext.language.parser.parse(value);
  const lines: JSX.Element[] = [];
  let currentTokens: JSX.Element[] = [];

  const addToken = (text: string, classes: string) => {
    currentTokens.push(
      <chakra.span key={currentTokens.length} className={classes}>
        {text}
      </chakra.span>,
    );
  };

  const flushLine = () => {
    lines.push(
      <Box
        lineHeight={lineHeight}
        key={lines.length}
        _selection={{
          background: `${themeSettings.selectionBackground} !important`,
        }}
        sx={{
          "& ::selection": {
            background: themeSettings.selectionBackground,
          },
        }}
      >
        {currentTokens.length > 0 ? currentTokens : <br />}
      </Box>,
    );
    currentTokens = [];
  };

  highlightCode(value, tree, highlightStyle, addToken, flushLine);

  if (currentTokens.length > 0) {
    flushLine();
  }
  return lines;
}

const SyntaxHighlightedBlock = ({
  value,
  containerProps,
}: {
  value: string;
  containerProps: EditorProps["containerProps"];
}) => {
  const { colorMode } = useColorMode();
  const highlightStyle =
    colorMode === "dark" ? darkHighlightStyle : lightHighlightStyle;
  const themeSettings = baseThemeSettings[colorMode];
  const unhighlighted = React.useMemo(() => {
    // Since we load the language extension asynchronously, generate a
    // placeholder of the anticipated height of the highlighted code. This
    // prevents jank when dealing with the Shell's virtual scroller.
    const numLines = value.split("\n").length;
    return <Box lineHeight={lineHeight} height={`${numLines}lh`} />;
  }, [value]);
  const [highlighted, setHighlighted] = useState<JSX.Element[] | null>(null);
  const ext = useRef<LanguageSupport>();

  useEffect(() => {
    async function initExtension() {
      if (!ext.current) {
        ext.current = languageExt;
      }
      setHighlighted(
        getHighlighted(value, ext.current, highlightStyle, themeSettings),
      );
    }
    initExtension();
  }, [value, highlightStyle, themeSettings]);

  return (
    <Code {...(containerProps ?? {})}>
      <Box
        whiteSpace="pre"
        padding="2px 0"
        color={themeSettings.foregroundColor}
      >
        {highlighted ?? unhighlighted}
      </Box>
    </Code>
  );
};

export default SyntaxHighlightedBlock;
