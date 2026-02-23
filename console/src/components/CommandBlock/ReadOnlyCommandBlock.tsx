// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Code, CodeProps, useTheme } from "@chakra-ui/react";
import { json } from "@codemirror/lang-json";
import { Annotation, EditorState } from "@codemirror/state";
import { EditorView, lineNumbers as initLineNumbers } from "@codemirror/view";
import React, { useEffect, useRef } from "react";

import { useValueOnMount } from "~/hooks/useValueOnMount";
import { MaterializeTheme } from "~/theme";
import { assert } from "~/util";

import highlightExt, { setHighlight } from "./highlightExt";
import languageExt from "./mzDialect";
import { themeExtensions } from "./theme";

export type CommandBlockProps = {
  value: string;
  containerProps?: CodeProps;
  lineNumbers?: boolean;
  lineWrap?: boolean;
  highlight?: boolean;
  highlightTerm?: string;
  language?: "sql" | "json";
};
const External = Annotation.define<boolean>();
const lineNumbersExt = initLineNumbers({});

export const ReadOnlyCommandBlock = ({
  containerProps,
  highlight = false,
  highlightTerm,
  lineNumbers = false,
  lineWrap = false,
  value,
  language = "sql",
}: CommandBlockProps) => {
  const { colors, colorMode } = useTheme<MaterializeTheme>();

  const container = useRef<HTMLDivElement | null>(null);
  const editorView = useRef<EditorView | undefined>();
  const initialValue = useValueOnMount(value);

  useEffect(() => {
    async function initExtensions(currentContainer: HTMLDivElement) {
      let langExt = null;
      if (language === "sql") {
        langExt = languageExt;
      } else if (language === "json") {
        langExt = json();
      } else {
        throw new Error(`Unsupported language: ${language}`);
      }
      assert(langExt !== null);
      const extensions = [
        langExt,
        EditorState.readOnly.of(true),
        EditorView.editable.of(false),
        // Set a tabindex for the CodeMirror content block so it can be focused
        // and receive mouse selection events. Without this, selection in
        // Firefox can be hard to activate without extra clicks.
        EditorView.contentAttributes.of({ tabindex: "0" }),
        ...themeExtensions[colorMode],
        EditorView.theme({
          ".highlight": {
            "background-color":
              colorMode === "light" ? colors.purple[200] : colors.purple[700],
          },
        }),
      ];

      if (highlight) {
        extensions.push(highlightExt);
      }
      if (lineNumbers) {
        extensions.push(lineNumbersExt);
      }
      if (lineWrap) {
        extensions.push(EditorView.lineWrapping);
      }

      const state: EditorState = EditorState.create({
        doc: initialValue.current,
        extensions,
      });

      editorView.current = new EditorView({
        state: state,
        parent: currentContainer,
      });
    }

    if (!container.current) {
      return;
    }

    initExtensions(container.current);

    return () => {
      if (editorView.current) {
        editorView.current.destroy();
      }
    };
  }, [
    colors,
    colorMode,
    highlight,
    lineNumbers,
    lineWrap,
    initialValue,
    language,
  ]);

  useEffect(() => {
    if (editorView.current) {
      const view = editorView.current;
      const curVal = view.state.doc.toString();
      if (curVal !== value) {
        editorView.current.dispatch({
          changes: { from: 0, to: curVal.length, insert: value },
          annotations: [External.of(true)],
        });
      }
    }
  }, [value]);

  useEffect(() => {
    /// Update highlights when the highlight term or document content changes.
    if (editorView.current && highlight && highlightTerm !== undefined) {
      editorView.current.dispatch({
        effects: [setHighlight.of(highlightTerm)],
      });
    }
  }, [highlight, highlightTerm, value]);

  return (
    <Code {...(containerProps ?? {})} cursor="text" tabIndex={0}>
      <div ref={container}></div>
    </Code>
  );
};

export default ReadOnlyCommandBlock;
