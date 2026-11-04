// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, BoxProps, useLatestRef, useTheme } from "@chakra-ui/react";
import {
  acceptCompletion,
  autocompletion,
  closeCompletion,
  moveCompletionSelection,
  startCompletion,
} from "@codemirror/autocomplete";
import { defaultKeymap, history, historyKeymap } from "@codemirror/commands";
import { Compartment, EditorState, Extension, Prec } from "@codemirror/state";
import {
  drawSelection,
  EditorView,
  keymap,
  lineNumbers as codeMirrorLineNumbers,
  placeholder as codeMirrorPlaceholder,
} from "@codemirror/view";
import React, { useEffect, useMemo, useRef } from "react";

import languageExt from "~/components/CommandBlock/mzDialect";
import { themeExtensions } from "~/components/CommandBlock/theme";
import { MaterializeTheme } from "~/theme";

import {
  forgetEditor,
  noteEditorFocused,
  setFallbackEditor,
} from "./activeEditor";

/**
 * Completion keys. At the highest precedence, so ↑/↓ move through an open
 * completion list before they reach the history bindings. Each command does
 * nothing when no list is open, letting the key fall through.
 */
const completionKeymap = Prec.highest(
  keymap.of([
    { key: "Tab", run: acceptCompletion },
    { key: "ArrowDown", run: moveCompletionSelection(true) },
    { key: "ArrowUp", run: moveCompletionSelection(false) },
    { key: "PageDown", run: moveCompletionSelection(true, "page") },
    { key: "PageUp", run: moveCompletionSelection(false, "page") },
    { key: "Escape", run: closeCompletion },
    { key: "Ctrl-Space", run: startCompletion },
  ]),
);

export type HistoryDirection = "previous" | "next";

export interface CellEditorProps {
  value: string;
  /** The SQL language, including its completions. Swapped in place. */
  language?: Extension;
  /**
   * Called for ↑ on the first line and ↓ on the last. Returning true
   * consumes the key; otherwise the cursor moves as usual.
   */
  onHistory?: (direction: HistoryDirection) => boolean;
  /**
   * Called for Tab and Shift-Tab once no completion is open, to move to
   * another cell. Without it, Tab leaves the editor as the browser decides.
   */
  onTab?: (direction: CellDirection) => void;
  onChange: (value: string) => void;
  /** Bound to Shift-Enter and Mod-Enter. */
  onRun: () => void;
  /** Clips the editor to its first line. */
  collapsed?: boolean;
  autoFocus?: boolean;
  placeholder?: string;
  containerProps?: BoxProps;
  /** Receives explorer insertions until another editor is focused. */
  isDefaultInsertTarget?: boolean;
  /** Shows line numbers. Omitted, the editor has none. */
  lineNumbers?: LineNumbering;
}

export interface LineNumbering {
  /** The number shown on the editor's first line. */
  first: number;
  /**
   * The gutter's width in digits. Cells of one worksheet share it, so their
   * code lines up.
   */
  digits: number;
}

const lineNumberExtension = (numbering: LineNumbering | undefined) =>
  numbering
    ? [
        codeMirrorLineNumbers({
          formatNumber: (n) => String(n + numbering.first - 1),
        }),
        EditorView.theme({
          ".cm-lineNumbers .cm-gutterElement": {
            // Chakra's global border-box would count the padding into the
            // width, leaving narrower numbers' code a couple of pixels left.
            boxSizing: "content-box",
            minWidth: `${numbering.digits}ch`,
          },
        }),
      ]
    : [];

/**
 * A standalone CodeMirror instance per cell. The shell's `Editor` reads and
 * writes the shell's single prompt atom, so it cannot back several cells.
 *
 * The view is created once. A `value` that differs from the document, such
 * as a reference renamed elsewhere or an edit from another tab, is applied as
 * a transaction, so undo history survives it.
 */
export type CellDirection = "next" | "previous";

export const CellEditor = ({
  value,
  language = languageExt,
  onHistory,
  onTab,
  onChange,
  onRun,
  collapsed = false,
  autoFocus = false,
  placeholder,
  containerProps,
  isDefaultInsertTarget = false,
  lineNumbers,
}: CellEditorProps) => {
  const { colorMode, colors } = useTheme<MaterializeTheme>();
  const containerRef = useRef<HTMLDivElement>(null);
  const viewRef = useRef<EditorView>();
  const themeRef = useRef(new Compartment());
  const languageRef = useRef(new Compartment());
  const lineNumbersRef = useRef(new Compartment());
  const onChangeRef = useLatestRef(onChange);
  const onRunRef = useLatestRef(onRun);
  const onHistoryRef = useLatestRef(onHistory);
  const onTabRef = useLatestRef(onTab);
  // The completion popup is styled by CodeMirror, not Chakra, so it needs
  // the theme's colors spelled out to follow light and dark mode.
  const theme = useMemo(
    () => [
      themeExtensions[colorMode],
      EditorView.theme(
        {
          ".cm-tooltip.cm-tooltip-autocomplete": {
            backgroundColor: colors.background.primary,
            border: `1px solid ${colors.border.primary}`,
            borderRadius: "6px",
            boxShadow: "0 4px 12px rgba(0, 0, 0, 0.12)",
            overflow: "hidden",
          },
          ".cm-tooltip-autocomplete > ul > li": {
            padding: "2px 8px",
            color: colors.foreground.primary,
          },
          ".cm-tooltip-autocomplete > ul > li[aria-selected]": {
            backgroundColor: colors.background.tertiary,
            color: colors.foreground.primary,
          },
          ".cm-completionDetail": {
            color: colors.foreground.tertiary,
            fontStyle: "normal",
            marginLeft: "1em",
          },
          // Tighter than the shell's gutter, which has room to spare.
          ".cm-gutters": { marginRight: "8px" },
          ".cm-lineNumbers .cm-gutterElement": { padding: "0 2px 0 0" },
          ".cm-completionMatchedText": {
            textDecoration: "none",
            color: colors.accent.purple,
            fontWeight: "600",
          },
        },
        { dark: colorMode === "dark" },
      ),
    ],
    [colorMode, colors],
  );
  const initialRef = useLatestRef({
    lineNumbers,
    value,
    theme,
    language,
    autoFocus,
    placeholder,
  });

  useEffect(() => {
    if (!containerRef.current) return;
    const initial = initialRef.current;
    const view = new EditorView({
      parent: containerRef.current,
      state: EditorState.create({
        doc: initial.value,
        extensions: [
          history(),
          drawSelection(),
          languageRef.current.of(initial.language),
          themeRef.current.of(initial.theme),
          // The default completion keymap accepts on Enter, which in SQL
          // swallows the newline that ends a clause. Tab accepts instead.
          autocompletion({ icons: false, defaultKeymap: false }),
          completionKeymap,
          EditorView.lineWrapping,
          lineNumbersRef.current.of(lineNumberExtension(initial.lineNumbers)),
          ...(initial.placeholder
            ? [codeMirrorPlaceholder(initial.placeholder)]
            : []),
          keymap.of([
            {
              key: "Mod-Enter",
              run: () => {
                onRunRef.current();
                return true;
              },
            },
            {
              key: "Shift-Enter",
              run: () => {
                onRunRef.current();
                return true;
              },
            },
            {
              key: "Tab",
              run: () => {
                if (!onTabRef.current) return false;
                onTabRef.current("next");
                return true;
              },
              shift: () => {
                if (!onTabRef.current) return false;
                onTabRef.current("previous");
                return true;
              },
            },
            {
              key: "ArrowUp",
              run: (v) => {
                const { main } = v.state.selection;
                if (!main.empty || v.state.doc.lineAt(main.head).number !== 1) {
                  return false;
                }
                return onHistoryRef.current?.("previous") ?? false;
              },
            },
            {
              key: "ArrowDown",
              run: (v) => {
                const { main } = v.state.selection;
                const { doc } = v.state;
                if (!main.empty || doc.lineAt(main.head).number !== doc.lines) {
                  return false;
                }
                return onHistoryRef.current?.("next") ?? false;
              },
            },
            ...defaultKeymap,
            ...historyKeymap,
          ]),
          EditorView.domEventHandlers({
            focus: (_event, v) => {
              noteEditorFocused(v);
            },
          }),
          EditorView.updateListener.of((update) => {
            if (update.docChanged) {
              onChangeRef.current(update.state.doc.toString());
            }
          }),
        ],
      }),
    });
    viewRef.current = view;
    if (initial.autoFocus) view.focus();
    return () => {
      forgetEditor(view);
      view.destroy();
      viewRef.current = undefined;
    };
  }, [initialRef, onChangeRef, onRunRef, onHistoryRef, onTabRef]);

  useEffect(() => {
    viewRef.current?.dispatch({
      effects: themeRef.current.reconfigure(theme),
    });
  }, [theme]);

  const firstLine = lineNumbers?.first;
  const lineDigits = lineNumbers?.digits;
  useEffect(() => {
    viewRef.current?.dispatch({
      effects: lineNumbersRef.current.reconfigure(
        lineNumberExtension(
          firstLine === undefined || lineDigits === undefined
            ? undefined
            : { first: firstLine, digits: lineDigits },
        ),
      ),
    });
  }, [firstLine, lineDigits]);

  useEffect(() => {
    const view = viewRef.current;
    if (!isDefaultInsertTarget || !view) return;
    setFallbackEditor(view);
    return () => setFallbackEditor(undefined);
  }, [isDefaultInsertTarget]);

  useEffect(() => {
    viewRef.current?.dispatch({
      effects: languageRef.current.reconfigure(language),
    });
  }, [language]);

  useEffect(() => {
    const view = viewRef.current;
    if (!view || view.state.doc.toString() === value) return;
    view.dispatch({
      changes: { from: 0, to: view.state.doc.length, insert: value },
    });
  }, [value]);

  return (
    <Box
      ref={containerRef}
      px="1"
      // One 1.5em line plus the 2px top and bottom padding the theme gives
      // `.cm-content`.
      maxHeight={collapsed ? "calc(1.5em + 4px)" : undefined}
      overflow={collapsed ? "hidden" : undefined}
      fontFamily="mono"
      fontSize="sm"
      {...containerProps}
    />
  );
};
