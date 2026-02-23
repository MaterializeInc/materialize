// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { HighlightStyle, syntaxHighlighting } from "@codemirror/language";
import { Extension } from "@codemirror/state";
import { EditorView } from "@codemirror/view";
import { tags as t } from "@lezer/highlight";

import { darkTheme, lightTheme, MaterializeTheme } from "~/theme";

// the default is 1.4, which causes fractional heights
export const lineHeight = 1.5;

export type ThemeSettings = {
  activeLine: string;
  caretColor: string;
  foregroundColor: string;
  selectionBackground: string;
};

export const baseThemeSettings: Record<"light" | "dark", ThemeSettings> = {
  dark: {
    activeLine: "#36334280",
    caretColor: "#C9D1D9",
    foregroundColor: "#C9D1D9",
    selectionBackground: "#003D73",
  },
  light: {
    activeLine: "inherit",
    caretColor: "black",
    foregroundColor: "#24292E",
    selectionBackground: "#BBDFFF",
  },
};

export const createCmTheme = (theme: MaterializeTheme) =>
  EditorView.theme(
    {
      "&": {
        backgroundColor: "transparent",
        color: baseThemeSettings[theme.colorMode].foregroundColor,
      },
      ".cm-content": {
        fontFamily: '"Roboto Mono", Menlo, monospace',
        fontSize: theme.fontSizes.sm,
        padding: "2px 0",
        caretColor: baseThemeSettings[theme.colorMode].caretColor,
      },
      ".cm-scroller": {
        lineHeight,
      },
      ".cm-line": {
        paddingLeft: 0,
      },
      "&.cm-focused": {
        outline: "none",
      },
      ".cm-gutters": {
        border: "none",
        marginRight: "16px",
        fontSize: theme.fontSizes.sm,
        fontFamily: theme.fonts.mono,
        backgroundColor: "transparent",
        color: theme.colors.foreground.tertiary,
      },
      "&.cm-focused .cm-selectionBackground, & .cm-line::selection, & .cm-selectionLayer .cm-selectionBackground, .cm-content ::selection":
        {
          background: `${baseThemeSettings[theme.colorMode].selectionBackground} !important`,
        },
      "& .cm-selectionMatch": {
        backgroundColor: baseThemeSettings[theme.colorMode].selectionBackground,
      },
      ".cm-activeLine": {
        backgroundColor: baseThemeSettings[theme.colorMode].activeLine,
      },
      ".cm-activeLineGutter": {
        backgroundColor: baseThemeSettings[theme.colorMode].activeLine,
      },
    },
    { dark: theme.colorMode === "dark" },
  );

export const darkHighlightStyle = HighlightStyle.define([
  { tag: [t.standard(t.tagName), t.tagName], color: "#7ee787" },
  { tag: [t.comment, t.bracket], color: "#8b949e" },
  { tag: [t.className, t.propertyName], color: "#d2a8ff" },
  {
    tag: [t.variableName, t.attributeName, t.number, t.operator],
    color: "#79c0ff",
  },
  {
    tag: [t.keyword, t.typeName, t.typeOperator, t.typeName],
    color: "#ff7b72",
  },
  { tag: [t.string, t.meta, t.regexp], color: "#a5d6ff" },
  { tag: [t.name, t.quote], color: "#7ee787" },
  { tag: [t.heading, t.strong], color: "#d2a8ff", fontWeight: "bold" },
  { tag: [t.emphasis], color: "#d2a8ff", fontStyle: "italic" },
  { tag: [t.deleted], color: "#ffdcd7", backgroundColor: "ffeef0" },
  { tag: [t.atom, t.bool, t.special(t.variableName)], color: "#ffab70" },
  { tag: t.link, textDecoration: "underline" },
  { tag: t.strikethrough, textDecoration: "line-through" },
  { tag: t.invalid, color: "#f97583" },
]);

export const lightHighlightStyle = HighlightStyle.define([
  { tag: [t.standard(t.tagName), t.tagName], color: "#116329" },
  { tag: [t.comment, t.bracket], color: "#6a737d" },
  { tag: [t.className, t.propertyName], color: "#6f42c1" },
  {
    tag: [t.variableName, t.attributeName, t.number, t.operator],
    color: "#005cc5",
  },
  {
    tag: [t.keyword, t.typeName, t.typeOperator, t.typeName],
    color: "#d73a49",
  },
  { tag: [t.string, t.meta, t.regexp], color: "#032f62" },
  { tag: [t.name, t.quote], color: "#22863a" },
  { tag: [t.heading, t.strong], color: "#24292e", fontWeight: "bold" },
  { tag: [t.emphasis], color: "#24292e", fontStyle: "italic" },
  { tag: [t.deleted], color: "#b31d28", backgroundColor: "ffeef0" },
  { tag: [t.atom, t.bool, t.special(t.variableName)], color: "#e36209" },
  { tag: [t.url, t.escape, t.regexp, t.link], color: "#032f62" },
  { tag: t.link, textDecoration: "underline" },
  { tag: t.strikethrough, textDecoration: "line-through" },
  { tag: t.invalid, color: "#cb2431" },
]);

export const darkLayoutThemeExt = createCmTheme(darkTheme);
export const lightLayoutThemeExt = createCmTheme(lightTheme);

export const darkSyntaxThemeExt = syntaxHighlighting(darkHighlightStyle);
export const lightSyntaxThemeExt = syntaxHighlighting(lightHighlightStyle);

export const themeExtensions: Record<"light" | "dark", Extension[]> = {
  dark: [darkLayoutThemeExt, darkSyntaxThemeExt],
  light: [lightLayoutThemeExt, lightSyntaxThemeExt],
};
