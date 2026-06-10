// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

export interface ErrorHighlightSegments {
  preHighlight: string;
  highlightChar: string;
  postHighlight: string;
}

export interface ErrorLine {
  lineNumber: number;
  lineText: string;
  charOffset: number;
  prevLineText?: string | null;
  prevLineNumber?: number | null;
  nextLineText?: string | null;
  nextLineNumber?: number | null;
}

interface LineInfo {
  text: string;
  lineNumber: number;
}

export interface ErrorContextLine {
  lineNumber: number;
  lineText: string;
  type: "prev" | "current" | "next";
}

/**
 * Calculates the segments of text to highlight for an error position within a specific line.
 * @param lineText - The text of the line containing the error.
 * @param errorIndexInLine - The 0-based index of the error character within lineText.
 * @returns Object containing the different segments of text for highlighting, or null if errorIndexInLine is invalid.
 */
export function calculateErrorHighlightSegments(
  lineText: string,
  errorIndexInLine: number,
): ErrorHighlightSegments | null {
  return {
    preHighlight: lineText.substring(0, errorIndexInLine),
    highlightChar: lineText[errorIndexInLine],
    postHighlight: lineText.substring(errorIndexInLine + 1),
  };
}

/**
 * Checks if a line is empty or only contains whitespace
 * @param line - The line to check
 * @returns True if the line is empty or only contains whitespace
 */
function isLineEmpty(line: string): boolean {
  return line.trim() === "";
}

/**
 * Finds the line containing the error position in a multi-line text,
 * @param text - The multi-line text
 * @param position - The 1-based position of the error
 * @returns The error line number, its text, character offset, and the text of the closest non-empty lines before and after.
 * Returns null if the position is invalid or the line cannot be found.
 */

export function findErrorLine(
  text: string,
  position: number,
): ErrorLine | null {
  if (position <= 0) {
    return null;
  }

  const lines = text.split("\n");

  // filtering out empty space
  const nonEmptyLines = lines
    .map((line, index) => {
      if (!isLineEmpty(line)) {
        return {
          text: line,
          lineNumber: index,
        };
      }
      return null;
    })
    .filter((line): line is LineInfo => line !== null);

  // Find the error line using original line indices
  let errorLine: LineInfo | null = null;
  let errorPositionOffset = -1;

  let lineStart = 0;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    // add 1 to account for the newline character
    const nextLineStart = lineStart + line.length + 1;

    if (position >= lineStart && position < nextLineStart) {
      errorLine = {
        text: line,
        lineNumber: i,
      };
      errorPositionOffset = position - lineStart;
      break;
    }
    lineStart = nextLineStart;
  }

  if (errorLine === null) {
    return null;
  }

  const errorLineIndex = nonEmptyLines.findIndex(
    (line) => line.lineNumber === errorLine.lineNumber,
  );

  let prevLine: LineInfo | null = null;

  if (errorLine.lineNumber > 0) {
    const prevLineInfo = nonEmptyLines[errorLineIndex - 1];
    if (prevLineInfo) {
      prevLine = {
        text: prevLineInfo.text,
        lineNumber: prevLineInfo.lineNumber,
      };
    }
  }

  let nextLine: LineInfo | null = null;

  if (errorLine.lineNumber < lines.length - 1) {
    const nextLineInfo = nonEmptyLines[errorLineIndex + 1];
    if (nextLineInfo) {
      nextLine = {
        text: nextLineInfo.text,
        lineNumber: nextLineInfo.lineNumber,
      };
    }
  }

  return {
    lineNumber: errorLine.lineNumber,
    lineText: errorLine.text,
    charOffset: errorPositionOffset,
    prevLineText: prevLine?.text ?? "",
    prevLineNumber: prevLine ? prevLine.lineNumber : null,
    nextLineText: nextLine?.text ?? "",
    nextLineNumber: nextLine ? nextLine.lineNumber : null,
  };
}

export function getSortedErrorContextLines(
  text: string,
  position: number,
): ErrorContextLine[] {
  const errorLine = findErrorLine(text, position);
  if (!errorLine) return [];

  const lines: ErrorContextLine[] = [];

  if (
    errorLine.prevLineText !== undefined &&
    errorLine.prevLineText !== null &&
    errorLine.prevLineNumber !== undefined &&
    errorLine.prevLineNumber !== null
  ) {
    lines.push({
      lineNumber: errorLine.prevLineNumber,
      lineText: errorLine.prevLineText,
      type: "prev",
    });
  }

  lines.push({
    lineNumber: errorLine.lineNumber,
    lineText: errorLine.lineText,
    type: "current",
  });

  if (
    errorLine.nextLineText !== undefined &&
    errorLine.nextLineText !== null &&
    errorLine.nextLineNumber !== undefined &&
    errorLine.nextLineNumber !== null
  ) {
    lines.push({
      lineNumber: errorLine.nextLineNumber,
      lineText: errorLine.nextLineText,
      type: "next",
    });
  }

  // Sort by line number
  lines.sort((a, b) => a.lineNumber - b.lineNumber);

  return lines;
}
