// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  calculateErrorHighlightSegments,
  findErrorLine,
} from "./errorHighlighting";

const MULTILINE_QUERY = `SELECT *
FROM table1

INNER JOIN table2 AbS m ON


WHERE id = 1;`;

const QUERY_WITH_MANY_EMPTY_LINES = `select

1



1;`;

const SINGLE_LINE_QUERY_WHITE_SPACE = "\n\n  SELECT stuff;\n\n";

const SINGLE_LINE_QUERY = "SELECT 1 1;";

const SINGLE_LINE_QUUERY_WITH_ERROR_AT_BEGINNING = "FROM table1";
const SINGLE_LINE_QUERY_WITH_ERROR_AT_END = "SELECT 1@;";

describe("calculateErrorHighlightSegments", () => {
  it("should split a line with the error in the middle", () => {
    const result = calculateErrorHighlightSegments(SINGLE_LINE_QUERY, 7);
    expect(result).toEqual({
      preHighlight: "SELECT ",
      highlightChar: "1",
      postHighlight: " 1;",
    });
  });

  it("should handle an error at the beginning of the line", () => {
    const result = calculateErrorHighlightSegments(
      SINGLE_LINE_QUUERY_WITH_ERROR_AT_BEGINNING,
      0,
    );
    expect(result).toEqual({
      preHighlight: "",
      highlightChar: "F",
      postHighlight: "ROM table1",
    });
  });

  it("should handle an error at the end of the line", () => {
    const result = calculateErrorHighlightSegments(
      SINGLE_LINE_QUERY_WITH_ERROR_AT_END,
      9,
    );
    expect(result).toEqual({
      preHighlight: "SELECT 1@",
      highlightChar: ";",
      postHighlight: "",
    });
  });
});

describe("findErrorLine", () => {
  it("should find the error line with its closest non-empty context lines", () => {
    const result = findErrorLine(MULTILINE_QUERY, 39);
    expect(result).toEqual({
      lineNumber: 3,
      lineText: "INNER JOIN table2 AbS m ON",
      charOffset: 17,
      prevLineText: "FROM table1",
      prevLineNumber: 1,
      nextLineText: "WHERE id = 1;",
      nextLineNumber: 6,
    });
  });

  it("should find context by skipping multiple empty lines", () => {
    const result = findErrorLine(QUERY_WITH_MANY_EMPTY_LINES, 13);
    expect(result).toEqual({
      charOffset: 0,
      lineNumber: 6,
      lineText: "1;",
      nextLineNumber: null,
      nextLineText: "",
      prevLineNumber: 2,
      prevLineText: "1",
    });
  });

  it("should handle an error on the first non-empty line", () => {
    const result = findErrorLine(MULTILINE_QUERY, 5);
    expect(result).toEqual({
      lineNumber: 0,
      lineText: "SELECT *",
      charOffset: 5,
      prevLineText: "",
      prevLineNumber: null,
      nextLineText: "FROM table1",
      nextLineNumber: 1,
    });
  });

  it("should handle an error on the last non-empty line", () => {
    const result = findErrorLine(MULTILINE_QUERY, 60);
    expect(result).toEqual({
      lineNumber: 6,
      lineText: "WHERE id = 1;",
      charOffset: 9,
      prevLineText: "INNER JOIN table2 AbS m ON",
      prevLineNumber: 3,
      nextLineText: "",
      nextLineNumber: null,
    });
  });

  it("should return null for an error position outside the query text", () => {
    expect(findErrorLine(MULTILINE_QUERY, 999)).toBeNull();
  });

  it("should return null for a zero-based error position", () => {
    expect(findErrorLine(MULTILINE_QUERY, 0)).toBeNull();
  });

  it("should handle a query with only one non-empty line", () => {
    const result = findErrorLine(SINGLE_LINE_QUERY_WHITE_SPACE, 5);
    expect(result).toEqual({
      lineNumber: 2,
      lineText: "  SELECT stuff;",
      charOffset: 3,
      prevLineText: "",
      prevLineNumber: null,
      nextLineText: "",
      nextLineNumber: null,
    });
  });
});
