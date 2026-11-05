// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { ChartSpec, WorksheetCell } from "./store";

/**
 * A y choice that plots the number of rows per x instead of a column. A
 * column cannot carry this name unless quoted, since `count(*)` defaults to
 * the name `count`.
 */
export const COUNT_OF_ROWS = "count(*)";

/** Lines in a cell's source, as its editor numbers them. */
export function lineCount(source: string) {
  return source.split("\n").length;
}

/**
 * A cell's dashboard title: the first sentence of a leading `--` comment,
 * or the cell's name. The rest of the comment is offered as detail.
 */
export function cellTitle(cell: Pick<WorksheetCell, "name" | "source">): {
  title: string;
  detail?: string;
} {
  const firstLine = cell.source.trimStart().split("\n")[0] ?? "";
  const comment = /^--\s*(.*)$/.exec(firstLine)?.[1].trim();
  if (!comment) return { title: cell.name };
  const sentenceEnd = comment.search(/\.(\s|$)/);
  return sentenceEnd > 0
    ? { title: comment.slice(0, sentenceEnd), detail: comment }
    : { title: comment };
}

/** A chart's dashboard title, from what it plots. */
export function chartTitle(chart: ChartSpec) {
  const value = chart.y === COUNT_OF_ROWS ? "Rows" : chart.y;
  const by = chart.mark === "pie" ? "per" : "by";
  const series =
    chart.color && chart.mark !== "pie" ? `, per ${chart.color}` : "";
  return chart.x && chart.y
    ? `${value} ${by} ${chart.x}${series}`
    : chart.source;
}

/**
 * Sizes a native select to its longest option. `width="auto"` sizes it to
 * the text alone and the chevron then covers the last characters.
 */
export function selectWidth(options: string[]) {
  const longest = Math.max(3, ...options.map((o) => o.length));
  return `calc(${longest}ch + 2rem)`;
}
