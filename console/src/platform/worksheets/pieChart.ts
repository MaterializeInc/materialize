// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import * as d3 from "d3";

import { WorksheetRow } from "./worksheetRuntime";

const LEGEND_ROW_PX = 16;

export interface PieChartOptions {
  label: string;
  value: string;
  width: number;
  height: number;
  textColor: string;
}

/**
 * Renders a pie of `value` per `label`, with a legend of values and shares.
 * Plot has no pie mark, so this draws with d3 directly.
 */
export function renderPieChart(
  rows: WorksheetRow[],
  { label, value, width, height, textColor }: PieChartOptions,
): SVGSVGElement {
  const slices = rows
    .map((row) => ({ label: String(row[label]), value: Number(row[value]) }))
    .filter((s) => Number.isFinite(s.value) && s.value > 0);
  const total = d3.sum(slices, (s) => s.value);
  // Coloring by sorted label keeps each slice's color stable while a live
  // source adds and removes rows.
  const color = d3
    .scaleOrdinal<string>()
    .domain(slices.map((s) => s.label).sort())
    .range(d3.schemeTableau10);
  const radius = height / 2 - 4;
  const arcs = d3
    .pie<(typeof slices)[number]>()
    .value((s) => s.value)
    .sort((a, b) => b.value - a.value)(slices);
  const arc = d3
    .arc<d3.PieArcDatum<(typeof slices)[number]>>()
    .innerRadius(0)
    .outerRadius(radius);

  const svg = d3
    .create("svg")
    .attr("width", width)
    .attr("height", height)
    .attr("font-size", 11)
    .attr("fill", textColor);

  const pie = svg
    .append("g")
    .attr("transform", `translate(${radius + 4},${height / 2})`);
  pie
    .selectAll("path")
    .data(arcs)
    .join("path")
    .attr("d", arc)
    .attr("fill", (d) => color(d.data.label))
    .attr("stroke", "var(--chakra-colors-background-primary, white)")
    .append("title")
    .text((d) => `${d.data.label}: ${d.data.value}`);

  const legend = svg
    .append("g")
    .attr("transform", `translate(${radius * 2 + 24},8)`)
    .selectAll("g")
    .data(
      [...arcs]
        .sort((a, b) => b.value - a.value)
        .slice(0, Math.floor((height - 8) / LEGEND_ROW_PX)),
    )
    .join("g")
    .attr("transform", (_d, i) => `translate(0,${i * LEGEND_ROW_PX})`);
  legend
    .append("rect")
    .attr("width", 10)
    .attr("height", 10)
    .attr("fill", (d) => color(d.data.label));
  legend
    .append("text")
    .attr("x", 16)
    .attr("y", 9)
    .text(
      (d) =>
        `${d.data.label}  ${d.data.value} (${d3.format(".0%")(d.data.value / total)})`,
    );

  return svg.node() as SVGSVGElement;
}
