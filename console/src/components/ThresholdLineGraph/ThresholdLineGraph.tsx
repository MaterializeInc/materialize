// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, useTheme } from "@chakra-ui/react";
import { AxisBottom, AxisLeft, AxisScale } from "@visx/axis";
import { localPoint } from "@visx/event";
import { GridRows } from "@visx/grid";
import ParentSize from "@visx/responsive/lib/components/ParentSize";
import { scaleLinear, scaleTime } from "@visx/scale";
import { LinePath } from "@visx/shape";
import { max } from "d3";
import React from "react";

import { MaterializeTheme } from "~/theme";
import {
  buildXYGraphLayoutProps,
  niceTicks,
  relativeTimeTickFormat,
} from "~/utils/graph";

import {
  assignLineColors,
  isBreaching,
  quantizeThreshold,
} from "./thresholdLineGraphHelpers";
import { ThresholdLineSeries } from "./types";

const MARGIN = { top: 20, right: 8, bottom: 36, left: 52 };
const DEFAULT_HEIGHT_PX = 240;
const X_TICK_COUNT = 4;
const Y_TICK_COUNT = 4;

/** Axis top when there is nothing to scale to: no data, or every value zero. */
const FALLBACK_DOMAIN_TOP = 1;

const DEFAULT_THRESHOLD_STEP = 0.1;
/** A 1.5px line is not a pointer target; this strip over the plot is. */
const HANDLE_HIT_HEIGHT_PX = 18;
const PILL_HEIGHT_PX = 18;
const PILL_MIN_WIDTH_PX = 44;
const PILL_CHAR_WIDTH_PX = 7;
const PILL_PADDING_PX = 16;
/** Multiplier on the step for PageUp / PageDown. */
const PAGE_STEP_MULTIPLIER = 10;

export interface ThresholdLineGraphProps<Datum> {
  /** Points in ascending time order. */
  data: Datum[];
  lines: ThresholdLineSeries<Datum>[];
  xAccessor: (d: Datum) => number;
  startTime: number;
  endTime: number;

  threshold: number;
  /** Fires with an already snapped and clamped value. */
  onThresholdChange: (value: number) => void;
  /** Smallest change a drag or an arrow key can make. Defaults to 0.1. */
  thresholdStep?: number;
  /** Floor the control will not go below. Defaults to 0. */
  minThreshold?: number;
  /**
   * Ceiling the control will not go above. Defaults to the top of the y axis,
   * which is as far as the handle can be dragged anyway. A threshold set above
   * the axis by other means is still honored: the line pins to the top of the
   * plot and nothing breaches.
   */
  maxThreshold?: number;

  /** Renders a value for the y axis and for the handle. */
  formatValue: (value: number) => string;
  /** Names the quantity for screen readers, for example "Freshness threshold". */
  thresholdLabel: string;
  /** Describes the whole graph for screen readers. */
  graphLabel: string;

  /**
   * Keys to highlight on top of whatever breaches. A union with the breaching
   * set, not a replacement.
   */
  selectedKeys?: ReadonlySet<string>;
  height?: number;
}

/**
 * A multi-line time series with a threshold the reader can drag.
 *
 * The threshold is the graph's organizing idea rather than an annotation on
 * it: every line whose `breachValue` sits above it gets a color of its own,
 * against the rest in grey, so moving the handle re-sorts the picture into
 * "over" and "under" without a query or a redraw of the page. A line's color
 * comes from its `breachValue` alone, so it keeps that color as the handle
 * moves; the threshold decides only whether the line is drawn in it.
 *
 * Controlled. The caller owns `threshold` and re-renders with each change.
 *
 * Carries no tooltip or legend. A caller that wants either can label its own
 * rows to match the lines by calling `assignLineColors` with the same lines,
 * which is deterministic.
 */
export const ThresholdLineGraph = <Datum,>(
  props: ThresholdLineGraphProps<Datum>,
) => {
  return (
    <Box height={`${props.height ?? DEFAULT_HEIGHT_PX}px`} width="100%">
      <ParentSize debounceTime={10}>
        {(parent) => (
          <ThresholdLineGraphInner
            {...props}
            width={parent.width}
            height={parent.height}
          />
        )}
      </ParentSize>
    </Box>
  );
};

const ThresholdLineGraphInner = <Datum,>(
  props: ThresholdLineGraphProps<Datum> & { width: number; height: number },
) => {
  const { colors, fonts } = useTheme<MaterializeTheme>();
  const svgRef = React.useRef<SVGSVGElement>(null);

  const {
    data,
    lines,
    xAccessor,
    threshold,
    onThresholdChange,
    formatValue,
    selectedKeys,
  } = props;
  const step = props.thresholdStep ?? DEFAULT_THRESHOLD_STEP;
  const minThreshold = props.minThreshold ?? 0;

  const [isDragging, setIsDragging] = React.useState(false);
  const [isFocused, setIsFocused] = React.useState(false);

  // Scaled to the data alone. Letting the threshold raise the axis makes every
  // line shift whenever the handle moves, which is both hard to read and,
  // during a drag, a control chasing its own effect.
  const yTicks = React.useMemo(() => {
    const drawnMax =
      max(data, (d) => max(lines, (line) => line.yAccessor(d) ?? 0)) ?? 0;
    return niceTicks(
      0,
      drawnMax > 0 ? drawnMax : FALLBACK_DOMAIN_TOP,
      Y_TICK_COUNT,
    );
  }, [data, lines]);

  const domainTop = yTicks[yTicks.length - 1];
  const maxThreshold = props.maxThreshold ?? domainTop;

  const xTicks = React.useMemo(
    () => [...niceTicks(props.startTime, props.endTime, X_TICK_COUNT)],
    [props.startTime, props.endTime],
  );

  const {
    xScaleRange,
    yScaleRange,
    svgProps,
    gridRowsProps,
    axisLeftProps,
    axisBottomProps,
    graphEventOverlayProps: plot,
  } = React.useMemo(
    () =>
      buildXYGraphLayoutProps({
        width: props.width,
        height: props.height,
        margin: MARGIN,
      }),
    [props.width, props.height],
  );

  const xScale = React.useMemo(
    () =>
      scaleTime({
        domain: [props.startTime, props.endTime],
        range: xScaleRange,
      }),
    [props.startTime, props.endTime, xScaleRange],
  );

  const yScale = React.useMemo(
    () =>
      scaleLinear({
        domain: [0, domainTop],
        range: yScaleRange,
        clamp: true,
      }),
    [domainTop, yScaleRange],
  );

  // Keyed off the data alone, so dragging the threshold never recolors a line.
  const lineColors = React.useMemo(() => assignLineColors(lines), [lines]);

  const isHighlighted = React.useCallback(
    (line: ThresholdLineSeries<Datum>) =>
      isBreaching(line, threshold) || (selectedKeys?.has(line.key) ?? false),
    [threshold, selectedKeys],
  );

  // Context first, highlights after, so a colored line is never buried under a
  // grey one. Within the highlights the worst breach is drawn last, on top.
  const orderedLines = [
    ...lines.filter((line) => !isHighlighted(line)),
    ...lines.filter((line) => isHighlighted(line)).reverse(),
  ];

  const commitThreshold = React.useCallback(
    (value: number) =>
      onThresholdChange(
        quantizeThreshold(value, {
          step,
          min: minThreshold,
          max: maxThreshold,
        }),
      ),
    [onThresholdChange, step, minThreshold, maxThreshold],
  );

  const handlePointerDown = (event: React.PointerEvent<SVGGElement>) => {
    event.preventDefault();
    setIsDragging(true);
    event.currentTarget.setPointerCapture(event.pointerId);
  };

  const handlePointerMove = (event: React.PointerEvent<SVGGElement>) => {
    if (!isDragging || !svgRef.current) return;
    const point = localPoint(svgRef.current, event);
    if (!point) return;
    commitThreshold(yScale.invert(point.y));
  };

  const endDrag = (event: React.PointerEvent<SVGGElement>) => {
    if (!isDragging) return;
    setIsDragging(false);
    if (event.currentTarget.hasPointerCapture(event.pointerId)) {
      event.currentTarget.releasePointerCapture(event.pointerId);
    }
  };

  const handleKeyDown = (event: React.KeyboardEvent<SVGGElement>) => {
    const nudge = (delta: number) => {
      event.preventDefault();
      commitThreshold(threshold + delta);
    };
    switch (event.key) {
      case "ArrowUp":
      case "ArrowRight":
        return nudge(step);
      case "ArrowDown":
      case "ArrowLeft":
        return nudge(-step);
      case "PageUp":
        return nudge(step * PAGE_STEP_MULTIPLIER);
      case "PageDown":
        return nudge(-step * PAGE_STEP_MULTIPLIER);
      case "Home":
        event.preventDefault();
        return commitThreshold(minThreshold);
      case "End":
        event.preventDefault();
        return commitThreshold(maxThreshold);
    }
  };

  const thresholdY = yScale(threshold);
  const bandHeight = Math.max(0, thresholdY - plot.y);
  const plotRight = plot.x + plot.width;

  const thresholdText = formatValue(threshold);
  const pillWidth = Math.max(
    PILL_MIN_WIDTH_PX,
    thresholdText.length * PILL_CHAR_WIDTH_PX + PILL_PADDING_PX,
  );

  return (
    // `role="group"` rather than `img`: `img` makes the subtree presentational,
    // which would hide the threshold slider from assistive technology.
    <svg ref={svgRef} {...svgProps} role="group" aria-label={props.graphLabel}>
      {/*
        The region over the threshold is a zone, not a boundary. A line crossing
        into it is the answer, and that reads faster than comparing each line
        against a rule.
      */}
      <rect
        x={plot.x}
        y={plot.y}
        width={plot.width}
        height={bandHeight}
        fill={colors.background.error}
        pointerEvents="none"
      />
      <GridRows
        {...gridRowsProps}
        scale={yScale}
        stroke={colors.border.primary}
        strokeDasharray="4"
        tickValues={yTicks}
        pointerEvents="none"
      />
      <AxisLeft<AxisScale<number>>
        {...axisLeftProps}
        scale={yScale}
        hideAxisLine
        hideTicks
        tickValues={yTicks}
        tickFormat={(value) => formatValue(Number(value))}
        tickLabelProps={() => ({
          dy: "4px",
          fill: colors.foreground.primary,
          fontFamily: fonts.mono,
          fontSize: 12,
          textAnchor: "end",
        })}
      />
      <AxisBottom<AxisScale<number>>
        {...axisBottomProps}
        scale={xScale}
        stroke={colors.border.primary}
        strokeWidth={2}
        tickStroke={colors.border.primary}
        tickValues={xTicks}
        tickFormat={(value) =>
          relativeTimeTickFormat(
            Number(value),
            new Date(props.startTime),
            new Date(props.endTime),
          )
        }
        tickLabelProps={() => ({
          dy: 4,
          fill: colors.foreground.primary,
          fontFamily: fonts.mono,
          fontSize: 12,
          textAnchor: "middle",
        })}
      />

      {orderedLines.map((line) => {
        const highlighted = isHighlighted(line);
        return (
          <LinePath
            key={line.key}
            data={data}
            stroke={
              highlighted ? lineColors.get(line.key) : colors.border.secondary
            }
            strokeWidth={highlighted ? 2 : 1}
            strokeLinejoin="round"
            defined={(d) => line.yAccessor(d) !== null}
            x={(d) => xScale(xAccessor(d))}
            y={(d) => yScale(line.yAccessor(d) ?? 0)}
          />
        );
      })}

      <line
        x1={plot.x}
        x2={plotRight}
        y1={thresholdY}
        y2={thresholdY}
        stroke={colors.accent.red}
        strokeWidth={1.5}
        strokeDasharray="5 4"
        pointerEvents="none"
      />
      <g
        role="slider"
        tabIndex={0}
        aria-label={props.thresholdLabel}
        aria-orientation="vertical"
        aria-valuenow={threshold}
        aria-valuemin={minThreshold}
        aria-valuemax={maxThreshold}
        aria-valuetext={thresholdText}
        cursor="ns-resize"
        onPointerDown={handlePointerDown}
        onPointerMove={handlePointerMove}
        onPointerUp={endDrag}
        onPointerCancel={endDrag}
        onKeyDown={handleKeyDown}
        onFocus={() => setIsFocused(true)}
        onBlur={() => setIsFocused(false)}
        style={{ outline: "none" }}
      >
        <rect
          x={plot.x}
          y={thresholdY - HANDLE_HIT_HEIGHT_PX / 2}
          width={plot.width}
          height={HANDLE_HIT_HEIGHT_PX}
          fill="transparent"
        />
        <rect
          x={plotRight - pillWidth}
          y={thresholdY - PILL_HEIGHT_PX / 2}
          width={pillWidth}
          height={PILL_HEIGHT_PX}
          rx={PILL_HEIGHT_PX / 2}
          fill={colors.accent.red}
          stroke={isFocused ? colors.foreground.primary : undefined}
          strokeWidth={isFocused ? 2 : 0}
          opacity={isDragging ? 1 : 0.9}
        />
        <text
          x={plotRight - pillWidth / 2}
          y={thresholdY}
          dy="0.35em"
          textAnchor="middle"
          fill={colors.foreground.inverse}
          fontFamily={fonts.mono}
          fontSize={12}
          pointerEvents="none"
        >
          {thresholdText}
        </text>
      </g>
    </svg>
  );
};
