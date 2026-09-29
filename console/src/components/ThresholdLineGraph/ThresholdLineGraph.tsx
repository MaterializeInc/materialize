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
import { Group } from "@visx/group";
import ParentSize, {
  ParentSizeProvidedProps,
} from "@visx/responsive/lib/components/ParentSize";
import { scaleLinear, scaleTime } from "@visx/scale";
import { Line, LinePath } from "@visx/shape";
import { max } from "d3";
import React from "react";

import { useDrag } from "~/hooks/useDrag";
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

/** Axis top when there is nothing to scale to. */
const FALLBACK_DOMAIN_TOP = 1;

const DEFAULT_THRESHOLD_STEP = 0.1;
const HANDLE_HIT_HEIGHT_PX = 18;
const PILL_HEIGHT_PX = 18;
const PILL_MIN_WIDTH_PX = 44;
const PILL_CHAR_WIDTH_PX = 7;
const PILL_PADDING_PX = 16;
const PAGE_STEP_MULTIPLIER = 10;

export interface ThresholdLineGraphProps<Datum> {
  /** Points in ascending time order. */
  data: Datum[];
  lines: ThresholdLineSeries<Datum>[];
  xAccessor: (d: Datum) => number;
  startTime: number;
  endTime: number;

  threshold: number;
  onThresholdChange: (value: number) => void;
  /** Smallest change a drag or key can make. Defaults to 0.1. */
  thresholdStep?: number;
  minThreshold?: number;
  /**
   * Defaults to the top of the y axis. A larger value set by other means still
   * holds: the line pins to the top of the plot and nothing breaches.
   */
  maxThreshold?: number;

  formatValue: (value: number) => string;
  thresholdLabel: string;
  graphLabel: string;

  /** Highlighted on top of whatever breaches; a union, not a replacement. */
  selectedKeys?: ReadonlySet<string>;
  height?: number;
}

/**
 * A multi-line time series with a threshold the reader can drag. Lines whose
 * `breachValue` is over it are colored, the rest grey, so moving the handle
 * re-sorts the picture without a query. Color comes from `breachValue`, so the
 * threshold decides only whether a line is drawn in its color.
 *
 * Controlled: the caller owns `threshold`.
 */
export const ThresholdLineGraph = <Datum,>(
  props: ThresholdLineGraphProps<Datum>,
) => {
  return (
    <Box height={`${props.height ?? DEFAULT_HEIGHT_PX}px`} width="100%">
      <ParentSize debounceTime={10}>
        {(parent) => <ThresholdLineGraphInner {...props} {...parent} />}
      </ParentSize>
    </Box>
  );
};

/** Whether the browser would draw its own focus ring, i.e. keyboard focus. */
function isFocusVisible(element: Element) {
  try {
    return element.matches(":focus-visible");
  } catch {
    return false;
  }
}

const ThresholdLineGraphInner = <Datum,>(
  props: ThresholdLineGraphProps<Datum> & ParentSizeProvidedProps,
) => {
  const { colors, fonts } = useTheme<MaterializeTheme>();
  const svgRef = React.useRef<SVGSVGElement>(null);
  const handleRef = React.useRef<SVGGElement>(null);

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

  const lineColors = React.useMemo(() => assignLineColors(lines), [lines]);

  const isHighlighted = React.useCallback(
    (line: ThresholdLineSeries<Datum>) =>
      isBreaching(line, threshold) || (selectedKeys?.has(line.key) ?? false),
    [threshold, selectedKeys],
  );

  // Painted back to front, so the worst breach ends up on top of the grey.
  const orderedLines = [
    ...lines.filter((line) => !isHighlighted(line)),
    ...lines
      .filter(isHighlighted)
      .sort(
        (a, b) =>
          (a.breachValue ?? 0) - (b.breachValue ?? 0) ||
          b.key.localeCompare(a.key),
      ),
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

  useDrag({
    ref: handleRef,
    onStart: () => setIsDragging(true),
    onStop: () => setIsDragging(false),
    onDrag: (event) => {
      if (!svgRef.current) return;
      const point = localPoint(svgRef.current, event);
      if (point) commitThreshold(yScale.invert(point.y));
    },
  });

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
    <svg ref={svgRef} {...svgProps} role="group" aria-label={props.graphLabel}>
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

      <Line
        from={{ x: plot.x, y: thresholdY }}
        to={{ x: plotRight, y: thresholdY }}
        stroke={colors.accent.red}
        strokeWidth={1.5}
        strokeDasharray="5 4"
        pointerEvents="none"
      />
      <Group
        innerRef={handleRef}
        top={thresholdY}
        left={plot.x}
        role="slider"
        tabIndex={0}
        aria-label={props.thresholdLabel}
        aria-orientation="vertical"
        aria-valuenow={threshold}
        aria-valuemin={minThreshold}
        aria-valuemax={maxThreshold}
        aria-valuetext={thresholdText}
        cursor="ns-resize"
        onKeyDown={handleKeyDown}
        onFocus={(event) => setIsFocused(isFocusVisible(event.currentTarget))}
        onBlur={() => setIsFocused(false)}
        style={{ outline: "none" }}
      >
        <rect
          x={0}
          y={-HANDLE_HIT_HEIGHT_PX / 2}
          width={plot.width}
          height={HANDLE_HIT_HEIGHT_PX}
          fill="transparent"
        />
        <rect
          x={plot.width - pillWidth}
          y={-PILL_HEIGHT_PX / 2}
          width={pillWidth}
          height={PILL_HEIGHT_PX}
          rx={PILL_HEIGHT_PX / 2}
          fill={colors.accent.red}
          stroke={isFocused ? colors.foreground.primary : undefined}
          strokeWidth={isFocused ? 2 : 0}
          opacity={isDragging ? 1 : 0.9}
        />
        <text
          x={plot.width - pillWidth / 2}
          y={0}
          dy="0.35em"
          textAnchor="middle"
          fill={colors.foreground.inverse}
          fontFamily={fonts.mono}
          fontSize={12}
          pointerEvents="none"
        >
          {thresholdText}
        </text>
      </Group>
    </svg>
  );
};
