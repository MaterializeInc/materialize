// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { AxisScale } from "@visx/axis";
import { localPoint } from "@visx/event";
import { ScaleInput } from "@visx/scale";
import { bisector, nice, ticks } from "d3";
import {
  differenceInDays,
  differenceInHours,
  differenceInMinutes,
} from "date-fns";
import { MouseEvent, PointerEvent } from "react";

export function niceTicks(start: number, stop: number, desiredCount: number) {
  // We need to calculate the nice values first to ensure that the start and stop values are included.
  const [niceStart, niceStop] = nice(start, stop, desiredCount);
  return ticks(niceStart, niceStop, desiredCount);
}

// This function was copied and then modified from the visx XYchart:
// https://github.com/airbnb/visx/blob/6edbde496fc461094f6b3c60e8ec86c4b720b4f5/packages/visx-xychart/src/utils/findNearestDatumSingleDimension.ts#L7
export function findNearestDatum<
  Scale extends AxisScale,
  Datum extends object,
>({
  scale,
  accessor,
  scaledValue,
  data,
  maxDistance,
}: {
  scale: Scale;
  accessor: (d: Datum) => ScaleInput<Scale>;
  scaledValue: number;
  data: Datum[];
  maxDistance?: number;
}) {
  if (data.length === 0) return null;

  const coercedScale = scale as AxisScale; // broaden type before type guards below

  let nearestDatum: Datum;
  let nearestDatumIndex: number;
  let dataValue: number;
  // if scale has .invert(), convert svg coord to nearest data value
  if ("invert" in coercedScale && typeof coercedScale.invert === "function") {
    const bisect = bisector(accessor).left;
    // find closest data value, then map that to closest datum
    dataValue = Number(coercedScale.invert(scaledValue));
    const index = bisect(data, dataValue);
    // take the two datum nearest this index, and compute which is closer
    const nearestDatum0 = data[index - 1];
    const nearestDatum1 = data[index];
    // Handle a single datapoint
    if (!nearestDatum0) {
      nearestDatum = nearestDatum1;
    } else if (!nearestDatum1) {
      nearestDatum = nearestDatum0;
    } else {
      nearestDatum =
        Math.abs(dataValue - accessor(nearestDatum0)) >
        Math.abs(dataValue - accessor(nearestDatum1))
          ? nearestDatum1
          : nearestDatum0;
    }
    nearestDatumIndex = nearestDatum === nearestDatum0 ? index - 1 : index;
  } else {
    console.warn(
      "[findNearestDatum] encountered incompatible scale type, bailing",
    );
    return null;
  }

  if (nearestDatum == null || nearestDatumIndex == null) return null;

  const distance = Math.abs(Number(accessor(nearestDatum)) - dataValue);

  if (maxDistance && distance > maxDistance) {
    return null;
  }

  return { datum: nearestDatum, index: nearestDatumIndex, distance };
}

export function calculateTooltipPosition({
  event,
}: {
  event: PointerEvent | MouseEvent;
}) {
  const svgPoint = localPoint(event);
  if (!svgPoint) return;
  // We assume the tooltip is anchored to the top left of the pointer
  return { tooltipLeft: svgPoint.x, tooltipTop: svgPoint.y };
}

export function relativeTimeTickFormat(
  value: number | Date,
  startTime: Date,
  endTime: Date = new Date(),
): string {
  const timePeriodMinutes = differenceInMinutes(endTime, startTime);

  const dateValue = new Date(value);

  if (timePeriodMinutes < 12 * 60) {
    return `${differenceInMinutes(endTime, dateValue).toString()}m`;
  }
  if (timePeriodMinutes < 30 * 24 * 60) {
    return `${differenceInHours(endTime, dateValue).toString()}h`;
  }
  return `${differenceInDays(endTime, dateValue).toString()}d`;
}

/**
 *
 * Used to build props related to the layout of the graph.
 *
 * @param width - The width of the graph container
 * @param height - The height of the graph container
 * @param margin - The margin outside the axes
 *
 * @returns
 * Each Props object should correspond to a visx / custom graph component.
 * xScaleRange/yScaleRange assume SVG Coordinates (left to right, top to bottom)
 */
export function buildXYGraphLayoutProps({
  width,
  height,
  margin,
}: {
  width: number;
  height: number;
  margin: { top: number; right: number; bottom: number; left: number };
}) {
  const innerGraphWidth = width - margin.left - margin.right;
  const innerGraphHeight = height - margin.top - margin.bottom;

  // Uses the SVG coordinate system (ascending top to bottom)
  const innerGraphTop = margin.top;

  const innerGraphLeft = margin.left;
  const innerGraphRight = width - margin.right;

  const bottomAxisTop = innerGraphTop + innerGraphHeight;

  // Map from the left of the x-axis to the right of the x-axis
  const xScaleRange = [innerGraphLeft, innerGraphRight];
  // Map from the bottom of the y-axis to the top of the y-axis
  const yScaleRange = [bottomAxisTop, innerGraphTop];

  return {
    svgProps: {
      width,
      height,
    },
    gridRowsProps: {
      left: innerGraphLeft,
      width: innerGraphWidth,
    },
    axisLeftProps: {
      left: innerGraphLeft,
    },
    axisBottomProps: {
      top: bottomAxisTop,
    },
    graphEventOverlayProps: {
      x: innerGraphLeft,
      y: innerGraphTop,
      width: innerGraphWidth,
      height: innerGraphHeight,
    },
    graphLineCursorProps: {
      graphTop: innerGraphTop,
      graphBottom: innerGraphTop + innerGraphHeight,
    },
    xScaleRange,
    yScaleRange,
  };
}
