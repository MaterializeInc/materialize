// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { ConnectorStatus } from "~/api/materialize/types";
import { ByteUnit, convertBytes } from "~/utils/format";

export type HealthStatus = "healthy" | "unhealthy" | "paused" | "all";

export type ConnectorStatusInfo =
  | { status: string; type: string }
  | { status: string; type: string; snapshotCommitted: boolean | null };

const KB = 1024;
const MB = 1024 * 1024;
const GB = 1024 * 1024 * 1024;
const TB = 1024 * 1024 * 1024 * 1024;

/**
 * Determins if a source or sink is current ingesting the initial snapshot.
 *
 * Note this is always false for sinks, since they don't have this concept.
 */
export const snapshotting = (connector: ConnectorStatusInfo) => {
  const supportsSnapshotting =
    connector.type === "postgres" || connector.type === "kakfa";
  return (
    supportsSnapshotting &&
    "snapshotCommitted" in connector &&
    !connector.snapshotCommitted &&
    connector.status === "running"
  );
};

/**
 * Derives a simple `healthy` | `unhealthy` status from the underlying connector status.
 */
export function connectorHealthStatus(status: ConnectorStatus): HealthStatus {
  switch (status) {
    case "created":
      return "healthy";
    case "starting":
      return "healthy";
    case "running":
      return "healthy";
    case "paused":
      return "paused";
    case "stalled":
      return "unhealthy";
    case "failed":
      return "unhealthy";
    case "dropped":
      return "unhealthy";
  }
}

export function align(
  value: number,
  step: number,
  fn: (value: number) => number,
) {
  step ||= 1.0;
  const inv = 1.0 / step;
  return fn(value * inv) / inv;
}

export function calculateAxisTicks({
  tickCount,
  min,
  max,
}: {
  tickCount: number;
  min: number;
  max: number;
}) {
  const range = max - min;
  const minStepSize = range / (tickCount - 1 || 1);
  const alignedMin = align(min, minStepSize, Math.floor);
  const alignedMax = align(max, minStepSize, Math.ceil);
  // Determine the step factor of the range
  const stepSize = align(
    alignedMax / (tickCount - 1 || 1),
    minStepSize,
    Math.ceil,
  );
  // Build an array with each tick value
  const ticks = Array.from({
    length: tickCount,
  }).map((_, i) => i * stepSize);

  // If the domain is very small or empty, we can end up with duplicate ticks
  return { alignedMin, alignedMax, stepSize, ticks: [...new Set(ticks)] };
}

export const formatValue = (
  value: number | null,
  yAxisUnit: ByteUnit | null,
) => {
  if (value === null) return "-";

  const result = yAxisUnit ? convertBytes(value, yAxisUnit) : value;
  const numberFormatter = Intl.NumberFormat("default", {
    maximumFractionDigits: decimalDigits(result),
  });
  return numberFormatter.format(result);
};

export function largestPossibleByteUnit(bytes: number): ByteUnit {
  if (bytes < KB) return "B";
  if (bytes < MB) return "KB";
  if (bytes < GB) return "MB";
  if (bytes < TB) return "GB";
  return "TB";
}

export function decimalDigits(max: number) {
  if (max === 0) return 0;
  if (max < 0.001) return 5;
  if (max < 0.01) return 4;
  if (max < 0.1) return 3;
  if (max < 1) return 2;
  if (max < 10) return 1;
  return 0;
}

/**
 * Nicely format a numeric axis value
 *
 * @param value - the value to format
 * @param max - the maximum number of decimal digits to format
 * @param maxCols - (optional) the number of digits to constrain the formatted number to. If the number is longer, the format is visually compacted (e.g., 5,000,000 -> 5M)
 */
export function prettyFormatAxisValue(value: number, max: number, maxCols = 0) {
  const minFractionDigits = decimalDigits(max);
  let maxFractionDigits = minFractionDigits;
  let compact = false;
  if (maxCols > 0) {
    const cols = Math.trunc(value).toString().length;
    compact = cols >= maxCols;
    if (compact && maxFractionDigits === 0) {
      maxFractionDigits = 2;
    }
  }
  const formatter = Intl.NumberFormat("default", {
    maximumFractionDigits: maxFractionDigits,
    minimumFractionDigits: minFractionDigits,
    notation: compact ? "compact" : "standard",
  });
  return formatter.format(value);
}
