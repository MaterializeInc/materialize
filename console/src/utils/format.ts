// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { IPostgresInterval } from "postgres-interval";

import { pluralize } from "~/util";

const kilobyte = 1024n;
const megabyte = 1024n * 1024n;
const gigabyte = 1024n * 1024n * 1024n;
const terabyte = 1024n * 1024n * 1024n * 1024n;

export type ByteUnit = "B" | "KB" | "MB" | "GB" | "TB";

const MAX_LAG_MS = 86_400_000;

export function humanizeBytes(bytes: bigint): {
  value: number;
  unit: ByteUnit;
} {
  if (bytes < kilobyte) {
    return { value: Number(bytes), unit: "B" };
  } else if (bytes < megabyte) {
    return { value: Number(bytes) / Number(kilobyte), unit: "KB" };
  } else if (bytes < gigabyte) {
    return { value: Number(bytes) / Number(megabyte), unit: "MB" };
  } else if (bytes < terabyte) {
    return { value: Number(bytes) / Number(gigabyte), unit: "GB" };
  } else {
    return { value: Number(bytes) / Number(terabyte), unit: "TB" };
  }
}

export const formatBytesShort = (bytes: bigint) => {
  const { value, unit } = humanizeBytes(bytes);
  const numberFormatter = Intl.NumberFormat("default", {
    maximumFractionDigits: value % 1 === 0 ? 0 : 2,
  });
  return `${numberFormatter.format(value)} ${unit}`;
};

export function convertBytes(bytes: number, unit: ByteUnit) {
  switch (unit) {
    case "B":
      return bytes;
    case "KB":
      return bytes / Number(kilobyte);
    case "MB":
      return bytes / Number(megabyte);
    case "GB":
      return bytes / Number(gigabyte);
    case "TB":
      return bytes / Number(terabyte);
  }
}

export function formatMemoryUsage(
  memoryStats:
    | {
        size: bigint | number | null;
        memoryPercentage?: number | null;
      }
    | undefined,
) {
  if (
    !memoryStats ||
    memoryStats.size === null ||
    memoryStats.memoryPercentage === null
  )
    return "-";
  const { size, memoryPercentage } = memoryStats;
  const { value, unit } = humanizeBytes(BigInt(size));

  const formattedMemoryUsage = `${value.toFixed(2)} ${unit}`;
  if (!memoryPercentage) {
    return formattedMemoryUsage;
  }
  const formattedPercentage =
    memoryPercentage < 0.1 ? "< 0.1%" : `${memoryPercentage.toFixed(1)}%`;

  return `${formattedMemoryUsage} (${formattedPercentage})`;
}

// For some reason TS doesn't ship with up-to-date type definitions [1]. Do the
// hacky thing and handjam them in.
//
// [1]: https://github.com/microsoft/TypeScript/issues/52072
interface MissingNumberFormatOptions {
  trailingZeroDisplay?: "auto" | "stripIfInteger";
}

/**
 * Format a number as USD.
 */
export function formatCurrency(
  amount: number,
  additionalOptions: Intl.NumberFormatOptions & MissingNumberFormatOptions = {},
): string {
  const formatter = Intl.NumberFormat("default", {
    style: "currency",
    currency: "USD",
    ...additionalOptions,
  });
  return formatter.format(amount);
}

function roundIntervalValue(
  valueToRound: number,
  smallerUnitValue: number | undefined,
  threshold: number,
) {
  if (smallerUnitValue && smallerUnitValue >= threshold) {
    return ++valueToRound;
  }
  return valueToRound;
}

export function formatIntervalShort(interval: IPostgresInterval) {
  if (interval.years) {
    const years = roundIntervalValue(interval.years, interval.months, 6);
    return `${years} ${pluralize(years, "year", "years")}`;
  }
  if (interval.months) {
    const months = roundIntervalValue(interval.months, interval.days, 15);
    return `${months} ${pluralize(months, "month", "months")}`;
  }
  if (interval.days) {
    const days = roundIntervalValue(interval.days, interval.hours, 12);
    return `${days} ${pluralize(days, "day", "days")}`;
  }
  if (interval.hours) {
    const hours = roundIntervalValue(interval.hours, interval.minutes, 30);
    return `${hours} ${pluralize(hours, "hour", "hours")}`;
  }
  if (interval.minutes) {
    const minutes = roundIntervalValue(interval.minutes, interval.seconds, 30);
    return `${minutes} ${pluralize(minutes, "minute", "minutes")}`;
  }
  if (interval.seconds) {
    const seconds = roundIntervalValue(
      interval.seconds,
      interval.milliseconds,
      500,
    );
    return `${seconds} ${pluralize(seconds, "second", "seconds")}`;
  }
  return "< 1 second";
}

export function formatInterval(interval: IPostgresInterval) {
  const parts: string[] = [];

  if (interval.years) {
    parts.push(`${interval.years}y`);
  }
  if (interval.months) {
    parts.push(`${interval.months}m`);
  }
  if (interval.days) {
    parts.push(`${interval.days}d`);
  }
  if (interval.hours) {
    parts.push(`${interval.hours}h`);
  }
  if (interval.minutes) {
    parts.push(`${interval.minutes}m`);
  }
  if (interval.seconds || interval.milliseconds) {
    parts.push(
      `${interval.seconds || 0}.${(interval.milliseconds || 0).toString().padStart(3, "0")}s`,
    );
  }

  if (parts.length === 0) {
    return "0s";
  }

  return parts.join(" ");
}

export function formatDurationForAxis(durationMs: number): string {
  // Cap at 24 hours (86,400,000 ms)
  const cappedMs = Math.min(durationMs, MAX_LAG_MS);

  if (cappedMs < 1000) {
    return `${Math.round(cappedMs)}ms`;
  } else if (cappedMs < 60000) {
    return `${cappedMs / 1000}s`;
  } else if (cappedMs < 3600000) {
    return `${Math.round(cappedMs / 60000)}m`;
  } else {
    const hours = cappedMs / 3600000;
    return `${hours.toFixed(1)}h`;
  }
}
