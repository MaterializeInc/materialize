// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { IPostgresInterval } from "~/api/materialize";
import { LagInfo } from "~/api/materialize/cluster/materializationLag";
import { ConnectorStatus } from "~/api/materialize/types";
import { WorkflowGraphNode } from "~/api/materialize/workflowGraphNodes";
import colors from "~/theme/colors";
import { kebabToTitleCase } from "~/util";

/**
 * Sums up the total number of hours from a posgres interval.
 */
export function calculateHoursFromInterval(interval: IPostgresInterval) {
  const years = interval?.years ?? 0;
  const months = interval?.months ?? 0;
  const days = interval?.days ?? 0;
  let hours = interval?.hours ?? 0;
  if (years >= 1) hours += years * 365 * 24;
  if (months >= 1) hours += months * 30 * 24;
  if (days >= 1) hours += days * 24;
  return hours;
}

/**
 * Given a postgres interval, formats the duration in the largest units possible,
 * hours, days or seconds. The value is floored, not rounded.
 *
 * e.g. `20h` or `12m` or `42s`
 */
export function formatLagInfoSimple(
  lagInfo: LagInfo | null | undefined,
): string {
  const { lag, hydrated } = lagInfo ?? {};
  if (hydrated === false) {
    return "Not hydrated";
  }
  if (!lag) return "-";
  const hours = calculateHoursFromInterval(lag);
  const minutes = lag?.minutes ?? 0;
  const seconds = lag?.seconds ?? 0;
  let formattedString = "";
  if (hours >= 1) {
    formattedString = `${hours}h`;
  } else if (minutes >= 1) {
    formattedString = `${minutes}m`;
  } else if (seconds >= 1) {
    formattedString = `${seconds}s`;
  } else {
    formattedString = "Less than 1s";
  }
  return formattedString.trim();
}
/**
 * Given a postgres interval, formats the duration as hours minutes and seconds.
 *
 * e.g. `20h 12m 42s`
 */
export function formatLagInfoDetailed(
  lagInfo: LagInfo | null | undefined,
): string {
  const { lag, hydrated } = lagInfo ?? {};
  if (hydrated === false) {
    return "Not hydrated";
  }

  if (!lag) return "-";
  const hours = calculateHoursFromInterval(lag);
  const minutes = lag?.minutes ?? 0;
  const seconds = lag?.seconds ?? 0;
  let formattedString = "";
  if (hours > 0) {
    formattedString += `${hours}h `;
  }
  if (minutes > 0) {
    formattedString += `${minutes}m `;
  }
  if (seconds >= 1) {
    formattedString += `${seconds}s`;
  }
  if (formattedString === "") {
    return "Less than 1s";
  }

  return formattedString.trim();
}

export function formatTableLagInfo(lagInfo: LagInfo | null | undefined) {
  return formatLagInfoDetailed(lagInfo);
}

export const sourceStatusToColor = (status: ConnectorStatus): string => {
  switch (status) {
    case "created":
    case "starting":
    case "paused":
    case "stalled":
      return colors.yellow[500];
    case "failed":
    case "dropped":
      return colors.orange[350];
    case "running":
      return colors.green[450];
  }
};

export const humanizeSourceStatus = (status: ConnectorStatus): string => {
  switch (status) {
    case "created":
      return "Created";
    case "starting":
      return "Starting";
    case "paused":
      return "Paused";
    case "stalled":
      return "Stalled";
    case "failed":
      return "Failed";
    case "dropped":
      return "Dropped";
    case "running":
      return "Running";
  }
};

export const LAG_STATUS_TO_COLOR_SCHEME: Record<
  string,
  { pillColor: string; nodeColor: string }
> = {
  LAGGING: {
    pillColor: colors.orange[350],
    nodeColor: colors.orange[350],
  },
  NOT_HYDRATED: {
    pillColor: colors.yellow[500],
    nodeColor: colors.yellow[500],
  },
  UP_TO_DATE: {
    pillColor: colors.green[500],
    nodeColor: colors.green[450],
  },
};

export function getLagInfoColorScheme(lagInfo: LagInfo) {
  if (lagInfo.hydrated === false) {
    return LAG_STATUS_TO_COLOR_SCHEME.NOT_HYDRATED;
  }

  if (lagInfo.isOutdated) {
    return LAG_STATUS_TO_COLOR_SCHEME.LAGGING;
  }

  return LAG_STATUS_TO_COLOR_SCHEME.UP_TO_DATE;
}

export function formatObjectType(node: WorkflowGraphNode) {
  if (node.sourceType === "subsource") {
    return "Subsource";
  }

  if (node.sourceType) {
    return `${kebabToTitleCase(node.sourceType)} Source`;
  }
  return kebabToTitleCase(node.type);
}
