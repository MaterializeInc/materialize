// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";

import storageAvailable from "~/utils/storageAvailable";

export type TimePeriodOptions = Record<string, string>;

export const TIME_PERIOD_SEARCH_PARAM_KEY = "timePeriod";

export const DEFAULT_OPTIONS = {
  "60": "Last hour" as const,
  "180": "Last 3 hours" as const,
  "360": "Last 6 hours" as const,
  "1440": "Last 24 hours" as const,
  "4320": "Last 3 days" as const,
  "10080": "Last 7 days" as const,
  "20160": "Last 14 days" as const,
  "43200": "Last 30 days" as const,
};

export const DEFAULT_OPTION_VALUES = Object.values(DEFAULT_OPTIONS);

export const DEFAULT_TIME_PERIOD = Object.keys(DEFAULT_OPTIONS)[0];

export const useTimePeriodMinutes = ({
  localStorageKey,
  defaultValue = DEFAULT_TIME_PERIOD,
  timePeriodOptions = DEFAULT_OPTIONS,
}: {
  localStorageKey: string;
  defaultValue?: string;
  timePeriodOptions?: TimePeriodOptions;
}) => {
  const [timePeriodMinutes, setTimePeriodMinutes] = React.useState(() => {
    let value = defaultValue;
    if (storageAvailable("localStorage")) {
      value = window.localStorage.getItem(localStorageKey) ?? defaultValue;
    }
    return timePeriodFromUrl(value, timePeriodOptions);
  });

  React.useEffect(() => {
    if (storageAvailable("localStorage")) {
      window.localStorage.setItem(
        localStorageKey,
        timePeriodMinutes.toString(),
      );
    }
  }, [timePeriodMinutes, localStorageKey]);

  return [timePeriodMinutes, setTimePeriodMinutes] as const;
};

const parseTimePeriod = (
  periodString: string | null,
  defaultValue: string,
  timePeriodOptions: TimePeriodOptions,
) => {
  const period =
    periodString && Object.keys(timePeriodOptions).includes(periodString)
      ? periodString
      : defaultValue;
  return parseInt(period);
};

const timePeriodFromUrl = (
  defaultValue: string,
  timePeriodOptions: TimePeriodOptions,
) => {
  const params = new URLSearchParams(window.location.search);
  const timePeriodParam = params.get(TIME_PERIOD_SEARCH_PARAM_KEY);
  return parseTimePeriod(timePeriodParam, defaultValue, timePeriodOptions);
};
