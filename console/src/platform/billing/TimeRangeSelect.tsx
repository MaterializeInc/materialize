// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";

import SearchableSelect from "~/components/SearchableSelect/SearchableSelect";
import { ClockIcon } from "~/icons";

export type TimeRangeSelectProps = {
  timeRange: number;
  setTimeRange: (val: number) => void;
};

const TimeRangeSelect = ({ timeRange, setTimeRange }: TimeRangeSelectProps) => {
  const timeRangeOptions: Record<number, string> = {
    7: "Last 7 days",
    14: "Last 14 days",
    30: "Last 30 days",
    90: "Last 90 days",
    180: "Last 180 days",
  };
  return (
    <SearchableSelect
      value={{
        id: timeRange.toString(),
        name: timeRangeOptions[timeRange],
      }}
      onChange={(value) => value && setTimeRange(parseInt(value.id))}
      isSearchable={false}
      ariaLabel="Select a time range"
      options={Object.entries(timeRangeOptions).map(([value, name]) => ({
        name,
        id: value,
      }))}
      leftIcon={<ClockIcon />}
    />
  );
};

export default TimeRangeSelect;
