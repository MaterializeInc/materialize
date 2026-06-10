// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";
import { useLocation, useNavigate } from "react-router-dom";

import { DEFAULT_OPTIONS } from "~/hooks/useTimePeriodSelect";

import SimpleSelect from "./SimpleSelect";

export interface TimePeriodSelectProps {
  timePeriodMinutes: number;
  setTimePeriodMinutes: (val: number) => void;
  options?: Record<string, string>;
}

const TimePeriodSelect = ({
  timePeriodMinutes,
  setTimePeriodMinutes,
  options = DEFAULT_OPTIONS,
}: TimePeriodSelectProps) => {
  const navigate = useNavigate();
  const location = useLocation();
  const setTimePeriod = (timePeriod: string) => {
    const searchParams = new URLSearchParams(location.search);
    searchParams.set("timePeriod", timePeriod);
    navigate(
      location.pathname + "?" + searchParams.toString() + location.hash,
      {
        replace: true,
      },
    );
    setTimePeriodMinutes(parseInt(timePeriod));
  };

  return (
    <SimpleSelect
      value={timePeriodMinutes}
      onChange={(e) => setTimePeriod(e.target.value)}
    >
      {Object.entries(options).map(([value, text]) => (
        <option key={value} value={value}>
          {text}
        </option>
      ))}
    </SimpleSelect>
  );
};

export default TimePeriodSelect;
