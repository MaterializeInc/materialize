// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useDatePicker } from "@rehookify/datepicker";

/**
 *
 * @param selectedDates: A list of dates of length 2 that are currently selected.
 *  The first element is always the earliest date.
 * @param onDatesChange: A callback that will be called when the user selects a date
 */
export const useRangeDatePicker = ({
  selectedDates,
  onDatesChange,
  maxDate,
  minDate,
}: {
  selectedDates: Date[];
  onDatesChange: (d: Date[]) => void;
  maxDate: Date;
  minDate: Date;
}) => {
  const datePickerState = useDatePicker({
    selectedDates,
    onDatesChange,
    calendar: {
      mode: "fluid",
    },
    dates: {
      mode: "range",
      selectSameDate: true,
      maxDate,
      minDate,
    },
  });

  return datePickerState;
};
