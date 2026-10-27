// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { VStack } from "@chakra-ui/layout";
import {
  Button,
  FormControl,
  FormErrorMessage,
  HStack,
  Input,
  InputProps,
  Popover,
  PopoverContent,
  PopoverTrigger,
  Text,
  useTheme,
} from "@chakra-ui/react";
import { max, min, parse, subHours } from "date-fns";
import React, { ChangeEvent, useCallback, useMemo, useRef } from "react";
import { SubmitHandler, useController, useFormContext } from "react-hook-form";

import {
  MAX_TIME_SPAN_BUFFERED_HOURS,
  QueryHistoryListSchema,
} from "~/api/materialize/query-history/queryHistoryList";
import RangeDatePicker from "~/components/DatePicker/RangeDatePicker";
import { useRangeDatePicker } from "~/components/DatePicker/useRangeDatePicker";
import { DropdownToggleButton } from "~/components/Dropdown/dropdownComponents";
import LabeledInput from "~/components/LabeledInput";
import { ClockIcon } from "~/icons";
import { MaterializeTheme } from "~/theme";
import { isInvalidDate } from "~/util";
import {
  DATE_FORMAT_SHORT,
  formatBrowserTimezone,
  formatDate,
  TIME_FORMAT_NO_SECONDS,
} from "~/utils/dateFormat";

import { formatSelectedDates } from "./queryHistoryUtils";

type DateRangeInputProps = {
  onSubmit: SubmitHandler<QueryHistoryListSchema>;
  toggleButtonProps?: InputProps;
};

export const DateRangeInput = (props: DateRangeInputProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  const initialFocusRef = useRef<HTMLInputElement>(null);

  const { resetField, handleSubmit } = useFormContext<QueryHistoryListSchema>();

  const dateRangeController = useController<
    QueryHistoryListSchema,
    "dateRange"
  >({
    name: "dateRange",
  });

  const fieldError = dateRangeController.fieldState.error;

  const selectedDateStrings = dateRangeController.field.value;

  const selectedDates = useMemo(
    () => selectedDateStrings.map((str) => new Date(str)),
    [selectedDateStrings],
  );

  const onDatesChange = useCallback(
    (dates: Date[]) => {
      const dateStrings = dates.map((date) => date.toISOString());
      dateRangeController.field.onChange(dateStrings);
    },
    [dateRangeController.field],
  );

  const onDatePickerChange = (dates: Date[]) => {
    if (dates.length === 2) {
      const [startDate, endDate] = dates;
      let newEndDate = new Date(endDate);
      newEndDate.setHours(23);
      newEndDate.setMinutes(59);
      newEndDate.setSeconds(59);
      newEndDate.setMilliseconds(999);

      const currentDate = new Date();
      const minStartDate = subHours(currentDate, MAX_TIME_SPAN_BUFFERED_HOURS);
      const newStartDate = max([startDate, minStartDate]);
      newEndDate = min([newEndDate, currentDate]);

      onDatesChange([newStartDate, newEndDate]);
      return;
    }
    onDatesChange(dates);
  };

  const currentDate = new Date();

  const datePickerState = useRangeDatePicker({
    selectedDates,
    onDatesChange: onDatePickerChange,
    maxDate: currentDate,
    minDate: subHours(currentDate, MAX_TIME_SPAN_BUFFERED_HOURS),
  });

  const [startDate, endDate] = selectedDates;

  const shouldDisableTimeInputs = !startDate || !endDate;

  const onStartDateChange = (e: ChangeEvent<HTMLInputElement>) => {
    const val = parse(e.target.value, TIME_FORMAT_NO_SECONDS, startDate);
    /**
     * The change event can contain invalid values such as an empty string
     * when the user inputs the backspace character
     */
    if (isInvalidDate(val)) {
      return;
    }
    onDatesChange([val, endDate]);
  };

  const onEndDateChange = (e: ChangeEvent<HTMLInputElement>) => {
    const val = parse(e.target.value, TIME_FORMAT_NO_SECONDS, endDate);
    if (isInvalidDate(val)) {
      return;
    }
    onDatesChange([startDate, val]);
  };

  const popoverTriggerValue = formatSelectedDates(selectedDates);

  return (
    <Popover
      placement="bottom-start"
      gutter={1}
      initialFocusRef={initialFocusRef}
      variant="dropdown"
    >
      {({ isOpen, onClose }) => {
        const onSubmit = handleSubmit(async (filters) => {
          await props.onSubmit(filters);

          onClose();
        });

        return (
          <>
            <PopoverTrigger>
              <DropdownToggleButton
                inputProps={{
                  "aria-label": "Date range filter",
                  isInvalid: !!fieldError,
                  variant: isOpen ? "focused" : "default",
                  boxShadow: "none",
                  ...props.toggleButtonProps,
                }}
                leftIcon={<ClockIcon />}
              >
                {popoverTriggerValue}
              </DropdownToggleButton>
            </PopoverTrigger>

            <PopoverContent
              motionProps={{
                animate: false,
              }}
              width="240px"
            >
              <RangeDatePicker
                containerProps={{ px: "2", pt: "2", pb: "4" }}
                previousMonthButtonProps={{ transition: "none" }}
                nextMonthButtonProps={{ transition: "none" }}
                datePickerState={datePickerState}
              />
              <FormControl
                as={VStack}
                alignItems="flex-start"
                px="4"
                pt="3"
                pb="4"
                borderTopWidth="1px"
                borderColor={colors.border.secondary}
                spacing="4"
                isInvalid={!!fieldError}
                tabIndex={-1}
              >
                <VStack alignItems="flex-start" width="100%">
                  <Text
                    color={colors.foreground.primary}
                    textStyle="text-ui-med"
                  >
                    Min start time ({formatBrowserTimezone()})
                  </Text>
                  <LabeledInput
                    containerProps={{
                      width: "100%",
                      "aria-invalid": !!fieldError,
                    }}
                    labelContainerProps={{ width: "94px" }}
                    label={
                      <Text textStyle="text-ui-med" lineHeight="4">
                        {startDate
                          ? formatDate(startDate, DATE_FORMAT_SHORT)
                          : "--/--/--"}
                      </Text>
                    }
                    renderInput={(inputStyles) => (
                      <Input
                        {...inputStyles}
                        ref={initialFocusRef}
                        type="time"
                        disabled={shouldDisableTimeInputs}
                        value={
                          startDate
                            ? formatDate(startDate, TIME_FORMAT_NO_SECONDS)
                            : ""
                        }
                        onChange={onStartDateChange}
                      />
                    )}
                  />
                </VStack>

                <VStack alignItems="flex-start" width="100%">
                  <Text
                    color={colors.foreground.primary}
                    textStyle="text-ui-med"
                  >
                    Max start time ({formatBrowserTimezone()})
                  </Text>
                  <LabeledInput
                    containerProps={{
                      width: "100%",
                      "aria-invalid": !!fieldError,
                    }}
                    labelContainerProps={{ width: "94px" }}
                    label={
                      <Text textStyle="text-ui-med" lineHeight="4">
                        {endDate
                          ? formatDate(endDate, DATE_FORMAT_SHORT)
                          : "--/--/--"}
                      </Text>
                    }
                    renderInput={(inputStyles) => (
                      <Input
                        {...inputStyles}
                        type="time"
                        width="100%"
                        _focus={{}}
                        disabled={shouldDisableTimeInputs}
                        value={
                          endDate
                            ? formatDate(endDate, TIME_FORMAT_NO_SECONDS)
                            : ""
                        }
                        onChange={onEndDateChange}
                      />
                    )}
                  />
                  {!!fieldError && (
                    <FormErrorMessage mt="0">
                      {fieldError?.message}
                    </FormErrorMessage>
                  )}
                </VStack>
              </FormControl>
              <HStack
                borderTopWidth="1px"
                borderColor={colors.border.secondary}
                width="100%"
                justifyContent="space-between"
                py="2"
                px="4"
              >
                <Button
                  variant="secondary"
                  transition="none"
                  size="sm"
                  onClick={() => {
                    resetField("dateRange");
                    onSubmit();
                  }}
                >
                  Reset filter
                </Button>
                <Button
                  variant="primary"
                  transition="none"
                  size="sm"
                  onClick={onSubmit}
                >
                  Apply filter
                </Button>
              </HStack>
            </PopoverContent>
          </>
        );
      }}
    </Popover>
  );
};

export default DateRangeInput;
