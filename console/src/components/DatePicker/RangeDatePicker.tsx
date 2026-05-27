// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { IconButton } from "@chakra-ui/button";
import { Grid, GridItem, HStack, VStack } from "@chakra-ui/layout";
import { ButtonProps, StackProps, Text, useTheme } from "@chakra-ui/react";
import { DPDay, useDatePicker } from "@rehookify/datepicker";
import React from "react";

import ChevronLeftIcon from "~/svg/ChevronLeftIcon";
import ChevronRightIcon from "~/svg/ChevronRightIcon";
import { MaterializeTheme } from "~/theme";

import CalendarCell, { Variant } from "./CalendarCell";

function getCalendarCellVariant({
  currentDay,
  currentDayIndex,
  selectedDates,
  days,
}: {
  currentDay: DPDay;
  currentDayIndex: number;
  selectedDates: Date[];
  days: DPDay[];
}): Variant {
  const isAnchorPoint = selectedDates.length === 1 && currentDay.selected;

  if (isAnchorPoint) {
    const prevNeighbor = days[currentDayIndex - 1];
    const nextNeighbor = days[currentDayIndex + 1];

    if (
      prevNeighbor &&
      (prevNeighbor.range === "will-be-in-range" ||
        prevNeighbor.range === "will-be-range-start")
    ) {
      return "anchorEnd";
    }
    if (
      nextNeighbor &&
      (nextNeighbor.range === "will-be-in-range" ||
        nextNeighbor.range === "will-be-range-end")
    ) {
      return "anchorStart";
    }
    return "anchorSingle";
  }

  if (
    currentDay.range === "will-be-range-start" ||
    currentDay.range === "range-start"
  ) {
    return "selectedStart";
  }

  if (
    currentDay.range === "will-be-range-end" ||
    currentDay.range === "range-end"
  ) {
    return "selectedEnd";
  }

  if (currentDay.range === "range-start range-end") {
    return "selectedSingle";
  }

  if (
    currentDay.range === "will-be-in-range" ||
    currentDay.range === "in-range"
  ) {
    return "withinRange";
  }

  return "default";
}

function stripLeadingZeroes(str: string) {
  return str.replace(/^0+/, "");
}

type DatePickerState = ReturnType<typeof useDatePicker>;

type RangeDatePickerProps = {
  containerProps: StackProps;
  datePickerState: DatePickerState;
  previousMonthButtonProps?: ButtonProps;
  nextMonthButtonProps?: ButtonProps;
};

/**
 *
 * Date picker component that allows a user to select a range of days.
 * Uses the `@rehookify/datepicker` library under the hood.
 *
 * We should use the results of useRangeDatePicker for `data` and `propGetters`.
 */
const RangeDatePicker = ({
  containerProps,
  previousMonthButtonProps,
  nextMonthButtonProps,
  datePickerState: {
    data,
    propGetters: { dayButton, addOffset, subtractOffset },
  },
}: RangeDatePickerProps) => {
  const { calendars, weekDays, selectedDates } = data;

  const { month, days, year } = calendars[0];

  const { colors } = useTheme<MaterializeTheme>();

  return (
    <VStack spacing="0" alignItems="stretch" {...containerProps}>
      <HStack py="2">
        <IconButton
          aria-label="Previous month"
          {...subtractOffset({ months: 1 })}
          icon={<ChevronLeftIcon h="4" w="4" />}
          size="xs"
          variant="borderless"
          {...previousMonthButtonProps}
        />
        <Text flexGrow="1" textAlign="center" textStyle="text-ui-med">
          {month} {year}
        </Text>
        <IconButton
          aria-label="Next month"
          {...addOffset({ months: 1 })}
          icon={<ChevronRightIcon h="4" w="4" />}
          size="xs"
          variant="borderless"
          {...nextMonthButtonProps}
        />
      </HStack>
      <VStack>
        <Grid templateColumns="repeat(7, minmax(0,1fr))">
          {weekDays.map((weekDay) => (
            <GridItem key={weekDay} textAlign="center" px="6px">
              <Text
                key={weekDay}
                textStyle="text-small"
                color={colors.foreground.secondary}
                w="5"
              >
                {weekDay.slice(0, 2)}
              </Text>
            </GridItem>
          ))}
        </Grid>

        <Grid templateColumns="repeat(7,  1fr)" rowGap="1" pt="1">
          {days.map((day, dayIndex) => {
            const dayButtonProps = dayButton(day);

            const variant = getCalendarCellVariant({
              currentDay: day,
              currentDayIndex: dayIndex,
              selectedDates,
              days,
            });

            return (
              <CalendarCell
                key={day.$date.toString()}
                buttonProps={{
                  ...dayButtonProps,
                  "aria-label": day.$date.toString(),
                }}
                variant={variant}
              >
                {stripLeadingZeroes(day.day)}
              </CalendarCell>
            );
          })}
        </Grid>
      </VStack>
    </VStack>
  );
};

export default RangeDatePicker;
