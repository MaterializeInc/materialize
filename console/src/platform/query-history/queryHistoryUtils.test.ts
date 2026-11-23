// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { addMinutes, startOfMinute, subMinutes } from "date-fns";
import PostgresInterval from "postgres-interval";

import {
  createMultiSelectReactHookFormHandler,
  formatDuration,
  mapQueryHistorySearchParams,
  queryHistoryPath,
} from "./queryHistoryUtils";
import { queryHistoryListUrlSchema } from "./utils";

type Values = "value1" | "value2" | "value3";

describe("createMultiSelectReactHookFormHandler", () => {
  it("should add a new value to field when a checkbox is selected", () => {
    const onChange = vitest.fn();
    const handler = createMultiSelectReactHookFormHandler({
      currentSelectValues: ["value1", "value2"] as Values[],
      onChange,
    });

    handler({ value: "value3", isSelected: true });

    expect(onChange).toHaveBeenLastCalledWith(["value1", "value2", "value3"]);
  });

  it("should add a new value to field when a checkbox is selected and field is empty", () => {
    const onChange = vitest.fn();
    const handler = createMultiSelectReactHookFormHandler({
      currentSelectValues: [] as Values[],
      onChange,
    });

    handler({ value: "value3", isSelected: true });

    expect(onChange).toHaveBeenLastCalledWith(["value3"]);
  });

  it("should remove a value from field when a checkbox is deselected", () => {
    const onChange = vitest.fn();
    const handler = createMultiSelectReactHookFormHandler({
      currentSelectValues: ["value1", "value2"] as Values[],
      onChange,
    });

    handler({ value: "value2", isSelected: false });

    expect(onChange).toHaveBeenLastCalledWith(["value1"]);
  });

  it("should set the field value to an empty array when all checkboxes are unchecked", () => {
    const onChange = vitest.fn();
    const handler = createMultiSelectReactHookFormHandler({
      currentSelectValues: ["value1"] as Values[],
      onChange,
    });

    handler({ value: "value1", isSelected: false });

    expect(onChange).toHaveBeenLastCalledWith([]);
  });

  it("should do nothing when an invalid value is deselected", () => {
    const onChange = vitest.fn();
    const handler = createMultiSelectReactHookFormHandler({
      currentSelectValues: ["value1"] as Values[],
      onChange,
    });

    handler({ value: "value2", isSelected: false });

    expect(onChange).toHaveBeenLastCalledWith(["value1"]);
  });
});

describe("formatDuration", () => {
  it("Should format seconds with milliseconds correctly", () => {
    const interval = PostgresInterval("00:00:01.001");
    expect(formatDuration(interval)).toBe("1.001s");
  });

  it("Should format seconds without milliseconds correctly", () => {
    const interval = PostgresInterval("00:00:01");
    expect(formatDuration(interval)).toBe("1s");
  });

  it("Should format milliseconds correctly", () => {
    const interval = PostgresInterval("00:00:00.001");
    expect(formatDuration(interval)).toBe("1ms");
  });

  it("Should format nanoseconds when at least 1 second correctly", () => {
    const interval = PostgresInterval("00:00:01.59999");
    expect(formatDuration(interval)).toBe("1.600s");
  });

  it("Should format nanoseconds when less than 1 second correctly", () => {
    const interval = PostgresInterval("00:00:00.59999");
    expect(formatDuration(interval)).toBe("600ms");
  });
});

describe("queryHistoryPath", () => {
  it("links to the list filtered to a cluster's statements from every user in a range, as the list reads its URL", () => {
    // The list only takes a range that starts within its lookback.
    const startDate = startOfMinute(subMinutes(new Date(), 20));
    const endDate = addMinutes(startDate, 3);

    const path = queryHistoryPath("aws-us-east-1", {
      clusterId: "u9",
      dateRange: [startDate, endDate],
      sortField: "duration",
    });
    const url = new URL(path, "https://console.test");

    expect(url.pathname).toBe("/regions/aws-us-east-1/query-history");
    expect(
      queryHistoryListUrlSchema.parse(
        mapQueryHistorySearchParams(url.searchParams, "me@example.com"),
      ),
    ).toMatchObject({
      clusterId: "u9",
      user: "*",
      dateRange: [startDate.toISOString(), endDate.toISOString()],
      sortField: "duration",
      sortOrder: "desc",
    });
  });

  it("links to only the cluster's running statements, oldest first", () => {
    const startDate = startOfMinute(subMinutes(new Date(), 60));
    const path = queryHistoryPath("aws-us-east-1", {
      clusterId: "u9",
      dateRange: [startDate, addMinutes(startDate, 61)],
      finishedStatuses: ["running"],
      sortField: "start_time",
      sortOrder: "asc",
    });
    const url = new URL(path, "https://console.test");

    expect(
      queryHistoryListUrlSchema.parse(
        mapQueryHistorySearchParams(url.searchParams, "me@example.com"),
      ),
    ).toMatchObject({
      clusterId: "u9",
      user: "*",
      finishedStatuses: ["running"],
      sortField: "start_time",
      sortOrder: "asc",
    });
  });
});
