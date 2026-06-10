// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { isSameDay } from "date-fns";
import { GroupBase, StylesConfig } from "react-select";

import { IPostgresInterval } from "~/api/materialize";
import { STATEMENT_LIFECYCLE_TABLE } from "~/api/materialize/query-history/queryHistoryDetail";
import {
  DEFAULT_SCHEMA_VALUES,
  QUERY_HISTORY_LIST_TABLE,
  QUERY_HISTORY_LIST_TABLE_REDACTED,
  QueryHistoryListSchema,
} from "~/api/materialize/query-history/queryHistoryList";
import { HasPrivilegeCallback } from "~/hooks/usePrivileges";
import { ThemeColors, ThemeShadows } from "~/theme";
import { decodeArraySearchParam } from "~/util";
import {
  DATE_FORMAT,
  formatBrowserTimezone,
  formatDate,
  TIME_FORMAT_NO_SECONDS,
} from "~/utils/dateFormat";

import { DEFAULT_COLUMNS } from "./useColumns";

/**
 * Maps the search params from the URL to a QueryHistoryFilters state object
 */
export function mapQueryHistorySearchParams(
  searchParams: URLSearchParams,
  currentUserEmail?: string,
) {
  const decodedSearchParamValues: Record<string, unknown> = {
    columns: decodeArraySearchParam(searchParams, "columns[]", DEFAULT_COLUMNS),
    dateRange: decodeArraySearchParam(
      searchParams,
      "date_range[]",
      DEFAULT_SCHEMA_VALUES.dateRange,
    ),
    clusterId: searchParams.get("cluster_id"),
    sessionId: searchParams.get("session_id"),
    user: searchParams.get("user"),
    statementTypes: decodeArraySearchParam(
      searchParams,
      "statement_types[]",
      [],
    ),
    finishedStatuses: decodeArraySearchParam(
      searchParams,
      "finished_statuses[]",
      [],
    ),
    showConsoleIntrospection:
      (searchParams.get("show_console_introspection") ?? "").toLowerCase() ===
      "true",
    applicationName: searchParams.get("application_name"),
    sqlText: searchParams.get("sql_text"),
    executionId: searchParams.get("execution_id"),
    durationRange: {
      minDuration: searchParams.get("duration_range.min_duration"),
      maxDuration: searchParams.get("duration_range.max_duration"),
    },
    sortField:
      searchParams.get("sort_field") ?? DEFAULT_SCHEMA_VALUES.sortField,
    sortOrder:
      searchParams.get("sort_order") ?? DEFAULT_SCHEMA_VALUES.sortOrder,
  };

  if (decodedSearchParamValues.user === null) {
    if (currentUserEmail) {
      decodedSearchParamValues.user = currentUserEmail;
    } else {
      decodedSearchParamValues.user = DEFAULT_SCHEMA_VALUES.user;
    }
  }

  return decodedSearchParamValues;
}

/**
 * Namespaces QueryHistoryFilters state object to URL parameter format
 */
export function formatToURLParamObject({
  columns,
  listFilters,
}: {
  columns: string[];
  listFilters: QueryHistoryListSchema;
}) {
  return {
    "columns[]": columns,
    "date_range[]": listFilters.dateRange,
    cluster_id: listFilters.clusterId,
    session_id: listFilters.sessionId,
    user: listFilters.user,
    application_name: listFilters.applicationName,
    sql_text: listFilters.sqlText,
    execution_id: listFilters.executionId,
    duration_range: {
      min_duration: listFilters.durationRange.minDuration,
      max_duration: listFilters.durationRange.maxDuration,
    },
    "statement_types[]": listFilters.statementTypes,
    "finished_statuses[]": listFilters.finishedStatuses,
    show_console_introspection: listFilters.showConsoleIntrospection,
    sort_field: listFilters.sortField,
    sort_order: listFilters.sortOrder,
  };
}

/**
 * Transforms strings with only whitespace to null.
 *
 * This is useful since React Hook Form registers non-set values as empty strings instead of null
 * which our schema validator rejects.
 */
export function transformEmptyStringToNull(val: unknown) {
  if (typeof val === "string" && val.trim() === "") {
    return null;
  }
  return val;
}

export function formatDuration(
  interval:
    | IPostgresInterval
    | {
        hours: number | null;
        minutes: number | null;
        seconds: number | null;
        milliseconds: number | null;
      }
    | null,
) {
  if (!interval) {
    return "-";
  }

  if (interval.hours) {
    return `${interval.hours}h${interval.minutes ?? 0}m${
      interval.seconds ?? 0
    }s`;
  }

  if (interval.minutes) {
    return `${interval.minutes}m${interval.seconds ?? 0}s`;
  }

  if (interval.seconds) {
    if (!interval.milliseconds) {
      return `${interval.seconds}s`;
    }

    const duration = interval.seconds + interval.milliseconds / 1000;
    return `${duration.toFixed(3)}s`;
  }

  if (interval.milliseconds) {
    return `${interval.milliseconds.toFixed(0)}ms`;
  }

  // We clamp to 1ms to avoid user confusion.
  return "1ms";
}

/**
 *
 * A HOF that returns a function that can be used as a React Hook Form handler
 * for multi-select fields.
 *
 * Fields must be an array of strings and selecting a new string will add that string to the end of the array.
 * Removing a string will remove that string from the array.
 *
 */
export function createMultiSelectReactHookFormHandler<TValue extends string>({
  currentSelectValues = [],
  onChange,
}: {
  currentSelectValues?: TValue[];
  onChange: (...events: any[]) => void;
}) {
  return ({ value, isSelected }: { value: TValue; isSelected: boolean }) => {
    if (isSelected) {
      onChange([...currentSelectValues, value]);
    } else {
      const newFinishedStatuses = currentSelectValues.filter(
        (selectValue) => selectValue !== value,
      );

      onChange(newFinishedStatuses);
    }
  };
}

export type QueryHistorySelectVariant = "error" | "focused" | "default";
/**
 * Used to override react-select's styles.
 * This utility function should be removed when Select styles are unified.
 */
export function getQueryHistorySelectStyleOverrides<
  Option = unknown,
  IsMulti extends boolean = boolean,
  Group extends GroupBase<Option> = GroupBase<Option>,
>({
  shadows,
  colors,
  variant = "default",
}: {
  colors: ThemeColors;
  shadows: ThemeShadows;
  variant?: QueryHistorySelectVariant;
}): StylesConfig<Option, IsMulti, Group> {
  return {
    container: (base, state) => {
      const isError = variant === "error";
      const isFocused = variant === "focused" || state.isFocused;

      return {
        ...base,
        borderColor: isError
          ? colors.accent.red
          : isFocused
            ? colors.accent.brightPurple
            : colors.border.secondary,

        boxShadow: isError
          ? shadows.input.error
          : isFocused
            ? shadows.input.focus
            : "none",
      };
    },
    group: (base) => ({
      ...base,
      paddingBottom: "0px",
    }),
    groupHeading: (base) => ({
      ...base,
      color: colors.foreground.secondary,
      padding: "0 16px",
    }),
  };
}

const ACTIVITY_LOG_REDACTED_SELECT_PRIVILEGE = {
  relation: QUERY_HISTORY_LIST_TABLE_REDACTED,
  privilege: "SELECT" as const,
};

const ACTIVITY_LOG_SELECT_PRIVILEGE = {
  relation: QUERY_HISTORY_LIST_TABLE,
  privilege: "SELECT" as const,
};

const STATEMENT_LIFECYCLE_SELECT_PRIVILEGE = {
  relation: STATEMENT_LIFECYCLE_TABLE,
  privilege: "SELECT" as const,
};

export function isAuthorizedSelector(hasPrivilege: HasPrivilegeCallback) {
  return (
    (hasPrivilege(ACTIVITY_LOG_REDACTED_SELECT_PRIVILEGE) ||
      hasPrivilege(ACTIVITY_LOG_SELECT_PRIVILEGE)) &&
    hasPrivilege(STATEMENT_LIFECYCLE_SELECT_PRIVILEGE)
  );
}

export function shouldShowRedactedSelector(hasPrivilege: HasPrivilegeCallback) {
  return (
    hasPrivilege(ACTIVITY_LOG_REDACTED_SELECT_PRIVILEGE) &&
    !hasPrivilege(ACTIVITY_LOG_SELECT_PRIVILEGE)
  );
}

export function formatSelectedDates(selectedDates: Date[]) {
  let formattedDateString = "-- / --";
  const [startDate, endDate] = selectedDates;

  if (selectedDates.length === 1) {
    formattedDateString = formatDate(startDate, DATE_FORMAT);
  } else if (selectedDates.length === 2 && isSameDay(startDate, endDate)) {
    formattedDateString = `${formatDate(startDate, DATE_FORMAT)} ${formatDate(startDate, TIME_FORMAT_NO_SECONDS)}-${formatDate(endDate, TIME_FORMAT_NO_SECONDS)} (${formatBrowserTimezone()})`;
  } else if (selectedDates.length === 2) {
    formattedDateString = `${formatDate(
      startDate,
      DATE_FORMAT,
    )}-${formatDate(endDate, DATE_FORMAT)}`;
  }

  return formattedDateString;
}
