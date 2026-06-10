// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { FormState } from "react-hook-form";
import { z } from "zod";

import {
  FinishedStatus,
  QueryHistoryListSchema,
  queryHistoryListSchema,
  WILDCARD_TOKEN,
} from "~/api/materialize/query-history/queryHistoryList";

import { COLUMN_KEYS, ColumnKey, REQUIRED_COLUMNS } from "./useColumns";

export const FINISHED_STATUS_TO_COLOR_SCHEME = {
  success: "green" as const,
  error: "red" as const,
  running: "blue" as const,
  canceled: "gray" as const,
};

export function getFinishedStatusColorScheme(finishedStatus: string) {
  return FINISHED_STATUS_TO_COLOR_SCHEME[finishedStatus as FinishedStatus];
}

export const queryHistoryListUrlSchema = queryHistoryListSchema.extend({
  columns: z.array(z.enum(COLUMN_KEYS)).refine((columns) => {
    const columnSet = new Set(columns);
    return REQUIRED_COLUMNS.every((requiredColumn) =>
      columnSet.has(requiredColumn as ColumnKey),
    );
  }),
});

export const ALL_USERS_OPTION = { name: "All users", id: WILDCARD_TOKEN };

export const FILTER_MENU_FORM_FIELDS = [
  "statementTypes",
  "finishedStatuses",
  "durationRange",
  "sessionId",
  "applicationName",
  "sqlText",
  "executionId",
  "showConsoleIntrospection",
] as const;

function isFormArrayDirty(formArray?: boolean[] | boolean | number) {
  if (Array.isArray(formArray)) {
    return formArray.some((isDirty) => !!isDirty);
  }

  return !!formArray;
}

/**
 *
 * We need to use dirtyFields instead of each individual field's isDirty state
 * because of this issue: https://github.com/react-hook-form/react-hook-form/issues/11690.
 *
 * TODO: Use isDirty directly once the issue is resolved.
 */
export function calculateDirtyState(
  dirtyFields: FormState<QueryHistoryListSchema>["dirtyFields"],
) {
  const isDirtyByField = FILTER_MENU_FORM_FIELDS.reduce(
    (accum, fieldKey) => {
      if (fieldKey === "statementTypes" || fieldKey === "finishedStatuses") {
        accum[fieldKey] = isFormArrayDirty(dirtyFields[fieldKey]);
      } else if (fieldKey === "durationRange") {
        accum[fieldKey] =
          !!dirtyFields.durationRange?.minDuration ||
          !!dirtyFields.durationRange?.maxDuration;
      } else {
        accum[fieldKey] = !!dirtyFields[fieldKey];
      }

      return accum;
    },
    {} as Record<(typeof FILTER_MENU_FORM_FIELDS)[number], boolean>,
  );

  return {
    isDirtyByField,
    numDirtyFields: Object.values(isDirtyByField).filter((isDirty) => !!isDirty)
      .length,
  };
}
