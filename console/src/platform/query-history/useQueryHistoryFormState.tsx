// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { zodResolver } from "@hookform/resolvers/zod";
import { useCallback, useEffect, useMemo, useState } from "react";
import { useForm } from "react-hook-form";

import {
  DEFAULT_SCHEMA_VALUES,
  QueryHistoryListSchema,
  queryHistoryListSchema,
} from "~/api/materialize/query-history/queryHistoryList";

import { DEFAULT_COLUMNS } from "./useColumns";
import { queryHistoryListUrlSchema } from "./utils";

const useQueryHistoryFormState = ({
  initialFilters,
  currentUserEmail,
}: {
  initialFilters: QueryHistoryListSchema;
  currentUserEmail?: string;
}) => {
  const [listFilters, setListFilters] = useState(initialFilters);

  const defaultValues = useMemo(() => {
    return queryHistoryListUrlSchema.parse({
      ...DEFAULT_SCHEMA_VALUES,
      user: currentUserEmail ?? DEFAULT_SCHEMA_VALUES.user,
      columns: DEFAULT_COLUMNS,
    });
  }, [currentUserEmail]);

  const draftListFiltersForm = useForm({
    resolver: zodResolver(queryHistoryListSchema),
    mode: "onBlur",
    reValidateMode: "onBlur",
    defaultValues: defaultValues,
  });

  const onSubmit = useCallback(
    async (newFilters: QueryHistoryListSchema) => {
      setListFilters(newFilters);
    },
    [setListFilters],
  );

  useEffect(() => {
    /**
     * Used to set the initial filters we want to set the form to. We can't set useForm's defaultValues
     * to initialFilters since we use defaultValues as the source of truth to tell when
     * the form is "dirty" or not. This is also why `keepDefaultValues` is true.
     */
    draftListFiltersForm.reset(initialFilters, {
      keepDefaultValues: true,
    });
  }, [draftListFiltersForm, initialFilters]);

  return {
    // A form to hold the draft values of the filters.
    draftListFiltersForm,
    // Form submit callback that accepts the filters and updates the listFilters state.
    onSubmit,
    // Final filters that are used to fetch the query history list
    listFilters,
  };
};

export default useQueryHistoryFormState;
