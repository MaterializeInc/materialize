// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Text, useTheme } from "@chakra-ui/react";
import React, { RefObject } from "react";
import {
  useController,
  useFormContext,
  UseFormHandleSubmit,
} from "react-hook-form";

import { QueryHistoryListSchema } from "~/api/materialize/query-history/queryHistoryList";
import SearchableSelect, {
  SelectOptionKind,
} from "~/components/SearchableSelect/SearchableSelect";
import { buildSearchableSelectFilterStyles } from "~/components/SearchableSelect/utils";
import { MaterializeTheme } from "~/theme";

import { useFetchQueryHistoryUsers } from "./queries";
import {
  getQueryHistorySelectStyleOverrides,
  QueryHistorySelectVariant,
} from "./queryHistoryUtils";
import { useReactSelectForceFocus } from "./useReactSelectForceFocus";
import { ALL_USERS_OPTION } from "./utils";

type UserFilterProps = {
  submitForm: ReturnType<UseFormHandleSubmit<QueryHistoryListSchema>>;
  variant?: QueryHistorySelectVariant;
};

const UserFilter = ({ submitForm, variant }: UserFilterProps) => {
  const { colors, shadows } = useTheme<MaterializeTheme>();
  const { data: users, isError, isLoading } = useFetchQueryHistoryUsers();

  const { formState } = useFormContext<QueryHistoryListSchema>();
  const { field: userField } = useController<QueryHistoryListSchema, "user">({
    name: "user",
  });

  const { ref, onMenuOpen } = useReactSelectForceFocus();

  const handleOptionChange = (option: SelectOptionKind | null) => {
    if (!option || !("id" in option)) return;
    userField.onChange(option.id);
    submitForm();
  };

  const defaultSelectedValue =
    formState.defaultValues?.user ?? ALL_USERS_OPTION.id;

  const defaultOptions =
    defaultSelectedValue === ALL_USERS_OPTION.id
      ? [ALL_USERS_OPTION]
      : [
          {
            id: defaultSelectedValue,
            name: defaultSelectedValue,
          },
          ALL_USERS_OPTION,
        ];

  const options =
    users?.reduce((accum, user) => {
      if (user.id !== defaultSelectedValue) {
        accum.push(user);
      }

      return accum;
    }, defaultOptions) ?? defaultOptions;

  const currentValue =
    options?.find((option) => option.id === userField.value) ??
    defaultOptions[0];

  const isDisabled = isError || isLoading;

  return (
    <SearchableSelect<SelectOptionKind, false>
      ariaLabel="User filter"
      isDisabled={isDisabled}
      options={options}
      leftIcon={<Text color={colors.foreground.secondary}>User</Text>}
      value={currentValue}
      onChange={(value) => handleOptionChange(value)}
      onMenuOpen={onMenuOpen}
      ref={ref as RefObject<any>}
      styles={buildSearchableSelectFilterStyles(
        {
          colors,
          shadows,
          containerWidth: "256px",
        },
        getQueryHistorySelectStyleOverrides({ colors, shadows, variant }),
      )}
    />
  );
};

export default UserFilter;
