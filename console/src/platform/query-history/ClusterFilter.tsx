// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useTheme } from "@chakra-ui/react";
import React, { RefObject } from "react";
import { useController, UseFormHandleSubmit } from "react-hook-form";

import { QueryHistoryListSchema } from "~/api/materialize/query-history/queryHistoryList";
import SearchableSelect, {
  SelectOptionKind,
} from "~/components/SearchableSelect/SearchableSelect";
import { buildSearchableSelectFilterStyles } from "~/components/SearchableSelect/utils";
import { ClustersIcon } from "~/icons";
import { MaterializeTheme } from "~/theme";

import { useFetchQueryHistoryClusters } from "./queries";
import { getQueryHistorySelectStyleOverrides } from "./queryHistoryUtils";
import { useReactSelectForceFocus } from "./useReactSelectForceFocus";

// null is used to represent the "All clusters" option since it's the field's default value
const DEFAULT_OPTION = { name: "All clusters", id: null };

type ClusterFilterProps = {
  submitForm: ReturnType<UseFormHandleSubmit<QueryHistoryListSchema>>;
};

const ClusterFilter = ({ submitForm }: ClusterFilterProps) => {
  const { colors, shadows } = useTheme<MaterializeTheme>();

  const { data: clusters, isError, isLoading } = useFetchQueryHistoryClusters();

  const { field: clusterIdField } = useController<
    QueryHistoryListSchema,
    "clusterId"
  >({
    name: "clusterId",
  });

  const { ref, onMenuOpen } = useReactSelectForceFocus();

  const handleOptionChange = (option: SelectOptionKind | null) => {
    if (!option || !("id" in option)) return;
    clusterIdField.onChange(option.id);
    submitForm();
  };

  const options = [DEFAULT_OPTION, ...(clusters?.rows ?? [])];

  const currentValue =
    clusters?.rows.find((cluster) => cluster.id === clusterIdField.value) ??
    DEFAULT_OPTION;

  const isDisabled = isError || isLoading;

  return (
    <SearchableSelect<SelectOptionKind, false>
      ariaLabel="Cluster filter"
      isDisabled={isDisabled}
      options={options}
      leftIcon={
        <ClustersIcon
          height="4"
          width="4"
          color={colors.foreground.secondary}
        />
      }
      value={currentValue}
      onChange={(value) => handleOptionChange(value)}
      onMenuOpen={onMenuOpen}
      ref={ref as RefObject<any>}
      styles={buildSearchableSelectFilterStyles(
        {
          colors,
          shadows,
          containerWidth: "148px",
        },
        getQueryHistorySelectStyleOverrides({ shadows, colors }),
      )}
    />
  );
};

export default ClusterFilter;
