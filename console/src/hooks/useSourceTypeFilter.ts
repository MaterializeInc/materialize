// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";
import { GroupBase } from "react-select";

import { useQueryStringState } from "~/useQueryString";

export interface SourceTypeFilterProps {
  selectedType: SourceTypeOption | undefined;
  setSelectedType: (type: string) => void;
}

const SOURCE_TYPES = [
  { id: "kafka", name: "Kafka" },
  { id: "postgres", name: "Postgres" },
  { id: "mysql", name: "MySQL" },
  { id: "webhook", name: "Webhook" },
  { id: "load-generator", name: "Load generator" },
];

export const SOURCE_TYPE_OPTIONS: GroupBase<SourceTypeOption>[] = [
  {
    label: "Filter by source type",
    options: [{ id: "all", name: "All types" }, ...SOURCE_TYPES],
  },
];

export type SourceTypeOption = {
  id: string;
  name: string;
};

export function useSourceTypeFilter() {
  const [sourceTypeId, setSourceType] = useQueryStringState("sourceType");
  const selectedType = React.useMemo(() => {
    return SOURCE_TYPE_OPTIONS[0].options.find(
      (option) => option.id === sourceTypeId,
    );
  }, [sourceTypeId]);
  const setSelectedType = React.useCallback(
    (id: string) => {
      const selected = SOURCE_TYPES.find((type) => type.id === id);

      setSourceType(selected ? selected.id : undefined);
    },
    [setSourceType],
  );

  return { selectedType, setSelectedType };
}

export type SourceTypeFilterState = ReturnType<typeof useSourceTypeFilter>;
