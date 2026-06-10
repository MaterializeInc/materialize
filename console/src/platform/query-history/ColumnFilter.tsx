// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";

import CheckedMultiSelect from "~/components/Dropdown/CheckedMultiSelect";
import ColumnIcon from "~/svg/ColumnIcon";

import {
  COLUMN_FILTER_ITEMS,
  ColumnItem,
  UseColumnsReturn,
} from "./useColumns";

type ColumnFilterProps = {
  selectedColumnFilterItems: ColumnItem[];
  onColumnChange: UseColumnsReturn["onColumnChange"];
};

const ColumnFilter = ({
  selectedColumnFilterItems,
  onColumnChange,
}: ColumnFilterProps) => {
  return (
    <CheckedMultiSelect
      items={COLUMN_FILTER_ITEMS}
      selectedItems={selectedColumnFilterItems}
      onSelectedItemChange={onColumnChange}
      leftIcon={<ColumnIcon />}
      toggleButtonContent="Columns"
      toggleButtonProps={{
        boxShadow: "none",
      }}
      getItemLabel={(item) => item?.label}
    />
  );
};

export default ColumnFilter;
