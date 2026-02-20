// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";

import SearchableSelect from "~/components/SearchableSelect/SearchableSelect";

type ClusterOption = { name: string };

export type ClusterDropdownProps = {
  onChange: (clusterName: string) => void;
  options: ClusterOption[];
  value: string;
  isDisabled: boolean;
  isLoading: boolean;
};

const ClusterDropdown = (props: ClusterDropdownProps) => {
  return (
    <SearchableSelect<{ name: string }>
      label="Cluster"
      ariaLabel="Cluster"
      isDisabled={props.isDisabled}
      placeholder="Select one"
      options={props.options}
      onChange={(value) => value && props.onChange(value.name)}
      value={{ name: props.value }}
      isLoading={props.isLoading}
      containerWidth="280px"
      menuWidth="280px"
    />
  );
};

export default ClusterDropdown;
