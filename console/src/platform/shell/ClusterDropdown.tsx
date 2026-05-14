// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { matchSorter } from "match-sorter";
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

/**
 * Rank cluster options against the user's search input using match-sorter's
 * default thresholds (CASE_SENSITIVE_EQUAL > EQUAL > STARTS_WITH >
 * WORD_STARTS_WITH > CONTAINS > ACRONYM > MATCHES). Returns the original
 * `options` reference unchanged when `inputValue` is empty so the menu's
 * first-render order is stable.
 *
 * Exported for unit testing.
 */
export function rankClusters(
  options: ClusterOption[],
  inputValue: string,
): ClusterOption[] {
  if (!inputValue) return options;
  return matchSorter(options, inputValue, { keys: ["name"] });
}

const ClusterDropdown = (props: ClusterDropdownProps) => {
  // Control react-select's input value so we can re-rank options on each
  // keystroke (see `rankClusters` above for the ranking strategy).
  const [inputValue, setInputValue] = React.useState("");

  const rankedOptions = React.useMemo(
    () => rankClusters(props.options, inputValue),
    [props.options, inputValue],
  );

  return (
    <SearchableSelect<{ name: string }>
      label="Cluster"
      ariaLabel="Cluster"
      isDisabled={props.isDisabled}
      placeholder="Select one"
      options={rankedOptions}
      onChange={(value) => value && props.onChange(value.name)}
      value={{ name: props.value }}
      isLoading={props.isLoading}
      containerWidth="280px"
      // Intentionally omit `menuWidth` so the popover auto-sizes to the
      // longest cluster name (falls back to theme default
      // `width: fit-content`, `minWidth: 240px`). The previous hard-coded
      // value truncated long cluster names on Edge/Firefox; Safari was
      // forgiving thanks to `-webkit-line-clamp` quirks.
      inputValue={inputValue}
      onInputChange={(newValue) => setInputValue(newValue)}
      // match-sorter already filtered + ranked the options above, so disable
      // react-select's default substring filter.
      filterOption={null}
    />
  );
};

export default ClusterDropdown;
