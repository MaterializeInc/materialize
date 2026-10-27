// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useTheme } from "@chakra-ui/react";
import { useAtom } from "jotai";
import React from "react";

import SearchableSelect from "~/components/SearchableSelect/SearchableSelect";
import { cloudRegionsSelector, getRegionId } from "~/store/cloudRegions";
import GlobeIcon from "~/svg/GlobeIcon";
import { MaterializeTheme } from "~/theme";

export type RegionSelectProps = {
  region: string;
  setRegion: (val: string) => void;
};

const RegionSelect = ({ region, setRegion }: RegionSelectProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const [cloudRegions] = useAtom(cloudRegionsSelector);
  const regionFilterOptions: Record<string, string> = {
    all: "All regions",
  };
  for (const [name, regionDetails] of cloudRegions) {
    regionFilterOptions[name] = getRegionId(regionDetails);
  }
  const selectedRegionId = regionFilterOptions[region] ? region : "all";
  return (
    <SearchableSelect
      value={{
        id: selectedRegionId,
        name: regionFilterOptions[selectedRegionId],
      }}
      isSearchable={false}
      onChange={(value) => value && setRegion(value.id)}
      ariaLabel="Select a region"
      options={Object.entries(regionFilterOptions).map(([value, name]) => ({
        name,
        id: value,
      }))}
      leftIcon={<GlobeIcon color={colors.foreground.tertiary} />}
      data-testid="region-select"
    />
  );
};

export default RegionSelect;
