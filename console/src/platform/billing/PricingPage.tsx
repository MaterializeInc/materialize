// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Text, useTheme } from "@chakra-ui/react";
import React from "react";

import IFrameWrapper from "~/components/IFrameWrapper";
import { MainContentContainer } from "~/layouts/BaseLayout";
import { MaterializeTheme } from "~/theme";

const PricingPage = () => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <MainContentContainer width="100%" maxWidth="1400px" mx="auto">
      <Text textStyle="heading-xs" mb="2">
        On-Demand Pricing
      </Text>
      <Text textStyle="text-small" mb="4" color={colors.foreground.secondary}>
        The following represents our on-demand pricing structure for compute,
        storage, and network resources. To calculate your estimated compute
        costs, multiply your projected compute credit usage by the listed price
        per credit.
      </Text>
      {/* IFrameWrapper passes width/height straight through as native
          iframe attributes, not CSS, so this must be a plain pixel number
          (browsers parse legacy iframe dimension attributes leniently,
          silently truncating anything else, e.g. "80vh" to 80 pixels). */}
      <IFrameWrapper
        src="https://materialize.com/pdfs/pricing.pdf"
        title="Materialize Pricing PDF"
        width="100%"
        height="900"
      />
    </MainContentContainer>
  );
};

export default PricingPage;
