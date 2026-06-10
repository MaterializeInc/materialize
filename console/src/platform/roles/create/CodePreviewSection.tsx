// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Link,
  Tab,
  TabList,
  TabPanel,
  TabPanels,
  Tabs,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React from "react";

import { CodeBlock } from "~/components/copyableComponents";
import TerraformIcon from "~/svg/TerraformIcon";
import { MaterializeTheme } from "~/theme";

const TERRAFORM_PROVIDER_URL =
  "https://registry.terraform.io/providers/MaterializeInc/materialize/latest/docs";

type CodePreviewSectionProps = {
  sqlCode: string;
  terraformCode: string;
  /** When false, always shows terraform code even if it starts with # (useful for edit mode) */
  showEmptyTerraformState?: boolean;
};

export const CodePreviewSection = ({
  sqlCode,
  terraformCode,
  showEmptyTerraformState = true,
}: CodePreviewSectionProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  const hasTerraformContent = showEmptyTerraformState
    ? terraformCode && !terraformCode.startsWith("#") && terraformCode.trim()
    : terraformCode && terraformCode.trim();

  return (
    <VStack alignItems="flex-start" spacing={4} width="100%" maxWidth="400px">
      <Text textStyle="heading-xs">Equivalent code</Text>

      <Tabs width="100%" variant="line">
        <TabList mb={4}>
          <Tab>Terraform</Tab>
          <Tab>SQL</Tab>
        </TabList>

        <TabPanels>
          <TabPanel px={0}>
            {hasTerraformContent ? (
              <VStack alignItems="stretch" spacing={2}>
                <Link
                  href={TERRAFORM_PROVIDER_URL}
                  textStyle="text-small"
                  color={colors.accent.brightPurple}
                  isExternal
                >
                  View docs
                </Link>
                <CodeBlock
                  title="Terraform"
                  contents={terraformCode}
                  lineNumbers={false}
                />
              </VStack>
            ) : (
              <VStack
                bg={colors.background.secondary}
                borderRadius="md"
                p={6}
                minHeight="200px"
                justify="center"
                spacing={2}
              >
                <TerraformIcon />
                <Text
                  textStyle="text-small"
                  color={colors.foreground.secondary}
                  textAlign="center"
                >
                  Enter a role name to see the Terraform configuration.
                </Text>
                <Link
                  href={TERRAFORM_PROVIDER_URL}
                  textStyle="text-small"
                  color={colors.accent.brightPurple}
                  isExternal
                >
                  Learn more about managing roles with Terraform
                </Link>
              </VStack>
            )}
          </TabPanel>
          <TabPanel px={0}>
            <CodeBlock title="SQL" contents={sqlCode} lineNumbers={false} />
          </TabPanel>
        </TabPanels>
      </Tabs>
    </VStack>
  );
};

export default CodePreviewSection;
