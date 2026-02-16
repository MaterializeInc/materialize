// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";

import { ClustersIcon } from "~/icons";
import {
  EmptyListHeader,
  EmptyListHeaderContents,
  EmptyListWrapper,
  IconBox,
  SampleCodeBoxWrapper,
} from "~/layouts/listPageComponents";
import docUrls from "~/mz-doc-urls.json";

import { CodeBlock } from "./copyableComponents";

const CREATE_EXAMPLE = `CREATE INDEX active_customers_geo_idx ON active_customers (geo_id);`;

const IndexListEmptyState = ({ title }: { title: string }) => {
  return (
    <EmptyListWrapper>
      <EmptyListHeader>
        <IconBox type="Missing">
          <ClustersIcon />
        </IconBox>
        <EmptyListHeaderContents
          title={title}
          helpText="Indexes assemble and maintain a query’s results in memory within a cluster, which provides future queries the data they need in a format they can immediately use."
        />
      </EmptyListHeader>
      <SampleCodeBoxWrapper docsUrl={docUrls["/docs/concepts/indexes/"]}>
        <CodeBlock
          lineNumbers
          title="Create an index"
          contents={CREATE_EXAMPLE}
        >
          {CREATE_EXAMPLE}
        </CodeBlock>
      </SampleCodeBoxWrapper>
    </EmptyListWrapper>
  );
};

export default IndexListEmptyState;
