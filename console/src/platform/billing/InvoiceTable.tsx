// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Table,
  Tbody,
  Td,
  Text,
  Th,
  Thead,
  Tr,
  useTheme,
} from "@chakra-ui/react";
import React from "react";

import { Invoice, PlanType } from "~/api/cloudGlobalApi";
import TextLink from "~/components/TextLink";
import { MaterializeTheme } from "~/theme";
import { formatDateInUtc } from "~/utils/dateFormat";
import { formatCurrency } from "~/utils/format";

const headerProps = {
  backgroundColor: "unset",
  borderWidth: 0,
  borderBottomWidth: "1px",
  sx: {
    "&:first-of-type": {
      borderLeftWidth: 0,
    },
    "&:last-child:not(:only-child)": {
      borderRightWidth: 0,
    },
  },
};

const InvoiceTable = ({
  invoices,
  planType,
}: {
  invoices: Invoice[];
  planType: PlanType;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const shouldShowTotal = planType !== "capacity";
  return (
    <Table variant="standalone" data-testid="invoice-table">
      <Thead>
        <Tr>
          <Th {...headerProps}>Name</Th>
          <Th {...headerProps}>Date received</Th>
          {shouldShowTotal && (
            <Th {...headerProps} isNumeric>
              Total
            </Th>
          )}
          <Th {...headerProps}></Th>
        </Tr>
      </Thead>
      <Tbody>
        {invoices.map((invoice, i) => {
          const url = invoice.webUrl || invoice.pdfUrl;
          const isDraft = invoice.status === "draft";
          return (
            <Tr key={i}>
              <Td>
                <Text
                  textStyle="text-ui-med"
                  textColor={
                    isDraft
                      ? colors.foreground.secondary
                      : colors.foreground.primary
                  }
                  fontStyle={isDraft ? "italic" : "normal"}
                >
                  {isDraft ? "(Draft)" : invoice.invoiceNumber}
                </Text>
              </Td>
              <Td>
                <Text textStyle="text-ui-med">
                  {formatDateInUtc(new Date(invoice.issueDate), "MMM d, yyyy")}
                </Text>
              </Td>
              {shouldShowTotal && (
                <Td isNumeric>
                  <Text textStyle="text-ui-med">
                    {formatCurrency(parseFloat(invoice.total))}
                  </Text>
                </Td>
              )}
              <Td textAlign="end">
                {url && (
                  <TextLink
                    href={url}
                    py={2}
                    px={2}
                    fontWeight="500"
                    borderRadius={2}
                    sx={{ _hover: { bg: colors.background.secondary } }}
                    isExternal
                  >
                    View invoice →
                  </TextLink>
                )}
              </Td>
            </Tr>
          );
        })}
      </Tbody>
    </Table>
  );
};

export default InvoiceTable;
