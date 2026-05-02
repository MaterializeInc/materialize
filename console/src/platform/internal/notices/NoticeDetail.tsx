// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Heading, Spinner, Text, useTheme, VStack } from "@chakra-ui/react";
import React from "react";
import { useParams } from "react-router-dom";

import { CodeBlock } from "~/components/copyableComponents";
import ErrorBox from "~/components/ErrorBox";
import {
  Breadcrumb,
  MainContentContainer,
  PageBreadcrumbs,
  PageHeader,
} from "~/layouts/BaseLayout";
import { MaterializeTheme } from "~/theme";
import {
  formatDate,
  FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
} from "~/utils/dateFormat";

import { useNoticeDetailPage } from "./queries";

type DetailHeadingParams = {
  text: string;
};

const DetailHeading = ({ text }: DetailHeadingParams) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Heading
      marginTop="1em"
      fontSize="md"
      fontWeight={500}
      color={colors.foreground.secondary}
    >
      {text}
    </Heading>
  );
};

type NoticeActionParams = {
  action: string | null;
  action_type: string | null;
  objectName: string | null;
  schemaName: string | null;
  databaseName: string | null;
};

const NoticeAction = ({
  action,
  action_type,
  objectName,
  schemaName,
  databaseName,
}: NoticeActionParams) => {
  if (!action) {
    return null;
  }

  if (action_type === "sql_statements") {
    const text =
      databaseName && schemaName && objectName
        ? `Execute the SQL snippet given below and then recreate \`${databaseName}.${schemaName}.${objectName}\`.`
        : "Execute the SQL snippet given below";

    return (
      <>
        <DetailHeading text="Corrective action" />
        <Text fontSize="md">{text}</Text>
        <CodeBlock
          marginTop="1em"
          lineNumbers
          title="SQL snippet"
          contents={action ?? ""}
        >
          {action}
        </CodeBlock>
      </>
    );
  }

  return (
    <>
      <DetailHeading text="Corrective action" />
      <Text fontSize="md">{action}</Text>
    </>
  );
};

export type NoticeDetailParams = {
  noticeKey: string;
};

const NoticeDetail = () => {
  const { colors } = useTheme<MaterializeTheme>();

  const params = useParams<NoticeDetailParams>();
  const { data, isLoading, isError } = useNoticeDetailPage({
    key: params.noticeKey ?? "",
  });

  const notice = data?.rows[0];
  const isUndefined = notice === undefined;

  const breadcrumbs: Breadcrumb[] = React.useMemo(
    () => [
      { title: "Notices", href: ".." },
      { title: notice?.noticeType ?? "unknown notice", href: "." },
    ],
    [notice?.noticeType],
  );

  return (
    <>
      <PageHeader variant="compact" boxProps={{ mb: 0 }}>
        <VStack spacing={0} alignItems="flex-start" width="100%">
          <PageBreadcrumbs crumbs={breadcrumbs} />
        </VStack>
      </PageHeader>
      {isError ? (
        <ErrorBox message="An error occurred loading notices" />
      ) : isLoading ? (
        <MainContentContainer>
          <Spinner data-testid="loading-spinner" />
        </MainContentContainer>
      ) : isUndefined ? (
        <MainContentContainer>
          <ErrorBox message="A notice with this key was not found" />
        </MainContentContainer>
      ) : (
        <MainContentContainer>
          <Heading
            marginTop="1em"
            fontSize="lg"
            fontWeight={500}
            color={colors.foreground.primary}
          >
            {notice.noticeType}
          </Heading>

          <DetailHeading text="Notice" />
          <Text fontSize="md">{notice.message}</Text>

          <DetailHeading text="Last updated" />
          <Text>
            {formatDate(notice.createdAt, FRIENDLY_DATETIME_FORMAT_NO_SECONDS)}
          </Text>

          <DetailHeading text="Hint" />
          <Text fontSize="md">{notice.hint}</Text>

          <NoticeAction
            action={notice.action}
            action_type={notice.actionType}
            objectName={notice.objectName}
            schemaName={notice.schemaName}
            databaseName={notice.databaseName}
          />
        </MainContentContainer>
      )}
    </>
  );
};

export default NoticeDetail;
