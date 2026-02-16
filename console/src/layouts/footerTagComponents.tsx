// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Tag } from "@chakra-ui/react";
import { useAtom } from "jotai";
import * as React from "react";

import { useMaybeCurrentOrganizationId } from "~/api/auth";
import { CopyButton } from "~/components/copyableComponents";
import { AppConfigSwitch } from "~/config/AppConfigSwitch";
import { currentEnvironmentState } from "~/store/environments";

const VersionTag = ({
  label,
  contents,
  title,
}: {
  label: string;
  contents: string;
  title: string;
}) => {
  return (
    <Tag size="sm" variant="outline" opacity={0.8} fontSize="80%">
      {label}
      <CopyButton
        contents={contents}
        size="xs"
        ml={1}
        title={title}
        height="auto"
      />
    </Tag>
  );
};

const LoadingVersionTag = () => {
  return (
    <Tag size="sm" variant="outline" opacity={0.8}>
      ...
    </Tag>
  );
};

export const OrgTag = () => {
  const maybeOrganizationId = useMaybeCurrentOrganizationId();

  if (maybeOrganizationId === null || maybeOrganizationId.data === undefined) {
    return null;
  }

  const truncatedOrgId = maybeOrganizationId.data.slice(0, 9) + "...";

  return (
    <VersionTag
      label={`Org ID ${truncatedOrgId}`}
      contents={maybeOrganizationId.data}
      title="Copy org ID"
    />
  );
};

export const ConsoleImageTag = () => {
  return (
    <AppConfigSwitch
      selfManagedConfigElement={({ appConfig: { mzConsoleImageTag } }) => {
        if (!mzConsoleImageTag) return null;
        return (
          <VersionTag
            label={mzConsoleImageTag}
            contents={mzConsoleImageTag}
            title="Copy console image ref"
          />
        );
      }}
    />
  );
};

const EnvironmentdImageRefTagContent = () => {
  const [environment] = useAtom(currentEnvironmentState);

  const mzEnvironmentdImageRef =
    environment?.state === "enabled" && environment.status.health === "healthy"
      ? `materialize/environmentd:${environment.status.version.crateVersion.version}`
      : null;

  if (!mzEnvironmentdImageRef) return <LoadingVersionTag />;

  return (
    <VersionTag
      label={mzEnvironmentdImageRef}
      contents={mzEnvironmentdImageRef}
      title="Copy environmentd image ref"
    />
  );
};

export const EnvironmentdImageRefTag = () => {
  return (
    <AppConfigSwitch
      selfManagedConfigElement={() => <EnvironmentdImageRefTagContent />}
    />
  );
};

const HelmChartVersionTagContent = () => {
  const [environment] = useAtom(currentEnvironmentState);

  if (
    environment?.state !== "enabled" ||
    environment.status.health !== "healthy"
  ) {
    return <LoadingVersionTag />;
  }

  // If there is no helm chart version, like in the Emulator, we want to show nothing.
  if (environment.status.version.helmChartVersion === undefined) {
    return null;
  }

  const mzHelmChartVersion = `v${environment.status.version.helmChartVersion.version}`;

  return (
    <VersionTag
      label={`Helm ${mzHelmChartVersion}`}
      contents={mzHelmChartVersion}
      title="Copy helm chart version"
    />
  );
};

export const HelmChartVersionTag = () => {
  return (
    <AppConfigSwitch
      selfManagedConfigElement={() => <HelmChartVersionTagContent />}
    />
  );
};
