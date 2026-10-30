// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { HStack, Text } from "@chakra-ui/react";
import { sort } from "d3";
import React from "react";

import SimpleSelect from "~/components/SimpleSelect";

export interface ReplicaSelectProps {
  replicas: Array<{
    id: string;
    name: string;
    size: string | null;
    /** A dropped replica, offered for its history. */
    isRetired?: boolean;
  }>;
  /** The selected replica's name, or undefined for all replicas. */
  value: string | undefined;
  onChange: (replicaName: string | undefined) => void;
  /** Offers "All replicas", which selects undefined. */
  includeAll?: boolean;
  /** Shown while `value` is undefined, for a choice without "All replicas". */
  placeholder?: string;
}

/** The cluster page's toolbar choice of replica. */
export const ReplicaSelect = ({
  replicas,
  value,
  onChange,
  includeAll = false,
  placeholder,
}: ReplicaSelectProps) => {
  const id = React.useId();
  return (
    <HStack spacing={2}>
      <Text as="label" htmlFor={id} textStyle="text-ui-med">
        Replica
      </Text>
      <SimpleSelect
        id={id}
        value={value ?? ""}
        placeholder={value === undefined ? placeholder : undefined}
        onChange={(event) => onChange(event.target.value || undefined)}
      >
        {includeAll && <option value="">All replicas</option>}
        {sort(
          replicas,
          ({ isRetired }) => Boolean(isRetired),
          ({ name }) => name,
        ).map(({ id: replicaId, name, size, isRetired }) => (
          <option key={replicaId} value={name}>
            {[name, size, isRetired ? "retired" : undefined]
              .filter(Boolean)
              .join(" · ")}
          </option>
        ))}
      </SimpleSelect>
    </HStack>
  );
};
