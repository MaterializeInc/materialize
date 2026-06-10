// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useForm } from "react-hook-form";

import { SASL_MECHANISMS } from "~/api/materialize/connection/createKafkaConnection";
import { Schema } from "~/api/materialize/schemaList";
import { formatOptions } from "~/api/materialize/source/createKafkaSourceStatement";
import { ENVELOPE_OPTIONS } from "~/api/materialize/source/createKafkaSourceStatement";
import { SecretOption } from "~/components/SecretSelectionControl";

import { KafkaConnectionFormState } from "./NewKafkaConnectionForm";
import { KafkaSourceFormState } from "./NewKafkaSourceForm";

export function useConnectionForm() {
  return useForm<KafkaConnectionFormState>({
    defaultValues: {
      connectionAction: "existing",
      name: "",
      brokers: [
        {
          hostPort: "",
          availabilityZone: "",
          port: "",
        },
      ],
      authenticationMode: "sasl",
      saslMechanism: { id: "PLAIN", name: SASL_MECHANISMS.PLAIN },
      saslUsername: "",
    },
    mode: "onTouched",
  });
}

export function useSourceForm() {
  return useForm<KafkaSourceFormState>({
    defaultValues: {
      name: "",
      schema: null,
      cluster: null,
      topic: "",
      keyFormat: formatOptions[0],
      valueFormat: formatOptions[0],
      csrConnection: null,
      envelope: ENVELOPE_OPTIONS[0],
      useSchemaRegistry: false,
    },
    mode: "onTouched",
  });
}

export interface CsrConnectionFormState {
  name: string;
  schema: Schema;
  url: string;
  useSsl: boolean;
  username: string;
  password: SecretOption;
  sslCertificateAuthority: SecretOption;
  sslCertificate: SecretOption;
  sslKey: SecretOption;
}

export function useCsrConnectionForm() {
  return useForm<CsrConnectionFormState>({
    defaultValues: {
      name: "",
      url: "",
      useSsl: false,
      username: "",
    },
    mode: "onTouched",
  });
}
