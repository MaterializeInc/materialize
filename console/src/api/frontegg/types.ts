// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

export interface UserApiToken {
  type: "personal";
  clientId: string;
  createdAt: string;
  description: string;
  metadata: Record<string, string>;
}

export interface NewUserApiToken extends UserApiToken {
  secret: string;
}

export interface TenantApiToken {
  type: "service";
  clientId: string;
  createdAt: string;
  description: string;
  metadata: Record<string, string>;
  user: string;
  roleIds: string[];
}

export interface NewTenantApiToken extends TenantApiToken {
  secret: string;
}

export type ApiToken = UserApiToken | TenantApiToken;
export type NewApiToken = NewUserApiToken | NewTenantApiToken;
export interface Tenant {
  _id: string;
  vendorId: string;
  tenantId: string;
  name: string;
  deletedAt: null;
  metadata: string;
  isReseller: boolean;
  creatorEmail: string;
  creatorName: string;
  id: string;
  createdAt: Date;
  updatedAt: Date;
  __v: number;
}
export interface NoMfaPolicy {
  enforceMFAType: "NotSet";
}

export interface MfaPolicy {
  enforceMFAType: "Force" | "DontForce" | "ForceExceptSAML";
  createdAt: string;
  updatedAt: string;
  id: string;
  allowRememberMyDevice: boolean;
  mfaDeviceExpiration: number;
}

export type MfaPolicyResponse = MfaPolicy | NoMfaPolicy;
