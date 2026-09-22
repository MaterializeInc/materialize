---
title: "Sync identity provider groups to database roles"
description: "Provision groups from your identity provider via SCIM and map them to Materialize database roles."
menu:
  main:
    parent: user-service-accounts
    weight: 20
---

{{< private-preview >}}
Group-to-role mapping
{{< /private-preview >}}

As an **administrator** of a Materialize organization, you can provision groups
from your identity provider (IdP) with [SCIM](https://scim.cloud/), assign those
groups custom organization roles, and map the roles to database roles.

The mapping has three layers:

| Layer | Example | Managed in |
|-------|---------|------------|
| IdP group | `analytics-team` | Your identity provider |
| Custom organization role | `analytics_reader` | Materialize Console (**Roles**) or Terraform |
| Database role | `analytics_reader` | SQL or Terraform |

Custom organization roles live in Materialize's identity provider and are scoped
to your organization. You can manage them through the Materialize Console or
Terraform.

SCIM provisions the group and its members. You assign the group a custom
organization role. When a member connects, Materialize reads their organization
role keys from the authentication token (JWT) and reconciles membership in
existing database roles with matching names. The IdP group name can differ from
the database role name.

{{< important >}}
Create both the custom organization role and the database role. Creating one
does not create the other. Database privileges come from grants to the database
role, not from the permissions selected when creating the organization role.
{{< /important >}}

The built-in **Organization Admin** and **Organization Member** roles control
organization access. Retain **Organization Member** alongside your custom roles
when users need its permissions. Assigning **Organization Admin** makes a user a
Materialize superuser, so do not use it as the starting point for a role intended
to grant limited database access.

{{< note >}}
$TODO: Before publishing this setup, specify the first provider release with
`materialize_organization_role` and custom-role mapping support, confirm
organization role management is enabled, and verify the production JWT claim
configuration used for database role sync. Confirm how to obtain the JWT key
for a role created in the Console.
{{< /note >}}

## Before you begin

* Your Materialize organization must have an [SSO
  connection](/security/cloud/users-service-accounts/sso/) configured for your
  identity provider. Group sync builds on SSO, so set that up first. You can
  confirm your connection under **Account** > **Account Settings** > **SSO**.

  ![SSO connections in the Materialize Console](/images/console/console-account-settings-sso.png "SSO connections in the Materialize Console")

* You must have an identity provider that supports SCIM 2.0 provisioning
  (e.g., Okta or Microsoft Entra ID).
* Your organization must have role mapping and custom organization role
  management enabled. During private preview, contact Materialize support to
  enable them.
* Only users assigned the **Organization Admin** role can manage provisioning,
  groups, and custom organization roles.
* Group-to-role sync applies on **connection**, never mid-session. Materialize
  makes a best effort to apply group changes on the user's next connection,
  but it may take several minutes for changes to be reflected.

## Step 1. Create a SCIM connection

* [Log in to the Materialize Console](/developer-tools/console/).

* Navigate to **Account** > **Account Settings** > **Provisioning**.

* Click **Add Connection**, name the integration, and select your identity
  provider (**Okta**, **Azure**, or **Custom SCIM** for any other SCIM
  2.0-compatible provider).

  ![Setup SCIM connection dialog in the Materialize Console](/images/console/console-account-settings-add-scim.png "Setup SCIM connection dialog in the Materialize Console")

* Follow the in-console guide for your provider. The Console generates a SCIM
  endpoint URL and an API token, which you enter into your identity provider's
  provisioning settings.

Once your identity provider connects successfully, the connection shows as
**Linked**.

![Provisioning connections in the Materialize Console](/images/console/console-account-settings-provisioning.png "Provisioning connections in the Materialize Console")

## Step 2. Choose which groups to sync

Materialize only syncs the groups you explicitly configure your identity
provider to send. Your other IdP groups are not visible to Materialize.

{{< tabs >}}
{{< tab "Okta" >}}

* In the Okta Admin Console, open the SCIM application you connected in
  [Step 1](#step-1-create-a-scim-connection).

* On the **Assignments** tab, assign the users (or groups) that should be
  provisioned into Materialize.

* On the **Push Groups** tab, click **Push Groups** and select the groups to
  sync, either by name or by rule.

  ![Push Groups tab of the Okta SCIM application](/images/console/okta-push-groups.png "Push Groups tab of the Okta SCIM application")

Once pushed, the groups and their memberships appear in the Materialize
Console under **Account** > **Account Settings** > **Groups**, marked with a
SCIM badge. Manage group names and membership in the IdP. You can edit the
organization roles assigned to a SCIM-provisioned group in Materialize.

{{< /tab >}}
{{< tab "Other providers" >}}

Configure your identity provider's SCIM provisioning to push the users and
groups that should exist in Materialize. Most providers let you scope
provisioning to specific groups, so only those groups and their members are
synced.

{{< /tab >}}
{{< /tabs >}}

## Step 3. Create custom organization roles

Create a custom organization role for each database access role in the
Materialize Console's **Roles** page or using the
[Terraform configuration below](#manage-with-terraform). For example, with
Terraform, create `analytics_reader` with `base_role_name = "Member"`.

If you apply the complete Terraform example, it also creates the group
assignment and database role described in Steps 4 and 5. Continue with database
grants and verification.

The Terraform resource sets the JWT key to the role name on creation and copies
the base role's organization permissions. Use its exported `key` as the database
role name. Copying **Organization Member** permissions provides a starting point
without organization administration permissions. These permissions do not
replace SQL grants.

Avoid reserved names listed under [Limitations](#limitations), including the
built-in organization role names and keys.

## Step 4. Assign organization roles to synced groups

Wait until the group appears under **Account** > **Account Settings** >
**Groups**. Edit its role assignments and add the custom role created in
[Step 3](#step-3-create-custom-organization-roles). Retain any built-in role
assignments the group still needs, such as **Organization Member**.

For example, assign `analytics_reader` to the SCIM group `analytics-team`.
Members receive the custom organization role through their group membership.
Multiple groups can grant the same role, and one group can grant multiple roles.

## Step 5. Create matching database roles

Create the database role in each Materialize region where the group should have
access. Its name must exactly match the custom organization role's JWT key,
including case. If Terraform has not already created the database role, create
it with SQL. For a role whose key is `analytics_reader`:

```mzsql
CREATE ROLE analytics_reader;
```

Grant the database role the privileges the group's members need. See
[Access control (RBAC)](/security/cloud/access-control/) for examples.
Role sync does not create database roles or grant object privileges. An
organization role with no matching database role is skipped during sync.

## Step 6. Verify grants and revocations

Have a dedicated test user in the synced group sign in and open a new database connection,
for example with the [SQL Shell](/console/sql-shell/). On first sign-in,
Materialize creates the user's own database role. The shared database role
`analytics_reader` must already exist.

As an administrator, query the role memberships:

```mzsql
SELECT r.name AS role, m.name AS member, g.name AS grantor
FROM mz_role_members rm
JOIN mz_roles r ON rm.role_id = r.id
JOIN mz_roles m ON rm.member = m.id
JOIN mz_roles g ON rm.grantor = g.id
WHERE r.name = 'analytics_reader'
  AND g.name = 'mz_jwt_sync';
```

The result should include the user's database role as `member` and
`mz_jwt_sync` as `grantor`. Remove the user from the IdP group, wait for
provisioning and token refresh, and have them reconnect. The sync-managed grant
should disappear unless another group or a direct organization role assignment
still grants the same custom role. Restore the test user's membership after
verifying revocation.

Grants and revokes performed by sync are recorded in
[`mz_audit_events`](/reference/system-catalog/mz_catalog/#mz_audit_events).

## How sync works

* **Sync happens at connection time.** When a user connects, Materialize
  compares the organization role keys in their JWT against their sync-managed role
  memberships and applies the difference. Materialize makes a best effort to
  apply changes made in the IdP on the user's next connection, but it may take
  several minutes for changes to be reflected. Changes are never applied to a
  session that is already connected.

* **Manual grants are never touched.** Group sync only manages memberships it
  granted itself (grantor `mz_jwt_sync`). A role granted manually with `GRANT`
  is never revoked by sync, even if the user leaves the corresponding group.
  Audit manual grants periodically to avoid users retaining access through
  stale manual grants.

* **Organization roles without a matching database role are skipped.** The
  connection proceeds and Materialize sends the client a `NOTICE` for each
  unmatched role key.

## Limitations

* **Reserved database role names are never mapped.** Organization role keys
  that collide with reserved database role names are skipped during sync.
  Reserved names are:

  * Any name beginning with `mz_`, `pg_`, or `external_`.
  * The `PUBLIC` role.
  * The role-specification keywords `current_user`, `current_role`,
    `session_user`, `user`, and `none`.

  Matching against reserved names is case-insensitive.

  Do not create custom or database roles named `Organization Admin`,
  `Organization Member`, `MaterializePlatformAdmin`, or `MaterializePlatform`.
  These are built-in organization role names and keys, not custom database
  access roles.
* **Roles are never created or dropped.** Group sync only assigns and
  unassigns role membership. An organization role only takes effect once you
  [create a matching database role](#step-5-create-matching-database-roles).
* **Changes are not applied in real time.** Group membership changes are only
  applied when a user connects, never to a session that is already connected.
  Materialize makes a best effort to apply changes on the next connection, but
  it may take several minutes for a change in your identity provider to be
  reflected.

## Manage with Terraform

The [Materialize Terraform provider](/manage/terraform/) manages SCIM
connections, custom organization roles, database roles, and group-to-role
assignments. Provision IdP-owned groups and membership through your identity
provider.

{{< note >}}
The `materialize_organization_role` resource and custom-role assignment support
must be available in your installed provider version before using this example.
{{< /note >}}

Create the SCIM integration with `materialize_scim_config` first. Configure your
IdP to push groups, and wait for them to appear in Materialize. Then apply the
role mapping. Terraform cannot wait for an IdP push simply by depending on the
SCIM connection resource.

```hcl
resource "materialize_organization_role" "reader" {
  name           = "analytics_reader"
  base_role_name = "Member"
}

resource "materialize_role" "reader" {
  name = materialize_organization_role.reader.key
}

data "materialize_scim_groups" "all" {}

locals {
  analytics_groups = [
    for group in data.materialize_scim_groups.all.groups : group
    if group.name == "analytics-team" && group.managed_by == "scim"
  ]
}

resource "materialize_scim_group_roles" "reader" {
  group_id = try(one(local.analytics_groups).id, "")
  roles    = ["Member", materialize_organization_role.reader.name]

  lifecycle {
    precondition {
      condition     = length(local.analytics_groups) == 1
      error_message = "Wait for exactly one SCIM group named analytics-team to be provisioned, then rerun Terraform."
    }
  }
}
```

The organization role copies **Organization Member** permissions when created.
Later changes to the base role are not copied automatically. Add database grants
separately to give `analytics_reader` access to the required objects.

`materialize_scim_group_roles` manages the group's complete set of organization
role assignments. Include every role the group should retain. Do not use
`materialize_scim_group` or `materialize_scim_group_users` to take ownership of
IdP-provisioned groups or their membership.

## Migrate from direct group-name mapping

Before changing the mapping mechanism for an existing organization:

1. Create a custom organization role for every database role currently supplied
   by group sync. Set its `name` to the existing database role name. Terraform
   uses this name as the JWT key.
2. Assign each synced group its corresponding custom organization roles.
3. Coordinate the switch with Materialize support. The organization must use
   the role claim before the group-name mapping is retired.
4. Verify both grants and revocations using [Step 6](#step-6-verify-grants-and-revocations).

Keep existing database roles and their object grants. Switching to a role claim
without preparing equivalent assignments can revoke sync-managed memberships
when users reconnect.

## See also

- [Audit events](/reference/system-catalog/mz_catalog/#mz_audit_events)
- [Access control (RBAC)](/security/cloud/access-control/)
- [Configure single sign-on (SSO)](/security/cloud/users-service-accounts/sso/)
- [Invite users](/security/cloud/users-service-accounts/invite-users/)
- [Manage with Terraform](/developer-tools/terraform/)
