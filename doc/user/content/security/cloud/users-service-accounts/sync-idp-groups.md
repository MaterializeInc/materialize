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

As an **administrator** of a Materialize organization, you can configure
[SCIM](https://scim.cloud/) to sync groups from your identity provider (IdP)
into Materialize and map them to database roles.

* Groups you forward from your IdP are provisioned into your Materialize
  organization via SCIM, along with their memberships.

* When a user authenticates with Materialize, their group memberships are
  included in the authentication exchange. Materialize uses this information
  to grant and revoke their membership in existing database roles with the
  same names.

This means you can manage who belongs to which database role from your IdP.
When a team member joins, leaves, or changes teams, updating their group
membership in the IdP updates their database role memberships, with no manual
`GRANT` or `REVOKE` statements required. Roles you grant manually with `GRANT`
are outside the scope of sync and are never revoked.

{{< important >}}
Group sync only **assigns and unassigns role membership**. It never creates or
drops database roles. You must create the database role yourself before a group
takes effect.
{{</ important >}}

{{< note >}}
Materialize has two separate permission layers, and a group can affect both:

* **Organization roles** (*Organization Admin*, *Organization Member*) control
  access to Materialize and organization administration. A group can be
  assigned an organization role, and members of the group receive that
  organization role automatically. By default, groups are assigned the
  *Organization Member* role. Assigning *Organization Admin* to a group makes
  its members **superusers** in Materialize.

* **Database roles** control access to database objects via [role-based access
  control (RBAC)](/security/cloud/access-control/). Group sync, the subject of
  this page, grants and revokes membership in the database role matching each
  group's name.
{{</ note >}}

## Before you begin

* Your Materialize organization must have an [SSO
  connection](/security/cloud/users-service-accounts/sso/) configured for your
  identity provider. Group sync builds on SSO, so set that up first. You can
  confirm your connection under **Account** > **Account Settings** > **SSO**.

  ![SSO connections in the Materialize Console](/images/console/console-account-settings-sso.png "SSO connections in the Materialize Console")

* You must have an identity provider that supports SCIM 2.0 provisioning
  (e.g., Okta or Microsoft Entra ID).
* Only users assigned the **Organization Admin** role can manage provisioning
  and groups.
* Group-to-role sync applies on **connection**, never mid-session. Materialize
  makes a best effort to apply group changes on the user's next connection,
  but it may take several minutes for changes to be reflected.

## Step 1. Create a SCIM connection

* [Log in to the Materialize Console](/console/).

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

{{< note >}}
Group names that collide with a reserved Materialize role name are skipped and
never mapped to a role. Avoid these names when choosing which groups to push.
See [Limitations](#limitations) for the full list.
{{</ note >}}

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
SCIM badge. Groups pushed from your IdP are read-only in Materialize and must
be managed from the IdP.

{{< /tab >}}
{{< tab "Other providers" >}}

Configure your identity provider's SCIM provisioning to push the users and
groups that should exist in Materialize. Most providers let you scope
provisioning to specific groups, so only those groups and their members are
synced.

{{< /tab >}}
{{< /tabs >}}

## Step 3. Create matching database roles

For each group that should grant access, create a database role with the
**same name** as the group and grant it the privileges the group's members
need. Members of the group get their access through membership in this role:

```mzsql
CREATE ROLE materialize_admins;
GRANT ALL PRIVILEGES ON SCHEMA production TO materialize_admins;
```

The database role must already exist before the user connects. Group sync
only assigns and unassigns membership in existing roles. It never creates
roles, so a group with no matching role is skipped during sync. Group names
must match role names exactly, including case. The roles you create act as an
allowlist for which groups take effect.

To check the mapping status of your groups, navigate to **Account** >
**Account Settings** > **Groups**. Each group indicates whether it has a
corresponding database role and will work with group mapping.

![Groups in the Materialize Console](/images/console/console-account-settings-groups.png "Groups in the Materialize Console")

## Step 4. Verify

Have a user in a synced group connect to Materialize (e.g., via the [SQL
Shell](/console/sql-shell/) or `psql`). On their first login via SSO,
Materialize auto-provisions the user's own database role (named from their
identity claim). Group sync then grants them membership in the database roles
matching their groups. The user's own role is auto-created. The group roles
must already exist.

Role memberships granted by group sync are recorded with the grantor
`mz_jwt_sync`, so you can distinguish them from manual grants:

```mzsql
SELECT r.name AS role, m.name AS member, g.name AS grantor
FROM mz_role_members rm
JOIN mz_roles r ON rm.role_id = r.id
JOIN mz_roles m ON rm.member = m.id
JOIN mz_roles g ON rm.grantor = g.id;
```

All grants and revokes performed by group sync are also recorded in
[`mz_audit_events`](/reference/system-catalog/mz_catalog/#mz_audit_events).

## How sync works

* **Sync happens at connection time.** When a user connects, Materialize
  compares their current group memberships against their sync-managed role
  memberships and applies the difference. Materialize makes a best effort to
  apply changes made in the IdP on the user's next connection, but it may take
  several minutes for changes to be reflected. Changes are never applied to a
  session that is already connected.

* **Manual grants are never touched.** Group sync only manages memberships it
  granted itself (grantor `mz_jwt_sync`). A role granted manually with `GRANT`
  is never revoked by sync, even if the user leaves the corresponding group.
  Audit manual grants periodically to avoid users retaining access through
  stale manual grants.

* **Groups without a matching role are skipped.** The connection proceeds and
  Materialize sends the client a `NOTICE` for each unmatched group.

## Limitations

* **Reserved role names are never mapped.** Groups whose names collide with
  a reserved Materialize role name are always skipped during sync, so IdP
  groups cannot grant system-level privileges. Reserved names are:

  * Any name beginning with `mz_`, `pg_`, or `external_`.
  * The `PUBLIC` role.
  * The role-specification keywords `current_user`, `current_role`,
    `session_user`, `user`, and `none`.

  Matching against reserved names is case-insensitive.
* **Roles are never created or dropped.** Group sync only assigns and
  unassigns role membership. A group only takes effect once you [create a
  database role with a matching name](#step-3-create-matching-database-roles).
* **Changes are not applied in real time.** Group membership changes are only
  applied when a user connects, never to a session that is already connected.
  Materialize makes a best effort to apply changes on the next connection, but
  it may take several minutes for a change in your identity provider to be
  reflected.

## Manage with Terraform

Instead of the Console, you can manage SCIM connections and groups with the
[Materialize Terraform provider](/manage/terraform/):

| Resource | Description |
|----------|-------------|
| [`materialize_scim_config`](https://registry.terraform.io/providers/MaterializeInc/materialize/latest/docs/resources/scim_config) | Manage SCIM provisioning connections. |
| [`materialize_scim_group`](https://registry.terraform.io/providers/MaterializeInc/materialize/latest/docs/resources/scim_group) | Create and manage groups. |
| [`materialize_scim_group_users`](https://registry.terraform.io/providers/MaterializeInc/materialize/latest/docs/resources/scim_group_users) | Manage group membership. |
| [`materialize_scim_group_roles`](https://registry.terraform.io/providers/MaterializeInc/materialize/latest/docs/resources/scim_group_roles) | Assign organization roles to groups. |

## See also

- [Access control (RBAC)](/security/cloud/access-control/)
- [Configure single sign-on (SSO)](/security/cloud/users-service-accounts/sso/)
- [Invite users](/security/cloud/users-service-accounts/invite-users/)
- [Manage with Terraform](/manage/terraform/)
