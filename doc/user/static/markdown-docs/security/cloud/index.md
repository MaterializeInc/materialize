# Cloud

Security for Materialize Cloud



This section covers security for Materialize Cloud.

| Guide | Description |
|-------|-------------|
| [User and service accounts](/security/cloud/users-service-accounts/) | Add user/service accounts |
| [Access control](/security/cloud/access-control/) | Reference for role-based access management (RBAC) |
| [Manage network policies](/security/cloud/manage-network-policies/) | Set up network policies |


See also:

- [Appendix: Privileges](/security/appendix/appendix-privileges/)
- [Appendix: Privileges by commands](/security/appendix/appendix-command-privileges/)
- [Appendix: Built-in roles](/security/appendix/appendix-built-in-roles/)



---

## Access control (Role-based)


> **Disambiguation:** This section focuses on the database access control. For information on organization roles, see [Users and service accounts](../users-service-accounts/).




## Role-based access control (RBAC)

In Materialize, role-based access control (RBAC) governs access to **database
objects** through privileges granted to [database
roles](./manage-roles/).

## Roles and privileges

<p>In Materialize, a database role is created:</p>
<ul>
<li>Automatically when a user/service account is created:
<ul>
<li>When a <a href="/security/cloud/users-service-accounts/invite-users/" >user account is
created</a>, an associated
database role with the user email as its name is created.</li>
<li>When a <a href="/security/cloud/users-service-accounts/create-service-accounts/" >service account is
created</a>, an
associated database role with the service account user as its name is created.</li>
</ul>
</li>
<li>Manually to create a role independent of any specific account,
usually to define a set of shared privileges that can be granted to other
user/service/standalone roles.</li>
</ul>

### Managing privileges

<p>Once a role is created, you can:</p>
<ul>
<li><a href="/security/cloud/access-control/manage-roles/#manage-current-privileges-for-a-role" >Manage its current
privileges</a>
(i.e., privileges on existing objects):
<ul>
<li>By granting privileges for a role or revoking privileges from a role.</li>
<li>By granting other roles to the role or revoking roles from the role.
<em>Recommended for user account/service account roles.</em></li>
</ul>
</li>
<li><a href="/security/cloud/access-control/manage-roles/#manage-future-privileges-for-a-role" >Manage its future
privileges</a>
(i.e., privileges on objects created in the future):
<ul>
<li>By defining default privileges for objects. With default privileges in
place, a role is automatically granted/revoked privileges as new objects are
created by <strong>others</strong> (When an object is created, the creator is granted all
<a href="/security/appendix/appendix-privileges/" >applicable privileges</a> for that
object automatically).</li>
</ul>
</li>
</ul>

> **Disambiguation:** <ul> <li> <p>Use <code>GRANT|REVOKE ...</code> to modify privileges on <strong>existing</strong> objects.</p> </li> <li> <p>Use <code>ALTER DEFAULT PRIVILEGES</code> to ensure that privileges are automatically granted or revoked when <strong>new objects</strong> of a certain type are created by others. Then, as needed, you can use <code>GRANT|REVOKE &lt;privilege&gt;</code> to adjust those privileges.</p> </li> </ul>


### Initial privileges

<p>All roles in Materialize are automatically members of
<a href="/security/appendix/appendix-built-in-roles/#public-role" ><code>PUBLIC</code></a>. As
such, every role includes inherited privileges from <code>PUBLIC</code>.</p>
<p>By default, the <code>PUBLIC</code> role has the following privileges:</p>
<p><strong>Baseline privileges via PUBLIC role:</strong></p>
<table>
<thead>
<tr>
<th>Privilege</th>
<th>Description</th>
<th>On database object(s)</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>USAGE</code></td>
<td>Permission to use or reference an object.</td>
<td><ul> <li>All <code>*.public</code> schemas (e.g., <code>materialize.public</code>);</li> <li><code>materialize</code> database; and</li> <li><code>quickstart</code> cluster.</li> </ul></td>
</tr>
</tbody>
</table>
<p><strong>Default privileges on future objects set up for PUBLIC:</strong></p>
<table>
<thead>
<tr>
<th>Object(s)</th>
<th>Object owner</th>
<th>Default Privilege</th>
<th>Granted to</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><a href="/sql/types/" ><code>TYPE</code></a></td>
<td><code>PUBLIC</code></td>
<td><code>USAGE</code></td>
<td><code>PUBLIC</code></td>
<td>When a <a href="/sql/types/" >data type</a> is created (regardless of the owner), all roles are granted the <code>USAGE</code> privilege. However, to use a data type, the role must also have <code>USAGE</code> privilege on the schema containing the type.</td>
</tr>
</tbody>
</table>
<p>Default privileges apply only to objects created after these privileges are
defined. They do not affect objects that were created before the default
privileges were set.</p>
<p>In addition, all roles have:</p>
<ul>
<li><code>USAGE</code> on all built-in types and <a href="/sql/system-catalog/" >all system catalog
schemas</a>.</li>
<li><code>SELECT</code> on <a href="/sql/system-catalog/" >system catalog objects</a>.</li>
<li>All <a href="/security/appendix/appendix-privileges/" >applicable privileges</a> for
an object they create; for example, the creator of a schema gets <code>CREATE</code> and
<code>USAGE</code>; the creator of a table gets <code>SELECT</code>, <code>INSERT</code>, <code>UPDATE</code>, and
<code>DELETE</code>.</li>
</ul>


You can modify the privileges of your organization's `PUBLIC` role as well as
the modify default privileges for `PUBLIC`.

## Privilege inheritance and modular access control

In Materialize, when you grant a role to another role (user role/service account
role/independent role), the target role inherits privileges through the granted
role.

In general, to grant a user or service account privileges, create roles with the
desired privileges and grant these roles to the database role associated with
the user/service account email/name. Although you can grant privileges directly
to the associated roles, using separate, reusable roles is recommended for
better access management.

With privilege inheritance, you can compose more complex roles by
combining existing roles, enabling modular access control. However:

- Inheritance only applies to role privileges; role attributes and parameters
  are not inherited.
- When you revoke a role from another role (user role/service account
role/independent role), the target role is no longer a member of the revoked
role nor inherits the revoked role&rsquo;s privileges. <strong>However</strong>, privileges are
cumulative: if the target role inherits the same privilege(s) from another role,
the target role still has the privilege(s) through the other role.

## Best practices



### Follow the principle of least privilege

Role-based access control in Materialize should follow the principle of
least privilege. Grant only the minimum access necessary for users and
service accounts to perform their duties.



### Restrict the assignment of **Organization Admin** role


{{< include-md file="shared-content/rbac-cloud/org-admin-recommendation.md" >}}



### Restrict the granting of `CREATEROLE` privilege


{{< include-md file="shared-content/rbac-cloud/createrole-consideration.md" >}}



### Use Reusable Roles for Privilege Assignment


{{< include-md file="shared-content/rbac-cloud/use-resusable-roles.md" >}}

See also [Manage database roles](/security/access-control/manage-roles/).



### Audit for unused roles and privileges.


{{< include-md file="shared-content/rbac-cloud/audit-remove-roles.md" >}}

See also [Show roles in
system](/security/cloud/access-control/manage-roles/#show-roles-in-system) and [Drop
a role](/security/cloud/access-control/manage-roles/#drop-a-role) for more
information.




---

## Manage network policies


> **Tip:** We recommend using [Terraform](https://registry.terraform.io/providers/MaterializeInc/materialize/latest/docs/resources/network_policy)
> to configure and manage network policies.
>


By default, Materialize is available on the public internet without any
network-layer access control. As an **administrator** of a Materialize
organization, you can configure network policies to restrict access to a
Materialize region using IP-based rules.

## Create a network policy

> **Note:** Network policies are applied **globally** (i.e., at the region level) and rules
> can only be configured for **ingress traffic**.
>


To create a new network policy, use the [`CREATE NETWORK POLICY`](/sql/create-network-policy)
statement to provide a list of rules for allowed ingress traffic.

```sql
CREATE NETWORK POLICY office_access_policy (
  RULES (
    new_york (action='allow', direction='ingress',address='1.2.3.4/28'),
    minnesota (action='allow',direction='ingress',address='2.3.4.5/32')
  )
);
```

## Alter a network policy

To alter an existing network policy, use the [`ALTER NETWORK POLICY`](/sql/alter-network-policy)
statement. Changes to a network policy will only affect new connections
and **will not** terminate active connections.

```mzsql
ALTER NETWORK POLICY office_access_policy SET (
  RULES (
    new_york (action='allow', direction='ingress',address='1.2.3.4/28'),
    minnesota (action='allow',direction='ingress',address='2.3.4.5/32'),
    boston (action='allow',direction='ingress',address='4.5.6.7/32')
  )
);
```

### Lockout prevention

To prevent lockout, the IP of the active user is validated against the policy
changes requested. This prevents users from modifying network policies in a way
that could lock them out of the system.

## Drop a network policy

To drop an existing network policy, use the [`DROP NETWORK POLICY`](/sql/drop-network-policy) statement.

```mzsql
DROP NETWORK POLICY office_access_policy;
```

To drop the pre-installed `default` network policy (or the network policy
subsequently set as default), you must first set a new system default using
the [`ALTER SYSTEM SET network_policy`](/sql/alter-system-set) statement.


---

## User and service accounts


As an administrator of a Materialize organization, you can manage the users and
apps (via service accounts) that can access your Materialize organization and
resources.

## Organization roles

During creation of a user/service account in Materialize, the account is
assigned an organization role:


| Organization role | Description |
| --- | --- |
| <strong>Organization Admin</strong> | <ul> <li> <p><strong>Console access</strong>: Has access to all Materialize console features, including administrative features (e.g., invite users, create service accounts, manage billing, and organization settings).</p> </li> <li> <p><strong>Database access</strong>: Has <red><strong>superuser</strong></red> privileges in the database.</p> </li> </ul>  |
| <strong>Organization Member</strong> | <ul> <li> <p><strong>Console access</strong>: Has no access to Materialize console administrative features.</p> </li> <li> <p><strong>Database access</strong>: Inherits role-level privileges defined by the <code>PUBLIC</code> role; may also have additional privileges via grants or default privileges. See <a href="/security/cloud/access-control/#roles-and-privileges" >Access control control</a>.</p> </li> </ul>  |

> **Note:** - The first user for an organization is automatically assigned the
>   **Organization Admin** role.
>
> - An <a href="/security/cloud/users-service-accounts/#organization-roles" >Organization
> Admin</a> has
> <red><strong>superuser</strong></red> privileges in the database. Following the principle of
> least privilege, only assign <strong>Organization Admin</strong> role to those users who
> require superuser privileges.
>
> - Users/service accounts can be granted additional database roles and privileges
>   as needed.
>
>



## User accounts

As an **Organization admin**, you can [invite new
users](./invite-users/) via the Materialize Console. When you invite a new user,
Materialize will email the user with an invitation link.

> **Note:** - Until the user accepts the invitation and logs in, the user is listed as
> **Pending Approval**.
>
> - When the user accepts the invitation, the user can set the user password and
> log in to activate their account. The first time the user logs in, a database
> role with the same name as their e-mail address is created, and the account
> creation is complete.
>
>



For instructions on inviting users to your Materialize organization, see [Invite
users](./invite-users/).

## Service accounts

> **Tip:** As a best practice, we recommend you use service accounts to connect external
> applications and services to Materialize.
>
>


As an **Organization admin**, you can create a new service account via
the [Materialize Console](/console/) or via
[Terraform](/manage/terraform/).

> **Note:** - The new account creation is not finished until the first time you connect with
> the account.
>
> - The first time the account connects, a database role with the same name as the
> specified service account <strong>User</strong> is created, and the service account creation is complete.
>
>


For instructions on creating a new service account in your Materialize
organization, see [Create service accounts](./create-service-accounts/).

## Single sign-on (SSO)

As an **Organization admin**, you can configure single sign-on (SSO) as
an additional layer of account security using your existing
[SAML](https://auth0.com/blog/how-saml-authentication-works/)- or [OpenID
Connect](https://auth0.com/intro-to-iam/what-is-openid-connect-oidc)-based
identity provider. This ensures that all users can securely log in to the
Materialize Console using the same authentication scheme and credentials across
all systems in your organization.

To configure SSO for your Materialize organization, follow [this step-by-step
guide](./sso/).

## See also

- [Role-based access control](/security/cloud/access-control/)
- [Manage with dbt](/manage/dbt/)
- [Manage with Terraform](/manage/terraform/)
