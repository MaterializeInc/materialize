# Self-managed

Authentication and authorization in Self-Managed Materialize.



This section covers security for Self-Managed Materialize.

| Guide | Description |
|-------|-------------|
| [Authentication](/security/self-managed/authentication/) | Enable authentication |
| [Access control](/security/self-managed/access-control/) | Reference for role-based access management (RBAC) |

See also

- [Appendix: Privileges](/security/appendix/appendix-privileges/)
- [Appendix: Privileges by commands](/security/appendix/appendix-command-privileges/)
- [Appendix: Built-in roles](/security/appendix/appendix-built-in-roles/)



---

## Access control (Role-based)


> **Note:** Initially, only the `mz_system` user (which has superuser/administrator
> privileges) is available to manage roles.
>


<a name="role-based-access-control-rbac" ></a>

## Role-based access control

In Materialize, role-based access control (RBAC) governs access to objects
through privileges granted to [database
roles](/security/self-managed/access-control/manage-roles/).

## Enabling RBAC

> **Warning:** If RBAC is not enabled, all users have <red>**superuser**</red> privileges.
>

<p>By default, role-based access control (RBAC) checks are not enabled (i.e.,
enforced) when using <a href="/security/self-managed/authentication/#configuring-authentication-type" >authentication</a>. To
enable RBAC, set the system parameter <code>enable_rbac_checks</code> to <code>'on'</code> or <code>True</code>.
You can enable the parameter in one of the following ways:</p>
<ul>
<li>
<p>For <a href="/self-managed-deployments/installation/#installation-guides" >local installations using
Kind/Minikube</a>, set <code>spec.enableRbac: true</code> option when instantiating the Materialize object.</p>
</li>
<li>
<p>For <a href="/self-managed-deployments/installation/#installation-guides" >Cloud deployments using Materialize&rsquo;s
Terraforms</a>, set
<code>enable_rbac_checks</code> in the environment CR via the <code>environmentdExtraArgs</code>
flag option.</p>
</li>
<li>
<p>After the Materialize instance is running, run the following command as
<code>mz_system</code> user:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">ALTER</span> <span class="k">SYSTEM</span> <span class="k">SET</span> <span class="n">enable_rbac_checks</span> <span class="o">=</span> <span class="s1">&#39;on&#39;</span><span class="p">;</span>
</span></span></code></pre></div></li>
</ul>
<p>If more than one method is used, the <code>ALTER SYSTEM</code> command will take precedence
over the Kubernetes configuration.</p>
<p>To view the current value for <code>enable_rbac_checks</code>, run the following <code>SHOW</code>
command:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">SHOW</span> <span class="n">enable_rbac_checks</span><span class="p">;</span>
</span></span></code></pre></div>> **Important:** If RBAC is not enabled, all users have <red>**superuser**</red> privileges.
>



## Roles and privileges

<p>In Materialize, you can create both:</p>
<ul>
<li>Individual user or service account roles; i.e., roles associated with a
specific user or service account.</li>
<li>Functional roles, not associated with any single user or service
account, but typically used to define a set of shared
privileges that can be granted to other user/service/functional roles.</li>
</ul>
<p>Initially, only the <code>mz_system</code> user is available.</p>


- <p>To create additional users or service accounts, login as the <code>mz_system</code> user,
using the <code>external_login_password_mz_system</code> password, and use <a href="/sql/create-role" ><code>CREATE ROLE ... WITH LOGIN PASSWORD ...</code></a>:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="k">ROLE</span> <span class="o">&lt;</span><span class="k">user</span><span class="o">&gt;</span> <span class="k">WITH</span> <span class="k">LOGIN</span> <span class="k">PASSWORD</span> <span class="s1">&#39;&lt;password&gt;&#39;</span><span class="p">;</span>
</span></span></code></pre></div>

- <p>To create functional roles, login as the <code>mz_system</code> user,
using the <code>external_login_password_mz_system</code> password, and use <a href="/sql/create-role" ><code>CREATE ROLE</code></a>:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="k">ROLE</span> <span class="o">&lt;</span><span class="k">role</span><span class="o">&gt;</span><span class="p">;</span>
</span></span></code></pre></div>

### Managing privileges

<p>Once a role is created, you can:</p>
<ul>
<li><a href="/security/self-managed/access-control/manage-roles/#manage-current-privileges-for-a-role" >Manage its current
privileges</a>
(i.e., privileges on existing objects):
<ul>
<li>By granting privileges for a role or revoking privileges from a role.</li>
<li>By granting other roles to the role or revoking roles from the role.
<em>Recommended for user account/service account roles.</em></li>
</ul>
</li>
<li><a href="/security/self-managed/access-control/manage-roles/#manage-future-privileges-for-a-role" >Manage its future
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
desired privileges and grant these roles to the user or service account role.
Although you can grant privileges directly to the user or service account role,
using separate, reusable roles is recommended for better access management.

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



### Restrict the granting of `CREATEROLE` privilege


{{< include-md file="shared-content/rbac-sm/createrole-consideration.md" >}}



### Use Reusable Roles for Privilege Assignment


{{< include-md file="shared-content/rbac-sm/use-resusable-roles.md" >}}

See also [Manage database roles](/security/self-managed/access-control/manage-roles/).



### Audit for unused roles and privileges.


{{< include-md file="shared-content/rbac-sm/audit-remove-roles.md" >}}

See also [Show roles in
system](/security/self-managed/access-control/manage-roles/#show-roles-in-system)
and [Drop a
role](/security/self-managed/access-control/manage-roles/#drop-a-role) for
more information.




---

## Authentication


## Configuring Authentication Type

To configure the authentication type used by Self-Managed Materialize, use the
`spec.authenticatorKind` setting in conjunction with any specific configuration
for the authentication method.

The `spec.authenticatorKind` setting determines which authentication method is
used:


| authenticatorKind Value | Description |
| --- | --- |
| <strong>None</strong> | Disables authentication. All users are trusted based on their claimed identity <strong>without</strong> any verification. <strong>Default</strong> |
| <strong>Password</strong> | <p>Enables <a href="#configuring-password-authentication" >password authentication</a> for users. When enabled, users must authenticate with their password.</p> > **Tip:** When enabled, you must also set the `mz_system` user password in > `external_login_password_mz_system`. See [Configuring password > authentication](#configuring-password-authentication) for details. > |
| <strong>Sasl</strong> | <p>Enables <a href="#configuring-saslscram-authentication" >SASL/SCRAM-SHA-256 authentication</a> for <strong>PostgreSQL wire protocol connections</strong>. This is a challenge-response authentication mechanism that provides enhanced security compared to simple password authentication.</p> > **Tip:** When enabled, you must also set the `mz_system` user password in > `external_login_password_mz_system`. See [Configuring SASL/SCRAM > authentication](#configuring-saslscram-authentication) for details. > |


> **Warning:** Ensure that the `authenticatorKind` field is set for any future version upgrades or rollouts of the Materialize CR. Having it undefined will reset `authenticationKind` to `None`.
>





## Configuring password authentication

> **Public Preview:** This feature is in public preview.


Password authentication requires users to log in with a password.

To configure Self-Managed Materialize for password authentication, update the following fields in the Materialize CR. For all Materialize CR settings, see [here](/installation/appendix-materialize-crd-field-descriptions/).

 Configuration | Description
---------------| ------------
`spec.authenticatorKind` | Set to `Password` to enable password authentication.
`external_login_password_mz_system` | Set to the Kubernetes Secret referenced by `spec.backendSecretName`, add the secret key `external_login_password_mz_system`. This is the password for the `mz_system` user [^1], who is the only user initially available when password authentication is enabled.

An example Materialize CR YAML file:

```hc {hl_lines="14 24"}
apiVersion: v1
kind: Namespace
metadata:
  name: materialize-environment
---
apiVersion: v1
kind: Secret
metadata:
  name: materialize-backend
  namespace: materialize-environment
stringData:
  metadata_backend_url: "..."
  persist_backend_url: "..."
  external_login_password_mz_system: "enter_mz_system_password"
---
apiVersion: materialize.cloud/v1alpha1
kind: Materialize
metadata:
  name: 12345678-1234-1234-1234-123456789012
  namespace: materialize-environment
spec:
  environmentdImageRef: materialize/environmentd:v0.147.2
  backendSecretName: materialize-backend
  authenticatorKind: Password
```

#### Logging in and creating users

Initially, only the `mz_system` user [^1] is available. To create additional
users:

1. Login as the `mz_system` user, using the
`external_login_password_mz_system` password,
![Image of Materialize Console login screen with mz_system user](/images/mz_system_login.png
"Materialize Console login screen with mz_system user")

1. Use [`CREATE ROLE ... WITH LOGIN PASSWORD ...`](/sql/create-role) to create
   new users:

   ```mzsql
   CREATE ROLE <user> WITH LOGIN PASSWORD '<password>';
   ```

[^1]: The `mz_system` user is also used by the Materialize Operator for upgrades
and maintenance tasks.

## Configuring SASL/SCRAM authentication

> **Note:** SASL/SCRAM-SHA-256 authentication requires Materialize `v26.0.0` or later.
>


SASL/SCRAM-SHA-256 authentication is a challenge-response authentication mechanism
that provides security for **PostgreSQL wire protocol connections**. It is
compatible with PostgreSQL clients that support SCRAM-SHA-256.

To configure Self-Managed Materialize for SASL/SCRAM authentication, update the following fields in the Materialize CR. For all Materialize CR settings, see [here](/installation/appendix-materialize-crd-field-descriptions/).

| Configuration | Description
|---------------| ------------
|`spec.authenticatorKind` | Set to `Sasl` to enable SASL/SCRAM-SHA-256 authentication for PostgreSQL connections.
|`external_login_password_mz_system` | Set to the Kubernetes Secret referenced by `spec.backendSecretName`, add the secret key `external_login_password_mz_system`. This is the password for the `mz_system` user [^1], who is the only user initially available when SASL authentication is enabled.

An example Materialize CR YAML file:

```hc {hl_lines="14 24"}
apiVersion: v1
kind: Namespace
metadata:
  name: materialize-environment
---
apiVersion: v1
kind: Secret
metadata:
  name: materialize-backend
  namespace: materialize-environment
stringData:
  metadata_backend_url: "..."
  persist_backend_url: "..."
  external_login_password_mz_system: "enter_mz_system_password"
---
apiVersion: materialize.cloud/v1alpha1
kind: Materialize
metadata:
  name: 12345678-1234-1234-1234-123456789012
  namespace: materialize-environment
spec:
  environmentdImageRef: materialize/environmentd:v0.147.2
  backendSecretName: materialize-backend
  authenticatorKind: Sasl
```

### Logging in and creating users

The process is the same as for [password authentication](#logging-in-and-creating-users):

1. Login as the `mz_system` user using the `external_login_password_mz_system` password
2. Use [`CREATE ROLE ... WITH LOGIN PASSWORD ...`](/sql/create-role) to create new users

User passwords are automatically stored in SCRAM-SHA-256 format in the database.

### Authentication behavior

When SASL authentication is enabled:

- **PostgreSQL connections** (e.g., `psql`, client libraries, [connection
  poolers](/integrations/connection-pooling/)) use SCRAM-SHA-256 authentication
- **HTTP/Web Console connections** use standard password authentication

This hybrid approach provides maximum security for SQL connections while maintaining
compatibility with web-based tools.

## Deploying authentication

Using the configured Materialize CR YAML file, we recommend rolling out the changes by adding the following fields:
```yaml
spec:
  ...
  requestRollout: <SOME_NEW_UUID> # Generate new UUID for rollout
  forceRollout: <SOME_NEW_UUID> # Rollout without requiring a version change
```

For more information on rollout configuration, view our [upgrade overview](/self-managed-deployments/upgrading/#rollout-configuration).

> **Warning:** Ensure that the `authenticatorKind` field is set for any future version upgrades or rollouts of the Materialize CR. Having it undefined will reset `authenticationKind` to `None`.
>



## Enabling RBAC



See [Access Control](/security/self-managed/access-control/) for details on role based authorization.
