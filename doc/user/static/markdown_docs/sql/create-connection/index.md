<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [SQL commands](/docs/sql/)

</div>

# CREATE CONNECTION

A connection describes how to connect and authenticate to an external
system you want Materialize to read from or write to. Once created, a
connection is **reusable** across multiple
[`CREATE SOURCE`](/docs/sql/create-source) and
[`CREATE SINK`](/docs/sql/create-sink) statements.

To use credentials that contain sensitive information (like passwords
and SSL keys) in a connection, you must first [create
secrets](/docs/sql/create-secret) to securely store each credential in
Materialize’s secret management system. Credentials that are generally
not sensitive (like usernames and SSL certificates) can be specified as
plain `text`, or also stored as secrets.

<div class="note">

**NOTE:** Connections using AWS PrivateLink is for Materialize Cloud
only.

</div>

## Source and sink connections

### AWS

An Amazon Web Services (AWS) connection provides Materialize with access
to an Identity and Access Management (IAM) user or role in your AWS
account. You can use AWS connections to perform [bulk exports to Amazon
S3](/docs/serve-results/s3/), perform [authentication with an Amazon MSK
cluster](#kafka-aws-connection), or perform [authentication with an
Amazon RDS MySQL database](#mysql-aws-connection).

<div class="highlight">

``` chroma
CREATE CONNECTION <connection_name> TO AWS (
    ENDPOINT = '<endpoint>',
    REGION = '<region>',
    ACCESS KEY ID = { '<access_key_id>' | SECRET <secret_name> },
    SECRET ACCESS KEY = SECRET <secret_name>,
    SESSION TOKEN = { '<session_token>' | SECRET <secret_name> },
    ASSUME ROLE ARN = '<role_arn>',
    ASSUME ROLE SESSION NAME = '<session_name>'
)
[WITH (<with_options>)];
```

</div>

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<thead>
<tr>
<th>Syntax element</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>&lt;connection_name&gt;</code></td>
<td>A name for the connection.</td>
</tr>
<tr>
<td><code>ENDPOINT</code></td>
<td><p><em>Value:</em> <code>text</code></p>
<p><em>Advanced.</em> Override the default AWS endpoint URL. Allows
targeting S3-compatible services like MinIO.</p></td>
</tr>
<tr>
<td><code>REGION</code></td>
<td><p><em>Value:</em> <code>text</code></p>
<p><em>For Materialize Cloud only</em> The AWS region to connect to.
Defaults to the current Materialize region.</p></td>
</tr>
<tr>
<td><code>ACCESS KEY ID</code></td>
<td><p><em>Value:</em> secret or <code>text</code></p>
<p>The access key ID to connect with. Triggers credentials-based
authentication.</p>
<p><strong>Warning!</strong> Use of credentials-based authentication is
deprecated. AWS strongly encourages the use of role assumption-based
authentication instead.</p></td>
</tr>
<tr>
<td><code>SECRET ACCESS KEY</code></td>
<td><p><em>Value:</em> secret</p>
<p>The secret access key corresponding to the specified access key
ID.</p>
<p>Required and only valid when <code>ACCESS KEY ID</code> is
specified.</p></td>
</tr>
<tr>
<td><code>SESSION TOKEN</code></td>
<td><p><em>Value:</em> secret or <code>text</code></p>
<p>The session token corresponding to the specified access key ID.</p>
<p>Only valid when <code>ACCESS KEY ID</code> is specified.</p></td>
</tr>
<tr>
<td><code>ASSUME ROLE ARN</code></td>
<td><p><em>Value:</em> <code>text</code></p>
<p>The Amazon Resource Name (ARN) of the IAM role to assume. Triggers
role assumption-based authentication.</p></td>
</tr>
<tr>
<td><code>ASSUME ROLE SESSION NAME</code></td>
<td><p><em>Value:</em> <code>text</code></p>
<p>The session name to use when assuming the role.</p>
<p>Only valid when <code>ASSUME ROLE ARN</code> is specified.</p></td>
</tr>
<tr>
<td><code>WITH (&lt;with_options&gt;)</code></td>
<td><p>The following <code>&lt;with_options&gt;</code> are
supported:</p>
<table>
<thead>
<tr>
<th>Field</th>
<th>Value</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>VALIDATE</code></td>
<td><code>boolean</code></td>
<td>Whether <a href="#connection-validation">connection validation</a>
should be performed on connection creation. Default:
<code>false</code>.</td>
</tr>
</tbody>
</table></td>
</tr>
</tbody>
</table>

#### Permissions

<div class="warning">

**WARNING!** Failing to constrain the external ID in your role trust
policy will allow other Materialize customers to assume your role and
use AWS privileges you have granted the role!

</div>

When using role assumption-based authentication, you must configure a
[trust
policy](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_terms-and-concepts.html#term_trust-policy)
on the IAM role that permits Materialize to assume the role.

Materialize always uses the following IAM principal to assume the role:

```
arn:aws:iam::664411391173:role/MaterializeConnection
```

Materialize additionally generates an [external
ID](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html)
which uniquely identifies your AWS connection across all Materialize
regions. To ensure that other Materialize customers cannot assume your
role, your IAM trust policy **must** constrain access to only the
external ID that Materialize generates for the connection:

<div class="highlight">

``` chroma
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::664411391173:role/MaterializeConnection"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "<EXTERNAL ID FOR CONNECTION>"
                }
            }
        }
    ]
}
```

</div>

You can retrieve the external ID for the connection, as well as an
example trust policy, by querying the
[`mz_internal.mz_aws_connections`](/docs/sql/system-catalog/mz_internal/#mz_aws_connections)
table:

<div class="highlight">

``` chroma
SELECT id, external_id, example_trust_policy FROM mz_internal.mz_aws_connections;
```

</div>

#### Examples

<div class="code-tabs">

<div class="tab-content">

<div id="tab-role-assumption" class="tab-pane" title="Role assumption">

In this example, we have created the following IAM role for Materialize
to assume:

Name

</div>

</div>

</div>

</div>

</div>
