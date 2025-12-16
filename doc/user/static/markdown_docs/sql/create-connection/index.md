<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/self-managed/v25.2/)
 /  [Reference](/docs/self-managed/v25.2/sql/)

</div>

# CREATE CONNECTION

A connection describes how to connect and authenticate to an external
system you want Materialize to read from or write to. Once created, a
connection is **reusable** across multiple
[`CREATE SOURCE`](/docs/self-managed/v25.2/sql/create-source) and
[`CREATE SINK`](/docs/self-managed/v25.2/sql/create-sink) statements.

To use credentials that contain sensitive information (like passwords
and SSL keys) in a connection, you must first [create
secrets](/docs/self-managed/v25.2/sql/create-secret) to securely store
each credential in Materialize’s secret management system. Credentials
that are generally not sensitive (like usernames and SSL certificates)
can be specified as plain `text`, or also stored as secrets.

## Source and sink connections

### AWS

An Amazon Web Services (AWS) connection provides Materialize with access
to an Identity and Access Management (IAM) user or role in your AWS
account. You can use AWS connections to perform [bulk exports to Amazon
S3](/docs/self-managed/v25.2/serve-results/s3/), perform [authentication
with an Amazon MSK cluster](#kafka-aws-connection), or perform
[authentication with an Amazon RDS MySQL
database](#mysql-aws-connection).

<div class="rr-diagram">

![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSI2MDEiIGhlaWdodD0iMjk3Ij4KICAgPHBvbHlnb24gcG9pbnRzPSI5IDE3IDEgMTMgMSAyMSI+PC9wb2x5Z29uPgogICA8cG9seWdvbiBwb2ludHM9IjE3IDE3IDkgMTMgOSAyMSI+PC9wb2x5Z29uPgogICA8cmVjdCB4PSIzMSIgeT0iMyIgd2lkdGg9Ijc2IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjI5IiB5PSIxIiB3aWR0aD0iNzYiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjM5IiB5PSIyMSI+Q1JFQVRFPC90ZXh0PgogICA8cmVjdCB4PSIxMjciIHk9IjMiIHdpZHRoPSIxMTYiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMTI1IiB5PSIxIiB3aWR0aD0iMTE2IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIxMzUiIHk9IjIxIj5DT05ORUNUSU9OPC90ZXh0PgogICA8cmVjdCB4PSIyODMiIHk9IjM1IiB3aWR0aD0iMTIwIiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjI4MSIgeT0iMzMiIHdpZHRoPSIxMjAiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjI5MSIgeT0iNTMiPklGIE5PVCBFWElTVFM8L3RleHQ+CiAgIDxyZWN0IHg9IjQ0MyIgeT0iMyIgd2lkdGg9IjEzNiIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iNDQxIiB5PSIxIiB3aWR0aD0iMTM2IiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iNDUxIiB5PSIyMSI+Y29ubmVjdGlvbl9uYW1lPC90ZXh0PgogICA8cmVjdCB4PSI3MiIgeT0iMTQ1IiB3aWR0aD0iNDAiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNzAiIHk9IjE0MyIgd2lkdGg9IjQwIiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI4MCIgeT0iMTYzIj5UTzwvdGV4dD4KICAgPHJlY3QgeD0iMTMyIiB5PSIxNDUiIHdpZHRoPSI1NCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIxMzAiIHk9IjE0MyIgd2lkdGg9IjU0IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIxNDAiIHk9IjE2MyI+QVdTPC90ZXh0PgogICA8cmVjdCB4PSIyMDYiIHk9IjE0NSIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjIwNCIgeT0iMTQzIiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjIxNCIgeT0iMTYzIj4oPC90ZXh0PgogICA8cmVjdCB4PSIyNzIiIHk9IjE0NSIgd2lkdGg9IjQ4IiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSIyNzAiIHk9IjE0MyIgd2lkdGg9IjQ4IiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iMjgwIiB5PSIxNjMiPmZpZWxkPC90ZXh0PgogICA8cmVjdCB4PSIzNjAiIHk9IjE3NyIgd2lkdGg9IjI4IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjM1OCIgeT0iMTc1IiB3aWR0aD0iMjgiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjM2OCIgeT0iMTk1Ij49PC90ZXh0PgogICA8cmVjdCB4PSI0MjgiIHk9IjE0NSIgd2lkdGg9IjM4IiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSI0MjYiIHk9IjE0MyIgd2lkdGg9IjM4IiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iNDM2IiB5PSIxNjMiPnZhbDwvdGV4dD4KICAgPHJlY3QgeD0iMjcyIiB5PSIxMDEiIHdpZHRoPSIyNCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIyNzAiIHk9Ijk5IiB3aWR0aD0iMjQiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjI4MCIgeT0iMTE5Ij4sPC90ZXh0PgogICA8cmVjdCB4PSI1MDYiIHk9IjE0NSIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjUwNCIgeT0iMTQzIiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjUxNCIgeT0iMTYzIj4pPC90ZXh0PgogICA8cmVjdCB4PSIzNzMiIHk9IjI2MyIgd2lkdGg9IjU4IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjM3MSIgeT0iMjYxIiB3aWR0aD0iNTgiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjM4MSIgeT0iMjgxIj5XSVRIPC90ZXh0PgogICA8cmVjdCB4PSI0NTEiIHk9IjI2MyIgd2lkdGg9IjEwMiIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iNDQ5IiB5PSIyNjEiIHdpZHRoPSIxMDIiIGhlaWdodD0iMzIiIGNsYXNzPSJub250ZXJtaW5hbCIgLz4KICAgPHRleHQgY2xhc3M9Im5vbnRlcm1pbmFsIiB4PSI0NTkiIHk9IjI4MSI+d2l0aF9vcHRpb25zPC90ZXh0PgogICA8cGF0aCBjbGFzcz0ibGluZSIgZD0ibTE3IDE3IGgyIG0wIDAgaDEwIG03NiAwIGgxMCBtMCAwIGgxMCBtMTE2IDAgaDEwIG0yMCAwIGgxMCBtMCAwIGgxMzAgbS0xNjAgMCBoMjAgbTE0MCAwIGgyMCBtLTE4MCAwIHExMCAwIDEwIDEwIG0xNjAgMCBxMCAtMTAgMTAgLTEwIG0tMTcwIDEwIHYxMiBtMTYwIDAgdi0xMiBtLTE2MCAxMiBxMCAxMCAxMCAxMCBtMTQwIDAgcTEwIDAgMTAgLTEwIG0tMTUwIDEwIGgxMCBtMTIwIDAgaDEwIG0yMCAtMzIgaDEwIG0xMzYgMCBoMTAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbS01NTEgMTQyIGwyIDAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtMiAwIGgxMCBtNDAgMCBoMTAgbTAgMCBoMTAgbTU0IDAgaDEwIG0wIDAgaDEwIG0yNiAwIGgxMCBtMjAgMCBoMTAgbTQ4IDAgaDEwIG0yMCAwIGgxMCBtMCAwIGgzOCBtLTY4IDAgaDIwIG00OCAwIGgyMCBtLTg4IDAgcTEwIDAgMTAgMTAgbTY4IDAgcTAgLTEwIDEwIC0xMCBtLTc4IDEwIHYxMiBtNjggMCB2LTEyIG0tNjggMTIgcTAgMTAgMTAgMTAgbTQ4IDAgcTEwIDAgMTAgLTEwIG0tNTggMTAgaDEwIG0yOCAwIGgxMCBtMjAgLTMyIGgxMCBtMzggMCBoMTAgbS0yMzQgMCBsMjAgMCBtLTEgMCBxLTkgMCAtOSAtMTAgbDAgLTI0IHEwIC0xMCAxMCAtMTAgbTIxNCA0NCBsMjAgMCBtLTIwIDAgcTEwIDAgMTAgLTEwIGwwIC0yNCBxMCAtMTAgLTEwIC0xMCBtLTIxNCAwIGgxMCBtMjQgMCBoMTAgbTAgMCBoMTcwIG0yMCA0NCBoMTAgbTI2IDAgaDEwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbTIgMCBsMiAwIG0tMjIzIDg2IGwyIDAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtMjIgMCBoMTAgbTAgMCBoMTkwIG0tMjIwIDAgaDIwIG0yMDAgMCBoMjAgbS0yNDAgMCBxMTAgMCAxMCAxMCBtMjIwIDAgcTAgLTEwIDEwIC0xMCBtLTIzMCAxMCB2MTIgbTIyMCAwIHYtMTIgbS0yMjAgMTIgcTAgMTAgMTAgMTAgbTIwMCAwIHExMCAwIDEwIC0xMCBtLTIxMCAxMCBoMTAgbTU4IDAgaDEwIG0wIDAgaDEwIG0xMDIgMCBoMTAgbTIzIC0zMiBoLTMiIC8+CiAgIDxwb2x5Z29uIHBvaW50cz0iNTkxIDI0NSA1OTkgMjQxIDU5OSAyNDkiPjwvcG9seWdvbj4KICAgPHBvbHlnb24gcG9pbnRzPSI1OTEgMjQ1IDU4MyAyNDEgNTgzIDI0OSI+PC9wb2x5Z29uPgo8L3N2Zz4=)

</div>

#### Connection options

<table>
<colgroup>
<col style="width: 33%" />
<col style="width: 33%" />
<col style="width: 33%" />
</colgroup>
<thead>
<tr>
<th><div style="min-width:240px">
Field
</div></th>
<th>Value</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>ENDPOINT</code></td>
<td><code>text</code></td>
<td><em>Advanced.</em> Override the default AWS endpoint URL. Allows
targeting S3-compatible services like MinIO.</td>
</tr>
<tr>
<td><code>REGION</code></td>
<td><code>text</code></td>
<td>The AWS region to connect to.</td>
</tr>
<tr>
<td><code>ACCESS KEY ID</code></td>
<td>secret or <code>text</code></td>
<td>The access key ID to connect with. Triggers credentials-based
authentication.<br />
<br />
<strong>Warning!</strong> Use of credentials-based authentication is
deprecated. AWS strongly encourages the use of role assumption-based
authentication instead.</td>
</tr>
<tr>
<td><code>SECRET ACCESS KEY</code></td>
<td>secret</td>
<td>The secret access key corresponding to the specified access key
ID.<br />
<br />
Required and only valid when <code>ACCESS KEY ID</code> is
specified.</td>
</tr>
<tr>
<td><code>SESSION TOKEN</code></td>
<td>secret or <code>text</code></td>
<td>The session token corresponding to the specified access key
ID.<br />
<br />
Only valid when <code>ACCESS KEY ID</code> is specified.</td>
</tr>
<tr>
<td><code>ASSUME ROLE ARN</code></td>
<td><code>text</code></td>
<td>The Amazon Resource Name (ARN) of the IAM role to assume. Triggers
role assumption-based authentication.</td>
</tr>
<tr>
<td><code>ASSUME ROLE SESSION NAME</code></td>
<td><code>text</code></td>
<td>The session name to use when assuming the role.<br />
<br />
Only valid when <code>ASSUME ROLE ARN</code> is specified.</td>
</tr>
</tbody>
</table>

#### `WITH` options

<table>
<colgroup>
<col style="width: 33%" />
<col style="width: 33%" />
<col style="width: 33%" />
</colgroup>
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
should be performed on connection creation.<br />
<br />
Defaults to <code>false</code>.</td>
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
[`mz_internal.mz_aws_connections`](/docs/self-managed/v25.2/sql/system-catalog/mz_internal/#mz_aws_connections)
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
