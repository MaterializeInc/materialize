```none
| object_owner | database | schema | object_type | grantee | privilege_type |
| ------------ | -------- | ------ | ----------- | ------- | -------------- |
| PUBLIC       | null     | null   | type        | PUBLIC  | USAGE          |
```

The example results show one default privilege. This default privilege grants
`USAGE` privilege to all users (`grantee` `PUBLIC`) for **new**
[types](/sql/types/#custom-types) created by any user (`object_owner` `PUBLIC`).

{{< note >}}

- To use a [type](/sql/types/#custom-types) created in a schema, the `USAGE`
access is required on the containing schema as well.
- {{% include-md file="shared-content/rbac-cloud/default-privilege-clarification.md"
  %}}

{{</ note >}}
