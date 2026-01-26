# CREATE NETWORK POLICY (Cloud)

`CREATE NETWORK POLICY` creates a network policy that restricts access to a Materialize Cloud region using IP-based rules.



*Available for Materialize Cloud only*

`CREATE NETWORK POLICY` creates a network policy that restricts access to a
Materialize region using IP-based rules. Network policies are part of
Materialize's framework for [access control](/security/cloud/).

## Syntax



```mzsql
CREATE NETWORK POLICY <name> (
  RULES (
    <rule_name> (action='allow', direction='ingress', address=<address>)
    [, ...]
  )
)
;

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The name of the network policy to modify.  |
| `<rule_name>` | The name for the network policy rule. Must be unique within the network policy.  |
| `<address>` | The Classless Inter-Domain Routing (CIDR) block to which the rule applies.  |



## Details

### Pre-installed network policy

When you enable a Materialize region, a default network policy named `default`
will be pre-installed. This policy has a wide open ingress rule `allow
0.0.0.0/0`. You can modify or drop this network policy at any time.

> **Note:** The default value for the `network_policy` session parameter is `default`.
> Before dropping the `default` network policy, a _superuser_ (i.e. `Organization
> Admin`) must run [`ALTER SYSTEM SET network_policy`](/sql/alter-system-set) to
> change the default value.
>


## Privileges

The privileges required to execute this statement are:

<ul>
<li><code>CREATENETWORKPOLICY</code> privileges on the system.</li>
</ul>


## Examples

```mzsql
CREATE NETWORK POLICY office_access_policy (
  RULES (
    new_york (action='allow', direction='ingress',address='1.2.3.4/28'),
    minnesota (action='allow',direction='ingress',address='2.3.4.5/32')
  )
);
```

```mzsql
ALTER SYSTEM SET network_policy = office_access_policy;
```

## Related pages

- [`ALTER NETWORK POLICY`](../alter-network-policy)
- [`DROP NETWORK POLICY`](../drop-network-policy)
- [`GRANT ROLE`](../grant-role)
