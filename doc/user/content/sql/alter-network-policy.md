---
title: "ALTER NETWORK POLICY"
description: "`ALTER NETWORK POLICY` alters a new network policy."
menu:
  main:
    parent: commands
---

{{< warn-if-unreleased-inline "v0.123.0" >}}
{{< public-preview />}}

`ALTER NETWORK POLICY` alters a new network policy.

Network Policies are used to manage access to the system.

## Syntax

{{< diagram "alter-network-policy.svg" >}}
{{< diagram "network-policy-rule-option.svg" >}}

#### Network Policy Options 

| <div style="min-width:240px">Field</div>  | Value            | Description
|-------------------------------------------|------------------|------------------------------------------------
| _name_                                    | `text`           | A name for the Network Policy.
| `RULES`                                   | `text[]`         | A comma-separated list of Network Policy Rules.

#### Network Policy Rule Options 

| <div style="min-width:240px">Field</div>  | Value            | Description
|-------------------------------------------|------------------|------------------------------------------------
| _name_                                    | `text`           | A name for the Network Policy Rule.
| `ACTION`                                  | `text`           | The action to take for this rule. `ALLOW` is the only valid option.
| `DIRECTION`                               | `text`           | The direction of traffic the rule applies to. `INGRESS` is the only valid option.
| `ADDRESS`                                 | `text`           | The Classless Inter-Domain Routing (CIDR) block the rule will be applied to.



## Details
Changes in network policies only take effect for new connections.

An initial `default` network policy will be created. This policy allows open access to the environment and can be altered by a `superuser`.


### Restrictions
There is a limit to 25 rules in a network policy and 25 network policies.
Rules in a network policy must have unique names.

## Privileges

The privileges required to execute this statement are:

- Ownership of the network policy.

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
ALTER NETWORK POLICY office_access_policy ( 
  RULES ( 
    new_york (action='allow', direction='ingress',address='1.2.3.4/28'),
    minnesota (action='allow',direction='ingress',address='2.3.4.5/32'),
    boston (action='allow',direction='ingress',address='4.5.6.7/32')
  )
);
```

```mzsql
ALTER SYSTEM SET network_policy = office_access_policy;
```

## Related pages
- [CREATE NETWORK POLICY](../create-network-policy)
- [DROP NETWORK POLICY](../drop-network-policy)

