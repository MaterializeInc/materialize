---
title: "CREATE NETWORK POLICY"
description: "`CREATE NETWORK POLICY` creates a new network policy."
menu:
  main:
    parent: commands
---

{{< warn-if-unreleased-inline "v0.123.0" >}}
{{< public-preview />}}

`CREATE NETWORK POLICY` creates a new network policy.

Network Policies are used to manage access to the system.

## Syntax

{{< diagram "create-network-policy.svg" >}}
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
Network policies can be created to manage access to the system. A network policy will only be applied to resources that are associated with the network policy
or resources with no network policy association if the network policy is set as the system `network_policy`.

### Restrictions
There is a limit to 25 rules in a network policy and 25 network policies.
Rules in a network policy must have unique names.

## Privileges

The privileges required to execute this statement are:

- `CREATENETWORKPOLICY` privileges on the system.

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

- [ALTER NETWORK POLICY](../alter-network-policy)
- [DROP NETWORK POLICY](../drop-network-policy)
