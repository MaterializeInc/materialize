# Network Policies

- Associated:
  - https://github.com/MaterializeInc/database-issues/issues/7062
  - https://github.com/MaterializeInc/database-issues/issues/4637
  - https://github.com/MaterializeInc/materialize/pull/29739
  - https://github.com/MaterializeInc/materialize/pull/29179

## The Problem
Customers would like to restrict access to Materialize by IP address.
https://github.com/MaterializeInc/database-issues/issues/4637

## Success Criteria
- Customers can define a global policy that restricts access to their Materialize environments based on the IP address of the client attempting to connect.
- Materialize support can unlock an environment where policies prevent access.
- The console is aware of whether an environment's network policies are blocking a connection it is trying to make.
- Users can adjust network policies via SQL and in the console.

Nice to haves:
- Per-role network policies.
- Policies for sources/sinks.
- Mitigations to prevent user lockouts; such as ensuring the user is not blocking their current IP.
- Termination of active connections based on newly applied policies.
- Policies for egress traffic.

## Out of Scope
- Preventing out-of-policy traffic from reaching Environmentd.
 (note this means network policies will not prevent DDOS)
- Policy inheritance from associated roles. IE if 'bob' is a member of role 'eng'
we will not apply policies from role 'eng' to 'bob'.
- Restricting global API access.
- Restricting access to Frontegg.

## Solution Proposal

#### Overview
The proposed solution is to use role-based policies with a default network policy that applies to any role without a policy. This can initially be implemented as a global default policy and will be extended to per-user and per-source/sink. The policy will be applied when an attempt is made to establish a new client connection with the coordinator.

#### New Resources
A new `NetworkPolicy` resource will be added to the catalog.
```rust
struct NetworkPolicy {
    id: NetworkPolicyId
    name: String,
    rules: Vec<NetworkPolicyRule>,
}

struct NetworkPolicyId {
   System(u64),
   User(64),
}


struct NetworkPolicyRule {
    name: String,
    action: NetworkPolicyRuleAction,
    direction: NetworkPolicyRuleDirection,
    source: IpNet,
}

enum NetworkPolicyRuleAction {
    Allow
    // Deny - may be added later
}

enum NetworkPolicyRuleDirection {
    Ingress
    // egress - will be added later

```

Users will be able to create `NetworkPolicies` directly.  A user must have `CREATENETWORKPOLICY` privileges to create, modify, or destroy network policies. Network policies will be limited to 25 rules. This will be controlled by an LD flag. `NetworkPolicyRules` must be created through a policy. The policy rules implementation will initially only contain an `Allow` variant, but we should be an enum to allow for a `Deny` variant in the future. `NetworkPolicyRule` will hold a `NetworkPolicyRuleDirection` enum to allow for both ingress and egress policies. The names of `NetworkPolicyRules` must be unique within a policy. A `NetworkPolicyRules` will contain a single `IpNet`. We will allow for comments on `NetworkPolicies` as well as individual `NetworkPolicyRules`, the latter will not be implemented initially.

Example syntax for creating a network policy
```sql
CREATE NETWORK POLICY OFFICE_01 (
  RULES (
    main_office ( DIRECTION='INGRESS', ACTION='ALLOW', SOURCE='1.2.3.4/32' ),
    justins_laptop ( DIRECTION='INGRESS', ACTION='ALLOW', SOURCE='52.1.2.3/32' ),
  )
);
```

Example of commenting on a network policy rule
```sql
COMMENT ON NETWORK RULE office_01.justins_laptop is 'Laptop assigned to Justin Bradfield on 2024-01-03'
```

In addition to comments, a future addition to network policies would include the ability to add or drop rules within a policy.
Examples:

```sql
ALTER NETWORK POLICY office_01 DROP RULE justins_laptop
```

```sql
ALTER NETWORK POLICY office_01 ADD RULE justins_laptop (
     DIRECTION='INGRESS', ACTION='ALLOW', SOURCE='52.1.2.3/32'
)
```

Network policies will be assignable to roles - eventually source/sinks as well. A user must have usage privileges for the network policy they wish to assign, as well as the privileges to modify the role.

Example syntax for assigning a network policy to a role
```sql
ALTER ROLE BOB SET network_policy = OFFICE_01;
```
* Policies can only be applied to login roles. There is no policy inheritance. Only the policy assigned to the role the user logged in as will be checked.

Along with network policies a new `SystemVar` (`network_policy`) will be added that points to a specific `NetworkPolicy`. This system var will only be modifiable by `mz_system` and `superuser`. If a resource does not have a network policy this policy will be applied.

Example syntax for updating the network_policy
```sql
ALTER SYSTEM SET network_policy = OFFICE_01;
```

### Predefined Network Policy
By default, a "predefined" user network policy will be created. This policy will
have a wide open ingress rule `allow 0.0.0.0/0`. Its ID will be used for the
initial assignment of the `network_policy` SystemVar. Similar to the quickstart
cluster, this policy will be modifiable by superusers. A superuser may also
remove this policy, however, no policy may be dropped if it is referenced by the
'network_policy` SystemVar. To remove the policy they will first need to create
an alternate policy and re-assign the `network_policy` SystemVar.

### Policy Enforcement
On `coord::handle_startup` a user will be inspected to see if they have a network policy. If the user does not have a policy, the policy specified by `network_policy` will be applied to the user. If the `client_ip` of the user is allowed by the policy the connection will continue normally. If the `client_ip` is denied by a policy, `handle_startup` will return an `AdapterError::UserSessionsDenied`. This error will be handled by the protocol layer, (`HTTP`,`pgwire`) to give the user an L7 response. In the case of `HTTP` this will be a `403 Forbidden`. Additionally, the response body will contain JSON data describing the failure ex:
```json
{
   "message": "session denied",
   "code": "MZ011",
   "detail": "Access denied for address 1.2.3.4",
}
```

When a 403 is returned with a `session denied` message, the console should be made to report that network policies are blocking the user to their environment. Access restriction will not be applied to the Global API, or Frontegg, as such, their UI components may still load.

### Handling lockout.
To mitigate user lockouts, we will prevent users from altering their network policy in a way that will block their current `client_ip`. Administrator lockout from network policies will be treated the same as admin password lockout. The administrator will need to reach out to Materialize support, their identity will be verified based on the policies we set forward, then we will update the policies based on the user's guidance to unlock the user.


### Possible downsides
This design presents a highly configurable solution that guarantees no access to data and is likely the easiest mechanism to implement, however, it does have some downsides. The largest downside is in the guarantee it provides. The best level of network restriction we could provide is that no network traffic reaches the database.  The proposed solution only guarantees that no connection can be established with the data plane (coordinator). This has some implications for DOS attacks which must be handled outside the scope of these policies.

## Minimal Viable Prototype

Minimally, this feature can be implemented with a single `SystemVar` (`network_policy_allow_list`), configurable by `superusers`, that contains an allow-list of CIDRs (`Vec<IpNet>`).

```sql
ALTER SYSTEM SET network_policy_allow_list = '100.10.0.0/28,100.10.128.0/28'
```

The policy will be checked at `coord::handle_startup`, respond with L7 errors on denial, and apply to all user's connections to the system.

PR for minimum prototype: https://github.com/MaterializeInc/materialize/pull/29739

This minimal prototype can be built upon to achieve the proposed solution by
- Introducing network policy resources, and SQL to create them
- Moving the `SystemVar` from a `Vec<IpNet>` to an `Ident` pointing to a `NetworkPolicy` resource
- Modifying roles to have an `Option<NetworkPolicy>` and adding the SQL to set this policy
- Adding validations to prevent lock out
- Following up with sinks/sources

## Alternatives

### What policies apply to.
A common alternative approach is to have a global allow-list. This approach was considered and will be the initially delivered solution; however, adding per-user policies with a default had similar complexity and added clarity around the scope of the policies; i.e., they only impact users connections, not sources, or sinks.

### Where network policies get applied.

Network policies could be applied at many layers of our stack, from network firewalls or security groups that intercept traffic before it hits application subnets, to k8s or cilium network policies, balancers, or within the database itself. The above solutions choose to implement policies within the database itself. This comes with some disadvantages. For instance, this layer does not have auto-scaling and requires the database to do some work for each denied request. For this reason, it makes sense to shift the policies left. One possible shift is to the balancer layer. In this scenario, balancers would support both HTTP and pgwire load-balancing as well as network policy enforcement. Balancers have auto-scaling and are relatively stateless. A large number of out-of-policy requests to a balancer would likely not impact any ongoing connections. The biggest challenge with implementing network policies in the balancer is that they do not have access to the policies or roles, which are stored in the database. To move network policies to the balancers we would need some way of sharing all the policies and roles for all the environments a balancer is proxying. Another place we could shift these policies would be to a WAF or network firewalls. Neither one of these seems reasonable to implement for both pgwire and HTTP in a multi-tenant ingress layer, but this could be revisited for private ingress. It would still have the same issues of keeping policies up-to-date as the balancer.

## Open questions

- **Single default policy for all resources?**
Should there be different default policies for users, sources, and sinks, or should a single default policy be applied to all resources once those resources start supporting policies? It may be difficult to roll out new resources if we only have one default, but it does seem nicer in the long run.

- **How do we handle Webhook Sources?**
Sources and sinks are planned as a follow-up to user-based policies, but it remains an open question how we provide a user-friendly mechanism for webhook sources where it may be hard to find a list of IPs if the webhook request is coming from
a third party.

- **The story on lockout is a bit weak.**
We may want to provide a more programmatic way to handle this, but we can wait and see if this becomes a problem.

- **Should we support per-database or per-cluster policies?**
The answer to this is just no, at least not right now. These sorts of things can be enforced with RBAC for the foreseeable future. It may be worth revisiting if we get per-cluster use-case isolation where envd isn't the single access point.

- **How do we terminate active connections on policy change?**
Once we get `pg_terminate_backend` we should be able to look at all existing connections and terminate them if the new policy would lead to a denial. Until then policies will not affect existing connections.
