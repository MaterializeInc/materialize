---
headless: true
---
{{< warning >}}
Once enabled, ensure that the `authenticatorKind` field is set for any future version upgrades or rollouts of the Materialize CR. Having it undefined will reset `authenticationKind` to `None`.
{{< /warning >}}
