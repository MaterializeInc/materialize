# Demos

Creating and sharing demos that highlight Materialize's feature set is a great
way to market our product. However, continuously adding new demos to the repository
increases the maintenance burden shared by the team. To balance the costs and benefits
of creating new demos, we've agreed to split demos into two tiers: 1) officially supported
demos, and 2) play demos.

## Officially Supported Demos

Officially supported demos are (as the name suggests) officially supported by the
engineering team. From a maintenance perspective, fixes to breakages in these demos
should have the same priority as fixes to breakages in the underlying product. To qualify
as an officially supported demo, a new demo must:
- Have consensus from the engineering team to become a supported demo.
- Be up to the regular coding standards of the repository, including tests.
- Have CI tests that run for every PR.

Currently, there are three officially supported demos:
- [Business Intelligence](https://materialize.com/docs/demos/business-intelligence/)
- [Log Parsing](https://materialize.com/docs/demos/log-parsing/)
- [Microservice](https://materialize.com/docs/demos/microservice/)

## Play Demos

By contrast, play demos are not actively supported by the engineering team and do not
require tests. These demos may be created to show off a single feature or to write a
blog post. Play demos can be merged to the repository without involving the engineering
team.
