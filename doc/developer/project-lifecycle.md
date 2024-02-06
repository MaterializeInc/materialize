# Project Lifecycle

The project lifecycle at Materialize consists of four phases:
Internal Development, Private Preview, Public Preview, and General
Availability. The [Directly Responsible Individual (DRI)](./project-management.md)
is responsible for bringing a project through all four phases.
This does not mean that the DRI must personally do each work item
for each lifecycle phase, but they are responsible for ensuring
each work item is completed.

The DRI is responsible for communicating progress through these
lifecycle phases by:

- Updating the "Lifecycle" tag on the associated GitHub Epic.
- Optionally, announcing the update in `#epd-announce` and
  `#shipped-it`.

## Internal Development

The goal of Internal Development is to build enough of a solution
to a problem that we can demo it in front of users, internal and external,
to test it. Depending on the size and scope of a project,
Internal Development can take anywhere from a few days to a few months to complete.

It is best practice for the DRI to break the solution into meaningful
and manageable work items before anyone begins implementing it.
Smaller, scoped work items provide good check in points for the
DRI to communicate progress on the solution more widely.

_Note: nothing shown or built in the Internal Development phase is
expected to be stable, nor should it ever be available in a customer environment._

Before a project can move to Private Preview, it must:

- Be code complete, unless we are intentionally putting a
  partial solution in front of testers.
- Have Stash migrations written and reviewed, if the code contains
  breaking syntax changes.
- Be stable enough that it will not:
    - Impact other customer environments when ungated for a single customer.
    - Impact other clusters/workloads, when ungated in a customer environment.
    - Cause stability issues for other parts of the system.
- Live behind a gated feature flag (disabled by default).
- Have basic tests live and running.
- Have basic reference documentation live behind a `{{< private-preview >}}` flag.
- Have its GitHub epic status set to "Private Preview".

## Private Preview

The goal of Private Preview is to identify and address all of
the reasons why the solution might not work. To do this, the
DRI is responsible for getting the solution in front of relevant
internal (e.g. testing team, analytics team) and external (e.g. customers) testers.
It is recommended to kick this off by posting to `#epd-announce`
and asking for support on identifying testers. Customer-facing
teams such as field engineering are well-positioned to provide
guidance on external testers. If you're unsure how to proceed,
reach out to your manager for help.

Testers may raise a few different classes of issues:
[value, usability, feasibility, and viability](https://www.svpg.com/four-big-risks/).
Some of these issues may be significant enough for the DRI to
reconsider the chosen solution, effectively sending a project
back to the Discovery phase. Other issues may be smaller, and
fixes can be made within the scoped solution. The Private Preview
phase may take a few days for smaller projects, and up to a few
weeks for larger ones.

_Note: solutions in Private Preview will not be available in
customer environments by default, they will be turned on for
individual customers via our [feature flagging system](https://www.notion.so/materialize/45cf26682e1b4d1d87325d04f5885725).
Customers who test solutions in this phase should be warned
that they are subject to significant, and occasionally
incompatible, changes._

Before a project can move to Public Preview, it must:

- Be verified that it solves the targeted problem for at least
  one external tester, with additional optional internal testers.
- Be entirely code complete, if it wasn't already.
- Have comprehensive tests live and running.
- Have suitable observability live and running.
- Have suitable metrics live and running.
- Have addressed all known stability issues.
- Have addressed all known limitations, except those that are explicitly out of scope.
- Support all known customer configurations, except those that are explicitly out of scope.
- Have polished reference documentation published with a `{{< private-preview >}}` notice.

To move a feature to Public Preview:

- Update any documentation about the feature to use the `{{< public-preview >}}` notice.
- Add a release note with a `{{< public-preview >}}` flag.
- Update the GitHub epic status to "Public Preview".
- Enable the feature flag in all environments.

## Public Preview

The goal of Public Preview is to ensure the stability of a
solution. In this phase, solutions are available in all customer
environments by default.

The Public Preview phase is likely to take the longest to
complete: it depends on internal confidence in the stability of
a solution as well as its adoption by our users. During this phase,
the DRI may continue to collect feedback about the solution. The
DRI is responsible for curating that feedback and applying
changes as needed.

Before a project can move to General Availability, it must:

- Be used by at least two customers in production use cases.
- Have additional documentation (like user guides) live, or
  an explicit justification for why it's not required.

To move a project to General Availability:

- Remove the `{{< public-preview >}}` notice from the documentation.
- Write a blog post describing the project, or explicitly
  justify why a blog post is not required.
- Write a release note announcing that the project is in General Availability.
- Write a Changelog announcement.
- Update the GitHub epic status to "GA", then close the issue.

## General Availability

The General Availability is the last phase of the project
lifecycle. By this point, we're convinced about a solution's value,
as well as its performance and stability. Generally available
solutions are available in all customer environments.

Once a project reaches this phase, we will consider removing its
feature flag.
