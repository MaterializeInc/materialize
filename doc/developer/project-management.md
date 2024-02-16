# Project Management

At Materialize, we have a
[unified Engineering, Product, Design (EPD) backlog](https://github.com/orgs/MaterializeInc/projects/43).
The backlog describes all prioritized projects across our EPD teams, including:
Product Teams, Functional Teams, Product Design, and Analytics. Each roadmap
item is marked with a priority ("Now," "Next," or "Later") that indicates the
timeline upon which the item will be picked up, worked on, and eventually
delivered.

Each project marked "Now" must be owned by a single Directly Responsible
Individual (DRI). The DRI for a project can be any member of the larger
EPD team. The DRI is responsible for driving the project to completion,
and also for keeping the status of the project visible and up to date
at all times. The DRI for any given project can change over time, as
long as the handoff is publicly documented.

The unified EPD backlog is revised by EPD management once a quarter.
For the most part, projects are picked up and prioritized from the
existing backlog of issues. In some cases, new projects will be spun
up to address an urgent and important need that is not already
represented in the backlog.

The EPD backlog is not set in stone. Although we only holistically
revise the backlog once a quarter, prioritizations of individual
projects can and should change as we learn new information. EPD
management is responsible for ensuring that we are working on the
right things each week in a recurring review. During this review,
the priority and staffing of any individual project can be updated.

The rest of this document describes how individual projects are
managed at Materialize, broken down into four interconnected phases:
identification, triage, discovery, and delivery.

## Identification

Fundamentally, there are two types of issues we create to identify
potential work: bugs and problems. Bugs report a defect in an
existing feature. Bugs can be reported by internal team members
and external users, and should be created in the repository that
contains the buggy code wherever possible (use a private repository
if the issue description contains sensitive information that you
want to record).

Problems, on the other hand, can identify potential improvements
as well as net-new features. Because of this, they can vary greatly
in both size and complexity. They can be as small and as scoped as
adding a simple SQL function to Materialize, or as large and as
unbounded as making sure that users can sufficiently observe Materialize's
performance. Both types of problems are important to solve in order to
ensure the overall success of Materialize as a product.

By default, all created issues will end up in a Materialize-wide
backlog. Issues created in our public GitHub repository will be
visible outside of our team.

## Triage

The purpose of the Triage phase is to ensure we're working on the
most urgent and important problems at all times.

Newly reported bugs will be triaged most frequently. If the reported
bug has been assigned a `P0` or `release-blocker` label, it must be
triaged within 24 hours. All other bugs will be triaged within 5
business days. The remaining backlog of problem Issues will be triaged
by EPD management at least once a month.

Some problems will be sufficiently straightforward with a clearly
defined scope, a limited solution space, and limited effects on other
teams and projects. In these cases, these GitHub Issues can be directly
assigned to an owner for implementation.

Other problems will be less straightforward. Some may require more
digging or debugging to understand what's going wrong. Some may only
highlight a symptom of a larger issue with the product, and require
the Product team to discover the true underlying problem. Some may
impact the work of other teams or require ongoing stakeholder management.
In any of these cases, or in any case where a solution does not feel
simple, problems should be turned into a GitHub Epic.

Each standalone Issue and Epic will be prioritized and tagged with
a Milestone: ["Now," "Next," or "Later,"](https://www.notion.so/Product-Planning-and-Prioritization-Guidance-ce7f91bd5e224a71841630c306414700#b745adb7b4604268a93033b38a504aae)
indicating when work for the project will begin. A DRI must be
assigned to an Issue or an Epic as it is marked to be worked on
"Now," moving the project forward to the Discovery phase.

## Discovery

The purpose of the Discovery phase is to sufficiently explore the
solution space of a problem before committing to a particular solution.
Design documents are the tool that we use to do this. The DRI is
responsible for driving the Design Document Process for their project.

If a project is sufficiently straightforward, the DRI can opt to skip
the Design Document Process. The DRI will need to write that they're
skipping this step in the GitHub Epic.

Otherwise, the DRI is responsible for researching the solution space,
proposing potential solutions, and getting signoff from relevant stakeholders.
This does not mean that the DRI is required to convince everyone of their chosen
solution — we do not intend to design by committee. If you are the DRI for a
project and need help identifying your relevant stakeholders, please reach
out to your manager.

## Delivery

Once a design has been approved, the DRI can move a project from Discovery
to Delivery. During Delivery, the DRI is responsible for ushering
their project through all of the phases of our [Project Lifecycle](./project-lifecycle.md),
from Internal Development to General Availability.

It is not required that the DRI personally deliver each piece
of work attached to a project. The DRI will likely require help from a
Product Manager, a Product Designer, Engineers, and our testing and GTM
teams along the way. The DRI is responsible for assigning each piece of
work attached to a project, and ensuring that progress is moving along
as expected.

The DRI is also responsible for ensuring that the status of their project
is up to date and visible at all times. At a minimum, this means including
the GitHub Epic on their team's GitHub project, marking the Epic with the
correct lifecycle phase, and proactively calling out any potential blockers.
Depending on the project, it might be useful to broadcast even more
information more frequently.
