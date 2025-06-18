---
title: "Release notes"
description: "Release notes for Self-managed Materialize"
menu:
  main:
    weight: 50
    name: "Release notes"
    identifier: "release-notes"
---

## v25.2

### Authentication + RBAC

Starting in v25.2, password authentication and role-based access control are available in self-managed Materialize. For details, see:

- [Password authentication](/manage/authentication).
- [Role-Based Access
  Control](/manage/access-control/#role-based-access-control-rbac).

## Self-managed versioning and lifecycle

Self-managed Materialize uses a calendar versioning (calver) scheme of the form
`vYY.R.PP` where:

- `YY` indicates the year.
- `R` indicates major release.
- `PP` indicates the patch number.

For Self-managed Materialize, Materialize supports the latest 2 major releases.
