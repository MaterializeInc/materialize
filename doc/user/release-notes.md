---
title: "Release Notes"
description: "What's new in this version of Materialize"
menu: "main"
weight: 500
---

This page details changes between versions of Materialize, including:

- New features
- Major bug fixes
- Substantial API changes

For information about available versions, see our [Versions page](../versions).

## 0.1.0 &rarr; 0.1.1 (unreleased)

- **Indexes on sources**: You can now create and drop indexes on sources, which
  lets you automatically store all of a source's data in an index. Previously,
  you would have to create a source, and then create a materialized view that
  selected all of the source's content.

## _NULL_ &rarr; 0.1.0

- [What is Materialize?](../overview/what-is-materialize/)
- [Architecture overview](../overview/architecture/)
