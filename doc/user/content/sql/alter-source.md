---
title: "ALTER SOURCE"
description: "`ALTER SOURCE` changes the provisioned size of a source."
menu:
  main:
    parent: 'commands'
---

`ALTER SOURCE` changes the provisioned [size](/sql/create-source/#sizing-a-source) of a source.

## Syntax

{{< diagram "alter-source.svg" >}}

Field   | Use
--------|-----
_name_  | The identifier of the source you want to alter.
_value_ | The new value for the source size. Accepts values: `3xsmall`, `2xsmall`, `xsmall`, `small`, `medium`, `large`, `xlarge`.

## Privileges

{{< private-preview />}}

The privileges required to execute this statement are:

- Ownership of the source being altered.

## See also

- [`CREATE SOURCE`](/sql/create-source/)
- [`SHOW SOURCES`](/sql/show-sources)
