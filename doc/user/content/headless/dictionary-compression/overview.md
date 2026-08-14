---
headless: true
---
{{< if-released "v26.38" >}}
{{< public-preview >}}
Arrangement dictionary compression
{{< /public-preview >}}
{{< /if-released >}}

{{< warn-if-unreleased "v26.38" >}}

Starting in v26.38, dictionary compression will be available (as **public
preview**) for managed clusters. Dictionary compression reduces the memory that
[arrangements](/get-started/arrangements/#arrangements) use when a column holds
the same values repeatedly. Instead of storing a repeated column value each time
it appears, Materialize stores that value once and has each row reference it.

Dictionary compression is off by default. You opt in per cluster with the
`EXPERIMENTAL ARRANGEMENT COMPRESSION` option.
