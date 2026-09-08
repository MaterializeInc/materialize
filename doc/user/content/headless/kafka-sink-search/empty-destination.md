---
headless: true
---

{{< warning >}}
A new sink's snapshot inserts only the rows that exist when it starts. The
snapshot does not remove old documents from the destination.

Do not point a new sink at a destination that already holds documents. Those
documents stay in the destination. No later write removes them.
{{< /warning >}}
