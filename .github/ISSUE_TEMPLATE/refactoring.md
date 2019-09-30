---
name: Refactoring
about: >
  Some artifact in the code base that needs to be replaced. It may be cross-
  cutting, as in "replace string identifier with newtype wrapping usize," or
  concerning a specific abstraction (or lack thereof), as in "consolidate
  helper functions for testing relation expressions in new module." If in doubt
  about whether an issue is a bug or refactoring, choose refactoring. If in
  doubt whether an issue is a refactoring or feature, choose feature.
labels: C-refactoring
---

Please make sure you follow the labeling rules for this bug!

1. Assign at least one area label, or more if the refactoring touches multiple
   areas.

   Don't worry about being exact with the area labels. If the refactoring is 90%
   a problem with the dataflow layer, but will require some small changes in the
   SQL layer and the glue layer, feel free to just assign **A-dataflow**.

2. Assign **good first issue** if the issue would make a good starter project
   for a new employee. You will be soundly thanked for this when the next
   employee starts!
