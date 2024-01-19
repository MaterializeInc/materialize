---
title: "Troubleshooting Dataflows"
description: ""
menu:
  main:
    name: "Troubleshooting dataflows"
    identifier: dataflow-troubleshooting-transform
    parent: transform
    weight: 25
---

If you're looking for somewhere to start in troubleshooting slow or unresponsive queries start at the [troubleshooting](troubleshooting) page.

Copy over from the existing https://materialize.com/docs/manage/troubleshooting guide:
* mental model and basic terminology
    * Rename with "Dataflows:" prefix
* The system catalog and introspection relations
* Where is Materialize spending compute time?
* Debugging expensive dataflows and operators
* Is work distributed equally across workers?
* I found a problematic operator. Where did it come from?

Should this also link out to the optimizations page?