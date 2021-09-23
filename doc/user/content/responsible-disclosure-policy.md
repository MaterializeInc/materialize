---
title: "Responsible Disclosure Policy"
description: "Materialize's responsible disclosure policy."
menu: "main"
weight: 600
---

It's extremely important to us at Materialize to ensure that user information is safe and secure. We welcome the contributions of external security researchers towards this end, and have developed this policy to uphold our legal responsibility to good-faith security researchers who provide us with their expertise.

## Scope

Materialize is on a weekly release schedule and expects all users to upgrade to the latest available version within two weeks of a release. Therefore, Materialize's Vulnerability Disclosure Policy covers only issues affecting the latest [version and the version prior to it](../versions).

This restriction applies to both the installed executable (`materialized`) and Materialize Cloud.

## Out of scope

The follow types of attack are out of scope:

- Attacks on physical security
- Attacks that use social engineering, distributed denial of service, or spam
- Attacks that require attacker app to have the permission to overlay on top of our app (for example, tapjacking)
- Vulnerabilities affecting users of outdated browsers or platforms

## How to report

E-mail your findings to <a href="mailto:security@materialize.com">security@materialize.com</a>.

Please include:

- How you discovered the bug
- The impact of the bug
- Any potential remediation
- Any relevant logs or example code

We will respond within two business days with a request for clarification or an estimated timeline for resolution.

**Please do not** report the problem to others until it has been resolved, or for a minimum of 30 days after you have reported the problem to us, whichever comes first.

Once the issue is resolved, you will be listed publically as the discoverer of the issue and may disclose this information at will.

## Legal protections

Materialize agrees not to pursue legal action against  individuals who submit vulnerability reports under the following conditions:

- You submit the report by email to <a href="mailto:security@materialize.com">security@materialize.com</a>.
- Your testing or reserarch does not harm Materialize or its customers.
- Your testing or research is within the [scope](#scope) of our vulnerability disclosure policy.
- If your research and testing affects Materialize users, you must obtain the explicit permission and consent of users who might be affected before engaging in vulnerability testing against their devices, software, and systems.
- You act in good faith to avoid privacy violations, destruction of data, and interruption or degradation of our services (including denial of service attacks).
- You refrain from disclosing vulnerability details to the public until the issue has been resolved, or for a minimum of 30 days after you have reported the problem to us.
- Your testing or research adheres to the laws of your location and the location of Materialize (the United States). For example, violating laws that would only result in a claim by Materialize (and not a criminal claim) may be acceptable as Materialize is authorizing the activity (reverse engineering or circumventing
protective measures) to improve its system.

Specifically, we will not pursue civil action or initiate a complaint to law enforcement for accidental good-faith violations of this policy. We consider activities conducted consistent with this policy to constitute “authorized” conduct under the Computer Fraud and Abuse Act. To the extent your activities are inconsistent with certain restrictions in our user agreement, we waive those restrictions for the limited purpose of permitting security research under this policy. We will not bring a DMCA claim against you for circumventing the technological measures we have used to protect the applications in scope.

If your report addresses a vulnerability of a Materialize business partner, Materialize reserves the right to share your submission in its entirety, including your identity, with the business partner to help facilitate testing and resolution of the reported vulnerability. If legal action is initiated by a third party against you and you have complied with Materialize's responsible disclosure policy, Materialize will take steps to make it known that your actions were conducted in compliance with this policy.

## Compensation

Unfortunately, at this time Materialize doesn't offer cash bounties or swag.

## About this policy

We may modify the terms of this policy or terminate this policy at any time. We won’t apply any changes we make to these policy terms retroactively. Any updates will be noted below in the version notes.

This policy draws heavily from model policies offered by the [National Telecommunications and Information Administration](https://www.ntia.doc.gov/files/ntia/publications/ntia_vuln_disclosure_early_stage_template.pdf) and [Dropbox, Inc.](https://hackerone.com/dropbox?type=team).

## Version notes

**Version 1.0** This document was created 4-October-2021.
