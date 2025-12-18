---
title: "Customer responsibility model (Cloud)"
description: "Details about the Materialize Cloud's customer responsibility model."
menu:
  main:
    parent: 'about'
    weight: 25
---

The Materialize Cloud platform is built with a **shared responsibility model**
to ensure the highest levels of data integrity, availability, and resilience.
This page outlines the specific responsibilities of customers to manage their
data and connectivity effectively. Understanding and fulfilling these
responsibilities is critical to leveraging the full potential of the platform.

### Backup

As part of the Materialize Cloud's shared responsibility model, customers are
tasked with maintaining backups of their original data. While Materialize
provides robust data processing capabilities, ensuring that you have a
comprehensive backup strategy is crucial. This includes:

- **Regular backups**. Implementing regular backup schedules that align with
    your data recovery and business continuity plans.

- **Data integrity**. Verifying the integrity of backups to ensure data is
    complete and accurate, enabling effective recovery if needed.

- **Secure storage**. Utilizing secure and reliable storage solutions to protect
    your backups from unauthorized access and potential data loss scenarios.

Materialize maintains backups of core system state and ingested data to ensure
timely recovery in the event of an outage. The data of record is expected to be
maintained in upstream customer systems.

### Recovery

Responsibility for recovery from data connectivity issues rests with the
customer. In the event of an outage or disruption, customers are expected to:

- **Connection recovery**. Re-establish connections to the Materialize platform
    promptly to minimize downtime.

- **Data resynchronization**. In cases where data streaming is interrupted,
    ensure mechanisms are in place to resynchronize any missing data once
    connectivity is restored.

- **Monitoring and alerts**. Implement monitoring solutions to quickly detect
    and respond to connectivity issues.

### Availability

While we strive to provide high-availability, customers play a vital role in
managing their end of the connection to maintain uninterrupted availability.
This includes:

- **Redundant connectivity**. Establishing redundant network paths to
    Materialize can help avoid single points of failure.

- **Load balancing**. Utilizing load balancers to distribute traffic efficiently
    and enhance resilience.

- **Disaster recovery planning**. Incorporating our service into your broader
    disaster recovery plan to ensure business continuity.

### Account management

Tenant accounts and account permissions are set by the customer using native
[access control](/manage/access-control/) features.
2FA, SSO, and password requirements are configurable and highly recommended.

### Data accuracy

Materialize is the data processor and our customers are the data controllers.
Data accuracy and completeness is fully controlled by platform users.

### Data governance

Materialize customers have full responsibility for responding to customer data
privacy and governance requests. For details, see our [Privacy Policy](https://materialize.com/privacy-policy/).

## Responsibility Matrix

| Area | Materialize Responsibilities | Customer Responsibilities |
|------|----------------------------|-------------------------|
| Infrastructure | - Platform maintenance and updates<br>- System availability<br>- Core service reliability | - Client-side infrastructure<br>- Network connectivity<br>- Load balancing configuration |
| Backups | - Core system state<br>- Ingested data | - Source data backups<br>- Integrity and security of source data backups |
| Connection Recovery |  | - Re-establish dropped connections<br>- Data resynchronization<br>- Monitoring and alerting on connection failure<br>- Client-side disaster recovery |
| Availability | - Core service availability<br>- Core platform disaster recovery | - Redundant connectivity<br>- Load balancing configuration<br>- Disaster recovery of customer-owned infrastructure |
| Account Management and Authorization | - Tenant account creation | - Create and manage users within your organization's account<br>- Configure and maintain SSO integration<br>- Enable and configure MFA<br>- Configure role based access control (RBAC)<br>- Audit and monitor user activity |
| Data | - Data processing integrity and correctness | - Data accuracy and quality<br>- Data privacy compliance<br>- Data governance<br>- Data retention policies |
| Security | - Platform security<br>- Enforce customer configured authorization controls<br>- Security patches & vulnerability management | - Account security<br>- Access management<br>- Authentication configuration |
| Monitoring | - Platform health monitoring<br>- System performance metrics | - Connection monitoring<br> |
## Conclusion

Adhering to these responsibilities ensures that your data is protected, and
Materialize is utilized effectively. If you have any questions or need further
clarification on your responsibilities, please [contact support](/support).
