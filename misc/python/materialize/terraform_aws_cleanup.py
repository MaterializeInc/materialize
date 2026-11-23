# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Deletes the AWS resources that test/terraform runs leak.

A run leaks when it dies before its `terraform destroy` completes, or when
that destroy fails. An agent stop mid-apply leaves the EKS cluster being
created outside Terraform's state. A VPC CNI ENI that the node group's cleanup
hook misses keeps the node security group, and with it the whole VPC,
undeletable. Every resource the runs create carries the `REASON` tag (through
the AWS provider's default tags), which is how leftovers are found without the
state of the run that leaked them.
"""

import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Any

import boto3
from botocore.exceptions import ClientError
from mypy_boto3_ec2.client import EC2Client
from mypy_boto3_ec2.type_defs import FilterTypeDef

REASON = "materialize test/terraform CI run"
REGION = "us-east-1"

# Endpoint and NAT gateway ENIs go away a few minutes after their owners.
ENI_RELEASE_TIMEOUT_SECONDS = 15 * 60


def delete_vpcs_named(name: str) -> None:
    """Deletes every test VPC with this `Name` tag, and everything in it."""
    ec2 = boto3.client("ec2", region_name=REGION)
    vpcs = ec2.describe_vpcs(
        Filters=[
            {"Name": "tag:Name", "Values": [name]},
            {"Name": "tag:reason", "Values": [REASON]},
        ]
    )["Vpcs"]
    for vpc in vpcs:
        print(f"Deleting leftover VPC {vpc['VpcId']} ({name})")
        delete_vpc(vpc["VpcId"])


def delete_expired() -> None:
    """Deletes test VPCs, KMS keys and DHCP option sets past their `deleteAfter`.

    Raises after attempting every VPC if any of them could not be deleted.
    """
    ec2 = boto3.client("ec2", region_name=REGION)
    reason_filter: list[FilterTypeDef] = [{"Name": "tag:reason", "Values": [REASON]}]

    vpc_ids = [
        vpc["VpcId"]
        for vpc in ec2.describe_vpcs(Filters=reason_filter)["Vpcs"]
        if _expired(vpc.get("Tags", []))
    ]
    print(f"Expired test/terraform VPCs: {vpc_ids}")
    failures = []
    # Each VPC waits minutes on its EKS cluster, NAT gateway and ENIs.
    with ThreadPoolExecutor(max_workers=8) as executor:
        for vpc_id, future in [(v, executor.submit(delete_vpc, v)) for v in vpc_ids]:
            try:
                future.result()
            except Exception as e:
                print(f"Failed to delete VPC {vpc_id}: {e}")
                failures.append(vpc_id)

    # The VPC deletions above leave none of these attached, but a destroy that
    # failed after deleting its VPC leaves its option set behind.
    for options in ec2.describe_dhcp_options(Filters=reason_filter)["DhcpOptions"]:
        if _expired(options.get("Tags", [])):
            _delete_dhcp_options(ec2, options["DhcpOptionsId"])

    _schedule_expired_kms_key_deletion()

    if failures:
        raise RuntimeError(f"Could not delete test/terraform VPCs: {failures}")


def delete_vpc(vpc_id: str) -> None:
    """Deletes a test VPC with its EKS clusters and network resources.

    Refuses, by raising, when the VPC still holds network interfaces that
    nothing here deletes, such as those of instances or load balancers.
    """
    # The default session is not thread-safe, and `delete_expired` runs this
    # on several threads.
    session = boto3.session.Session(region_name=REGION)
    ec2 = session.client("ec2")
    eks = session.client("eks")
    in_vpc: list[FilterTypeDef] = [{"Name": "vpc-id", "Values": [vpc_id]}]

    for name in eks.list_clusters()["clusters"]:
        try:
            cluster = eks.describe_cluster(name=name)["cluster"]
        except eks.exceptions.ResourceNotFoundException:
            continue
        if cluster["resourcesVpcConfig"]["vpcId"] == vpc_id:
            _delete_eks_cluster(session, name)

    for eni in ec2.describe_network_interfaces(
        Filters=in_vpc + [{"Name": "status", "Values": ["available"]}]
    )["NetworkInterfaces"]:
        print(f"Deleting detached ENI {eni['NetworkInterfaceId']}")
        _ignore_not_found(
            ec2.delete_network_interface,
            NetworkInterfaceId=eni["NetworkInterfaceId"],
        )

    endpoint_ids = [
        endpoint["VpcEndpointId"]
        for endpoint in ec2.describe_vpc_endpoints(Filters=in_vpc)["VpcEndpoints"]
        if endpoint["State"].lower() not in ("deleting", "deleted")
    ]
    if endpoint_ids:
        print(f"Deleting VPC endpoints {endpoint_ids}")
        ec2.delete_vpc_endpoints(VpcEndpointIds=endpoint_ids)

    nat_gateways = ec2.describe_nat_gateways(
        Filters=in_vpc
        + [{"Name": "state", "Values": ["pending", "available", "deleting"]}]
    )["NatGateways"]
    for nat_gateway in nat_gateways:
        if nat_gateway["State"] != "deleting":
            print(f"Deleting NAT gateway {nat_gateway['NatGatewayId']}")
            ec2.delete_nat_gateway(NatGatewayId=nat_gateway["NatGatewayId"])
    if nat_gateways:
        ec2.get_waiter("nat_gateway_deleted").wait(
            NatGatewayIds=[n["NatGatewayId"] for n in nat_gateways]
        )
    # An EIP stays associated, and so unreleasable, until its NAT gateway is gone.
    for nat_gateway in nat_gateways:
        for address in nat_gateway["NatGatewayAddresses"]:
            if "AllocationId" in address:
                print(f"Releasing EIP {address['AllocationId']}")
                _ignore_not_found(
                    ec2.release_address, AllocationId=address["AllocationId"]
                )

    for igw in ec2.describe_internet_gateways(
        Filters=[{"Name": "attachment.vpc-id", "Values": [vpc_id]}]
    )["InternetGateways"]:
        print(f"Deleting internet gateway {igw['InternetGatewayId']}")
        ec2.detach_internet_gateway(
            InternetGatewayId=igw["InternetGatewayId"], VpcId=vpc_id
        )
        ec2.delete_internet_gateway(InternetGatewayId=igw["InternetGatewayId"])

    deadline = time.time() + ENI_RELEASE_TIMEOUT_SECONDS
    while enis := ec2.describe_network_interfaces(Filters=in_vpc)["NetworkInterfaces"]:
        if time.time() > deadline:
            raise RuntimeError(
                f"VPC {vpc_id} still has network interfaces: "
                + ", ".join(
                    f"{e['NetworkInterfaceId']} ({e.get('Description', '')})"
                    for e in enis
                )
            )
        time.sleep(15)

    security_groups = [
        group
        for group in ec2.describe_security_groups(Filters=in_vpc)["SecurityGroups"]
        if group["GroupName"] != "default"
    ]
    # Rules reference the other groups, which blocks deleting those groups.
    for group in security_groups:
        rules = ec2.describe_security_group_rules(
            Filters=[{"Name": "group-id", "Values": [group["GroupId"]]}]
        )["SecurityGroupRules"]
        ingress = [r["SecurityGroupRuleId"] for r in rules if not r["IsEgress"]]
        egress = [r["SecurityGroupRuleId"] for r in rules if r["IsEgress"]]
        if ingress:
            ec2.revoke_security_group_ingress(
                GroupId=group["GroupId"], SecurityGroupRuleIds=ingress
            )
        if egress:
            ec2.revoke_security_group_egress(
                GroupId=group["GroupId"], SecurityGroupRuleIds=egress
            )
    for group in security_groups:
        print(f"Deleting security group {group['GroupId']} ({group['GroupName']})")
        ec2.delete_security_group(GroupId=group["GroupId"])

    for subnet in ec2.describe_subnets(Filters=in_vpc)["Subnets"]:
        ec2.delete_subnet(SubnetId=subnet["SubnetId"])
    for route_table in ec2.describe_route_tables(Filters=in_vpc)["RouteTables"]:
        if not any(a.get("Main") for a in route_table["Associations"]):
            ec2.delete_route_table(RouteTableId=route_table["RouteTableId"])

    options_id = ec2.describe_vpcs(VpcIds=[vpc_id])["Vpcs"][0]["DhcpOptionsId"]
    print(f"Deleting VPC {vpc_id}")
    ec2.delete_vpc(VpcId=vpc_id)
    if options_id != "default":
        _delete_dhcp_options(ec2, options_id)


def _delete_eks_cluster(session: boto3.session.Session, name: str) -> None:
    """Deletes an EKS cluster with its node groups and waits until it is gone."""
    eks = session.client("eks")
    try:
        status = eks.describe_cluster(name=name)["cluster"]["status"]
        # A cluster whose apply an agent stop cut short is still CREATING here.
        # DeleteCluster documents no behavior for that state, so wait for the
        # creation to end (nightly 18816's ended FAILED).
        while status == "CREATING":
            print(f"Waiting for EKS cluster {name} to finish creating")
            time.sleep(30)
            status = eks.describe_cluster(name=name)["cluster"]["status"]
    except eks.exceptions.ResourceNotFoundException:
        return

    node_groups = eks.list_nodegroups(clusterName=name)["nodegroups"]
    for node_group in node_groups:
        print(f"Deleting EKS node group {node_group} of {name}")
        try:
            eks.delete_nodegroup(clusterName=name, nodegroupName=node_group)
        except eks.exceptions.ResourceInUseException:
            pass  # Already deleting.
    for node_group in node_groups:
        eks.get_waiter("nodegroup_deleted").wait(
            clusterName=name, nodegroupName=node_group
        )

    if status != "DELETING":
        print(f"Deleting EKS cluster {name} ({status})")
        eks.delete_cluster(name=name)
    eks.get_waiter("cluster_deleted").wait(name=name)


def _schedule_expired_kms_key_deletion() -> None:
    tagging = boto3.client("resourcegroupstaggingapi", region_name=REGION)
    kms = boto3.client("kms", region_name=REGION)
    for page in tagging.get_paginator("get_resources").paginate(
        TagFilters=[{"Key": "reason", "Values": [REASON]}],
        ResourceTypeFilters=["kms:key"],
    ):
        for resource in page["ResourceTagMappingList"]:
            if not _expired(resource.get("Tags", [])):
                continue
            key_id = resource["ResourceARN"]
            # Keys that Terraform destroyed are already pending deletion.
            state = kms.describe_key(KeyId=key_id)["KeyMetadata"]["KeyState"]
            if state in ("Enabled", "Disabled"):
                print(f"Scheduling deletion of KMS key {key_id}")
                kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)


def _delete_dhcp_options(ec2: EC2Client, options_id: str) -> None:
    try:
        ec2.delete_dhcp_options(DhcpOptionsId=options_id)
        print(f"Deleted DHCP options {options_id}")
    except ClientError as e:
        # Still attached to a VPC, or deleted concurrently.
        if e.response["Error"]["Code"] not in (
            "DependencyViolation",
            "InvalidDhcpOptionID.NotFound",
        ):
            raise


def _expired(tags: list[Any]) -> bool:
    delete_after = next((t["Value"] for t in tags if t["Key"] == "deleteAfter"), None)
    if delete_after is None:
        return False
    return datetime.fromisoformat(delete_after) < datetime.now(UTC)


def _ignore_not_found(call: Callable[..., Any], **kwargs: Any) -> None:
    try:
        call(**kwargs)
    except ClientError as e:
        if not e.response["Error"]["Code"].endswith("NotFound"):
            raise
