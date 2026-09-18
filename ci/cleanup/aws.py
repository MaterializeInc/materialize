# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from datetime import datetime, timedelta, timezone
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import unquote, urlparse

import boto3

from materialize import scratch

MAX_AGE = timedelta(hours=1)

# The `reason` tag that test/terraform/mzcompose.py stamps on everything the
# AWS Terraform tests create. That harness also sets `deleteAfter`, which is
# what decides expiry here.
TERRAFORM_TEST_REASON = "materialize test/terraform CI run"


def clean_up_kinesis() -> None:
    print(f"Deleting Kinesis streams whose age exceeds {MAX_AGE}")
    client = boto3.client("kinesis")
    streams = client.list_streams()["StreamNames"]
    for stream in streams:
        if not stream.startswith("testdrive"):
            print("Skipping non-testdrive stream {}", stream)
            continue
        desc = client.describe_stream(StreamName=stream)
        created_at = desc["StreamDescription"]["StreamCreationTimestamp"]
        age = datetime.now(timezone.utc) - created_at
        if age <= MAX_AGE:
            print(f"Skipping stream {stream} whose age is beneath threshold")
            continue
        print(f"Deleting Kinesis stream {stream!r} (age={age})")
        client.delete_stream(StreamName=stream)


def clean_up_s3() -> None:
    print(f"Deleting S3 buckets whose age exceeds {MAX_AGE}")
    client = boto3.client("s3")
    buckets = client.list_buckets()["Buckets"]
    for desc in buckets:
        if not desc["Name"].startswith("testdrive"):
            print("Skipping non-testdrive bucket {}".format(desc["Name"]))
            continue
        age = datetime.now(timezone.utc) - desc["CreationDate"]
        if age <= MAX_AGE:
            print(
                "Skipping bucket {} whose age is beneath threshold".format(desc["Name"])
            )
            continue
        print("Deleting bucket {} (age={})".format(desc["Name"], age))
        try:
            bucket = boto3.resource("s3").Bucket(desc["Name"])
            bucket.objects.all().delete()
            bucket.delete()
        except client.exceptions.NoSuchBucket:
            print(
                f"Couldn't delete {desc['Name']}: NoSuchBucket. This might be a transient issue."
            )


def clean_up_sqs() -> None:
    print(f"Deleting SQS queues whose age exceeds {MAX_AGE}")
    client = boto3.client("sqs")
    queues = client.list_queues()
    if "QueueUrls" in queues:
        for queue in queues["QueueUrls"]:
            name = PurePosixPath(unquote(urlparse(queue).path)).parts[2]
            if not name.startswith("testdrive"):
                print(f"Skipping non-testdrive queue {name}")
                continue
            attributes = client.get_queue_attributes(
                QueueUrl=queue, AttributeNames=["All"]
            )
            created_at = int(attributes["Attributes"]["CreatedTimestamp"])
            age = datetime.now(timezone.utc) - datetime.fromtimestamp(
                created_at, timezone.utc
            )
            if age <= MAX_AGE:
                print(f"Skipping queue {name} whose age is beneath threshold")
                continue
            print(f"Deleting SQS queue {name} (age={age})")
            client.delete_queue(QueueUrl=queue)


def clean_up_ec2() -> None:
    print("Terminating scratch ec2 instances whose age exceeds the deletion time")
    olds = [i["InstanceId"] for i in scratch.get_old_instances()]
    if olds:
        print(f"Instances to delete: {olds}")
        boto3.client("ec2").terminate_instances(InstanceIds=olds)
    else:
        print("No instances to delete")


def clean_up_iam() -> None:
    client = boto3.client("iam")
    roles = get_testdrive_roles(client)

    if not roles:
        print("No testdrive IAM roles found")
        return

    now = datetime.utcnow().timestamp()

    print(f"Found {len(roles)} candidate IAM roles for deletion")
    for role in roles:
        used = role.get("RoleLastUsed", {}).get("LastUsedDate")
        if used is None:
            used = role["CreateDate"]

        role_name = role["RoleName"]
        expiration = (used + MAX_AGE).timestamp()
        if now > expiration:
            policy_response = client.list_role_policies(RoleName=role_name)
            for policy_name in policy_response.get("PolicyNames", []):
                client.delete_role_policy(RoleName=role_name, PolicyName=policy_name)

            client.delete_role(RoleName=role_name)
            print(f"Deleted role {role_name}")
        else:
            print(f"Skipping role {role_name}")


def get_testdrive_roles(client: Any) -> list[Any]:
    roles = []

    paginator = client.get_paginator("list_roles")
    page_iterator = paginator.paginate()

    for page in page_iterator:
        roles.extend(page.get("Roles", []))

    return [r for r in roles if r["RoleName"].startswith("testdrive")]


def expired_terraform_test_resources() -> dict[str, list[str]]:
    """Resource ids of expired AWS Terraform test resources, keyed by ARN type.

    Only resources whose `deleteAfter` has passed are returned. The
    aws-persistent root sets `deleteAfter` to 2099, so it never expires and is
    never a candidate here.
    """
    now = datetime.now(timezone.utc)
    by_type: dict[str, list[str]] = {}

    paginator = boto3.client("resourcegroupstaggingapi").get_paginator("get_resources")
    for page in paginator.paginate(
        TagFilters=[{"Key": "reason", "Values": [TERRAFORM_TEST_REASON]}]
    ):
        for resource in page["ResourceTagMappingList"]:
            tags = {t["Key"]: t["Value"] for t in resource["Tags"]}
            delete_after = tags.get("deleteAfter")
            if delete_after is None:
                continue
            try:
                expires = datetime.strptime(delete_after, "%Y-%m-%dT%H:%M:%SZ").replace(
                    tzinfo=timezone.utc
                )
            except ValueError:
                print(f"Skipping {resource['ResourceARN']}: bad deleteAfter tag")
                continue
            if now <= expires:
                continue
            # The resource portion of an ARN is `<type>/<id>` for EC2 and EKS
            # and `<type>:<id>` for CloudWatch Logs.
            resource_part = resource["ResourceARN"].split(":", 5)[5]
            kind, _, ident = resource_part.replace(":", "/", 1).partition("/")
            by_type.setdefault(kind, []).append(ident)

    return by_type


def clean_up_terraform_tests() -> None:
    """Delete what a failed AWS Terraform test run leaked.

    The test roots use a fixed name prefix, so a leaked cluster or log group
    fails every later run on "already exists", and the leaked VPC bills a NAT
    gateway and a dozen interface endpoints in the meantime. The run's own
    teardown handles this when it gets to run at all; this covers the case
    where the job died before it could.

    Best effort throughout. Deletions have ordering constraints that can need
    more than one pass, and a leftover that needs the next run is much better
    than a cleanup step that goes red.
    """
    print(f"Deleting expired resources tagged {TERRAFORM_TEST_REASON!r}")
    expired = expired_terraform_test_resources()
    if not expired:
        print("Nothing expired")
        return

    ec2 = boto3.client("ec2")
    eks = boto3.client("eks")
    logs = boto3.client("logs")
    kms = boto3.client("kms")

    def attempt(what: str, thunk: Any) -> None:
        try:
            thunk()
            print(f"Deleted {what}")
        except Exception as e:
            print(f"Could not delete {what}, leaving it for the next run: {e}")

    # Node groups first: a cluster with one attached cannot be deleted, which
    # is the failure mode that leaks the whole environment in the first place.
    for cluster in expired.get("cluster", []):
        try:
            node_groups = eks.list_nodegroups(clusterName=cluster)["nodegroups"]
        except Exception as e:
            print(f"Could not list node groups of {cluster}: {e}")
            continue
        for node_group in node_groups:
            attempt(
                f"node group {cluster}/{node_group}",
                lambda c=cluster, n=node_group: eks.delete_nodegroup(
                    clusterName=c, nodegroupName=n
                ),
            )
        for node_group in node_groups:
            try:
                eks.get_waiter("nodegroup_deleted").wait(
                    clusterName=cluster,
                    nodegroupName=node_group,
                    WaiterConfig={"Delay": 15, "MaxAttempts": 40},
                )
            except Exception as e:
                print(f"Node group {cluster}/{node_group} still deleting: {e}")

    for cluster in expired.get("cluster", []):
        attempt(f"EKS cluster {cluster}", lambda c=cluster: eks.delete_cluster(name=c))

    # After the cluster, so control plane logging cannot recreate the group
    # while the cluster drains.
    for log_group in expired.get("log-group", []):
        attempt(
            f"log group {log_group}",
            lambda g=log_group: logs.delete_log_group(logGroupName=g),
        )

    instance_ids = expired.get("instance", [])
    if instance_ids:
        attempt(
            f"instances {instance_ids}",
            lambda i=instance_ids: ec2.terminate_instances(InstanceIds=i),
        )

    for endpoint in expired.get("vpc-endpoint", []):
        attempt(
            f"VPC endpoint {endpoint}",
            lambda e=endpoint: ec2.delete_vpc_endpoints(VpcEndpointIds=[e]),
        )

    for nat in expired.get("natgateway", []):
        attempt(
            f"NAT gateway {nat}", lambda n=nat: ec2.delete_nat_gateway(NatGatewayId=n)
        )
    for nat in expired.get("natgateway", []):
        try:
            ec2.get_waiter("nat_gateway_deleted").wait(
                NatGatewayIds=[nat], WaiterConfig={"Delay": 15, "MaxAttempts": 40}
            )
        except Exception as e:
            print(f"NAT gateway {nat} still deleting: {e}")

    # Unattached addresses bill by the hour, and the NAT deletes above are what
    # release them, so this has to come after.
    for address in expired.get("elastic-ip", []):
        attempt(
            f"elastic IP {address}",
            lambda a=address: ec2.release_address(AllocationId=a),
        )

    for template in expired.get("launch-template", []):
        attempt(
            f"launch template {template}",
            lambda t=template: ec2.delete_launch_template(LaunchTemplateId=t),
        )

    for interface in expired.get("network-interface", []):
        attempt(
            f"network interface {interface}",
            lambda i=interface: ec2.delete_network_interface(NetworkInterfaceId=i),
        )

    for gateway in expired.get("internet-gateway", []):
        # Detaching needs the VPC it is actually attached to, which is not
        # necessarily one of the expired ones.
        try:
            described = ec2.describe_internet_gateways(InternetGatewayIds=[gateway])
            attachments = described["InternetGateways"][0]["Attachments"]
        except Exception as e:
            print(f"Could not describe internet gateway {gateway}: {e}")
            attachments = []
        for attachment in attachments:
            attempt(
                f"internet gateway {gateway} from {attachment['VpcId']}",
                lambda g=gateway, v=attachment["VpcId"]: ec2.detach_internet_gateway(
                    InternetGatewayId=g, VpcId=v
                ),
            )
        attempt(
            f"internet gateway {gateway}",
            lambda g=gateway: ec2.delete_internet_gateway(InternetGatewayId=g),
        )

    for subnet in expired.get("subnet", []):
        attempt(f"subnet {subnet}", lambda s=subnet: ec2.delete_subnet(SubnetId=s))

    for group in expired.get("security-group", []):
        attempt(
            f"security group {group}",
            lambda g=group: ec2.delete_security_group(GroupId=g),
        )

    for table in expired.get("route-table", []):
        attempt(
            f"route table {table}",
            lambda t=table: ec2.delete_route_table(RouteTableId=t),
        )

    for vpc in expired.get("vpc", []):
        attempt(f"VPC {vpc}", lambda v=vpc: ec2.delete_vpc(VpcId=v))

    # A key can only be scheduled for deletion, and 7 days is the minimum.
    for key in expired.get("key", []):
        attempt(
            f"KMS key {key}",
            lambda k=key: kms.schedule_key_deletion(KeyId=k, PendingWindowInDays=7),
        )


def main() -> None:
    clean_up_kinesis()
    clean_up_s3()
    clean_up_sqs()
    clean_up_ec2()
    clean_up_iam()
    clean_up_terraform_tests()


if __name__ == "__main__":
    main()
