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
from urllib.parse import unquote, urlparse

import boto3

from materialize import scratch

MAX_AGE = timedelta(hours=1)


def clean_up_kinesis() -> None:
    print("Deleting Kinesis streams whose age exceeds {}".format(MAX_AGE))
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
            print("Skipping stream {} whose age is beneath threshold".format(stream))
            continue
        print("Deleting Kinesis stream {!r} (age={})".format(stream, age))
        client.delete_stream(StreamName=stream)


def clean_up_s3() -> None:
    print("Deleting S3 buckets whose age exceeds {}".format(MAX_AGE))
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
    print("Deleting SQS queues whose age exceeds {}".format(MAX_AGE))
    client = boto3.client("sqs")
    queues = client.list_queues()
    if "QueueUrls" in queues:
        for queue in queues["QueueUrls"]:
            name = PurePosixPath(unquote(urlparse(queue).path)).parts[2]
            if not name.startswith("testdrive"):
                print("Skipping non-testdrive queue {}".format(name))
                continue
            attributes = client.get_queue_attributes(
                QueueUrl=queue, AttributeNames=["All"]
            )
            created_at = int(attributes["Attributes"]["CreatedTimestamp"])
            age = datetime.now(timezone.utc) - datetime.fromtimestamp(
                created_at, timezone.utc
            )
            if age <= MAX_AGE:
                print("Skipping queue {} whose age is beneath threshold".format(name))
                continue
            print("Deleting SQS queue {} (age={})".format(name, age))
            client.delete_queue(QueueUrl=queue)


def clean_up_ec2() -> None:
    print(f"Terminating scratch ec2 instances whose age exceeds the deletion time")
    olds = [i["InstanceId"] for i in scratch.get_old_instances()]
    if olds:
        print(f"Instances to delete: {olds}")
        boto3.client("ec2").terminate_instances(InstanceIds=olds)
    else:
        print("No instances to delete")


def clean_up_iam() -> None:
    client = boto3.client("iam")
    response = client.list_roles()
    roles = response.get("Roles", [])

    roles = [r for r in roles if r["RoleName"].startswith("testdrive")]

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


def main() -> None:
    clean_up_kinesis()
    clean_up_s3()
    clean_up_sqs()
    clean_up_ec2()
    clean_up_iam()


if __name__ == "__main__":
    main()
