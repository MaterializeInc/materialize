from pathlib import Path
import subprocess

import pulumi
import pulumi_aws as aws
import pulumi_awsx as awsx
import pulumi_command as command

root = Path(__file__).parent

project_name = pulumi.get_project()
stack_name = pulumi.get_stack()
full_name = f"{project_name}-{stack_name}"

default_tags = {
    "Project": project_name,
    "Stack": stack_name,
}

ssh_key_path = root / "ssh_key"
if not ssh_key_path.exists():
    subprocess.run(["ssh-keygen", "-f", ssh_key_path, "-q", "-N", ""], check=True)
ssh_public_key = ssh_key_path.with_suffix(".pub").read_text()
ssh_private_key = ssh_key_path.read_text()


_azs = aws.get_availability_zones()
availability_zones = {name: id for (name, id) in zip(_azs.names, _azs.zone_ids)}

vpc = awsx.ec2.Vpc(
    "default",
    nat_gateways=awsx.ec2.NatGatewayConfigurationArgs(
        strategy=awsx.ec2.NatGatewayStrategy.SINGLE,
    ),
    tags={
        "Name": full_name,
        **default_tags,
    },
)

allow_all_sg = aws.ec2.SecurityGroup(
    "allow-all",
    vpc_id=vpc.vpc_id,
    name=f"{project_name}-allow-all",
    tags={
        "Name": full_name,
        **default_tags
    },
)

aws.ec2.SecurityGroupRule(
    "allow-all-ingress",
    type="ingress",
    protocol="-1",
    cidr_blocks=["0.0.0.0/0"],
    ipv6_cidr_blocks=["::/0"],
    to_port=0,
    from_port=0,
    security_group_id=allow_all_sg.id,
)

aws.ec2.SecurityGroupRule(
    "allow-all-egress",
    type="egress",
    protocol="-1",
    cidr_blocks=["0.0.0.0/0"],
    ipv6_cidr_blocks=["::/0"],
    to_port=0,
    from_port=0,
    security_group_id=allow_all_sg.id,
)

load_balancer = aws.lb.LoadBalancer(
    "default",
    name=full_name,
    load_balancer_type="network",
    subnets=vpc.private_subnet_ids,
    internal=True,
    tags=default_tags,
)

endpoint_service = aws.ec2.VpcEndpointService(
    "default",
    acceptance_required=False,
    # TODO: this is insecure. Scope to a single role.
    allowed_principals=["arn:aws:iam::664411391173:root"],
    network_load_balancer_arns=[load_balancer.arn],
    tags=default_tags,
)

kafka_cluster = aws.msk.Cluster(
    "default",
    kafka_version="3.2.0",
    number_of_broker_nodes=6,
    cluster_name=full_name,
    encryption_info=aws.msk.ClusterEncryptionInfoArgs(
        encryption_in_transit=aws.msk.ClusterEncryptionInfoEncryptionInTransitArgs(
            client_broker="TLS_PLAINTEXT",
        )
    ),
    broker_node_group_info=aws.msk.ClusterBrokerNodeGroupInfoArgs(
        instance_type="kafka.t3.small",
        client_subnets=[
            vpc.private_subnet_ids[0],
            vpc.private_subnet_ids[1],
        ],
        storage_info=aws.msk.ClusterBrokerNodeGroupInfoStorageInfoArgs(
            ebs_storage_info=aws.msk.ClusterBrokerNodeGroupInfoStorageInfoEbsStorageInfoArgs(
                volume_size=100,
            ),
        ),
        security_groups=[allow_all_sg.id],
    ),
    tags=default_tags,
)

def handle_kafka_brokers(result: aws.msk.GetBrokerNodesResult):
    base_port = 9092
    for broker in result.node_info_lists:
        target_group = aws.lb.TargetGroup(
            f"kafka-{broker.broker_id}",
            name_prefix=f"kafka",
            protocol="TCP",
            port=9092,
            target_type="ip",
            vpc_id=vpc.vpc_id,
            tags=default_tags,
        )
        aws.lb.TargetGroupAttachment(
            f"kafka-{broker.broker_id}",
            target_group_arn=target_group.arn,
            target_id=broker.client_vpc_ip_address,
        )
        aws.lb.Listener(
            f"kafka-{broker.broker_id}",
            load_balancer_arn=load_balancer.arn,
            protocol="TCP",
            port=base_port,
            default_actions=[aws.lb.ListenerDefaultActionArgs(
                type="forward",
                target_group_arn=target_group.arn,
            )]
        )
        base_port += 1


aws.msk.get_broker_nodes_output(kafka_cluster.arn).apply(handle_kafka_brokers)

ecs_cluster = aws.ecs.Cluster(
    "default",
    name=full_name,
)

schema_registry_target_group = aws.lb.TargetGroup(
    "schema-registry",
    name_prefix=f"sr",
    protocol="TCP",
    port=8081,
    target_type="ip",
    vpc_id=vpc.vpc_id,
    tags=default_tags,
)

schema_registry_listener = aws.lb.Listener(
    "schema-registry",
    load_balancer_arn=load_balancer.arn,
    protocol="TCP",
    port=8081,
    default_actions=[aws.lb.ListenerDefaultActionArgs(
        type="forward",
        target_group_arn=schema_registry_target_group.arn,
    )]
)

schema_registry_service = awsx.ecs.FargateService("schema-registry",
    cluster=ecs_cluster.arn,
    desired_count=1,
    load_balancers=[
        aws.ecs.ServiceLoadBalancerArgs(
            container_name="container",
            container_port=8081,
            target_group_arn=schema_registry_target_group.arn,
        )
    ],
    task_definition_args=awsx.ecs.FargateServiceTaskDefinitionArgs(
        container=awsx.ecs.TaskDefinitionContainerDefinitionArgs(
            image="confluentinc/cp-schema-registry:7.3.0",
            cpu=1024,
            memory=1024,
            essential=True,
            environment=[
                awsx.ecs.TaskDefinitionKeyValuePairArgs(
                    name="SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                    value=kafka_cluster.bootstrap_brokers,
                ),
                awsx.ecs.TaskDefinitionKeyValuePairArgs(
                    name="SCHEMA_REGISTRY_HOST_NAME",
                    value="localhost",
                ),
                # awsx.ecs.TaskDefinitionKeyValuePairArgs(
                #     name="SCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL",
                #     value="SSL",
                # ),
            ],
            port_mappings=[awsx.ecs.TaskDefinitionPortMappingArgs(
                container_port=8081,
            )],
        ),
    ),
    network_configuration=aws.ecs.ServiceNetworkConfigurationArgs(
        subnets=[
            vpc.private_subnet_ids[0],
        ],
        security_groups=[allow_all_sg.id],
    ),
    opts=pulumi.ResourceOptions(
        depends_on=[schema_registry_listener],
    )
)

postgres_parameter_group = aws.rds.ParameterGroup(
    "default",
    name=full_name,
    family="postgres14",
    parameters=[
        aws.rds.ParameterGroupParameterArgs(
            name="rds.logical_replication",
            value="1",
            apply_method="pending-reboot",
        )
    ],
    tags=default_tags,
)

postgres_subnet_group = aws.rds.SubnetGroup(
    "default",
    name=full_name,
    subnet_ids=[
        vpc.private_subnet_ids[0],
        vpc.private_subnet_ids[1]
    ],
    tags=default_tags,
)

postgres = aws.rds.Instance(
    "default",
    allocated_storage=100,
    db_name="testdb",
    db_subnet_group_name=postgres_subnet_group.name,
    engine="postgres",
    engine_version="14.4",
    instance_class="db.t3.micro",
    parameter_group_name=postgres_parameter_group.name,
    skip_final_snapshot=True,
    username="testuser",
    password="testpass",
    tags=default_tags,
    vpc_security_group_ids=[allow_all_sg.id],
)

bastion_key_pair = aws.ec2.KeyPair(
    "default",
    key_name=full_name,
    public_key=ssh_public_key,
)

bastion = aws.ec2.Instance(
    "default",
    vpc_security_group_ids=[allow_all_sg.id],
    instance_type="t3.medium",
    # us-east-1, Ubuntu 22.04, amd64
    ami="ami-0096528c9fcc1a6a9",
    root_block_device=aws.ec2.InstanceRootBlockDeviceArgs(volume_size=64),
    subnet_id=vpc.public_subnet_ids[0],
    key_name=bastion_key_pair.key_name,
)

# command.remote.Command(
#     "default",
#     connection=command.remote.ConnectionArgs(
#         host=bastion.public_ip,
#         user="ubuntu",
#         private_key=ssh_private_key,
#     ),
#     create=(root / "provision.sh").read_text(),
# )

pulumi.export("bastion_ip", bastion.public_ip)
pulumi.export("bastion_instance_id", bastion.id)
pulumi.export("bastion_ssh_command", pulumi.Output.concat("ssh -i ssh_key ubuntu@", bastion.public_ip))
pulumi.export("kafka_bootstrap_servers", kafka_cluster.bootstrap_brokers)
pulumi.export("schema_registry_url", pulumi.Output.concat("http://", load_balancer.dns_name, ":8081"))
pulumi.export("vpc_endpoint_service_name", endpoint_service.service_name)
pulumi.export("vpc_endpoint_availability_zones", endpoint_service.availability_zones.apply(lambda azs: [availability_zones[az] for az in azs]))
