from materialize.mzcompose.service import (
    Service,
    ServiceConfig,
)


class PolarisBootstrap(Service):
    def __init__(
        self,
        name: str = "polaris-bootstrap",
        image: str = "apache/polaris-admin-tool",
        tag: str = "latest",
        environment: list[str] = [
            "POLARIS_BOOTSTRAP_CREDENTIALS=POLARIS,root,root",
            "POLARIS_PERSISTENCE_TYPE=relational-jdbc",
            "QUARKUS_DATASOURCE_USERNAME=postgres",
            "QUARKUS_DATASOURCE_PASSWORD=postgres",
            "QUARKUS_DATASOURCE_JDBC_URL=jdbc:postgresql://postgres:5432/postgres",
            "POLARIS_PERSISTENCE_RELATIONAL_JDBC_MAX_RETRIES=5",
            "POLARIS_PERSISTENCE_RELATIONAL_JDBC_INITIAL_DELAY_IN_MS=100",
            "POLARIS_PERSISTENCE_RELATIONAL_JDBC_MAX_DURATION_IN_MS=5000",
        ],
    ):
        config: ServiceConfig = {
            "image": f"{image}:{tag}",
            "environment": environment,
            "command": [
                "bootstrap",
                "--realm",
                "POLARIS",
                "--credential",
                "POLARIS,root,root",
            ],
            "depends_on": {"postgres": {"condition": "service_healthy"}},
        }
        super().__init__(name, config)


class Polaris(Service):
    def __init__(
        self,
        name: str = "polaris",
        image: str = "apache/polaris",
        tag: str = "latest",
        # 8181: api port, 8182: management port
        ports: list[str | int] = [8181, 8182],
        environment: list[str] = [
            "QUARKUS_DATASOURCE_USERNAME=postgres",
            "QUARKUS_DATASOURCE_PASSWORD=postgres",
            "QUARKUS_DATASOURCE_JDBC_URL=jdbc:postgresql://postgres:5432/postgres",
            "POLARIS_PERSISTENCE_TYPE=relational-jdbc",
            "POLARIS_PERSISTENCE_RELATIONAL_JDBC_MAX_RETRIES=5",
            "POLARIS_PERSISTENCE_RELATIONAL_JDBC_INITIAL_DELAY_IN_MS=100",
            "POLARIS_PERSISTENCE_RELATIONAL_JDBC_MAX_DURATION_IN_MS=5000",
            "POLARIS_BOOTSTRAP_CREDENTIALS=POLARIS,root,root",
            'polaris.features."ALLOW_INSECURE_STORAGE_TYPES"=true',
            'polaris.features."SUPPORTED_CATALOG_STORAGE_TYPES"=["FILE","S3","GCS","AZURE"]',
            "polaris.readiness.ignore-severe-issues=true",
            "AWS_REGION=minio",
        ],
        depends_on_extra: list[str] = [],
    ) -> None:
        config: ServiceConfig = {
            "image": f"{image}:{tag}",
            "ports": ports,
            "environment": environment,
            "depends_on": {
                "postgres": {"condition": "service_healthy"},
                "polaris-bootstrap": {"condition": "service_completed_successfully"},
                **{s: {"condition": "service_started"} for s in depends_on_extra},
            },
            "healthcheck": {
                "test": ["CMD", "curl", "http://localhost:8182/q/health"],
                "interval": "2s",
                "timeout": "10s",
                "retries": 10,
                "start_period": "10s",
            },
        }
        super().__init__(name, config)
