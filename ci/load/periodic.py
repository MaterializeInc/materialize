import datetime

from materialize.cli.scratch import DEFAULT_INSTPROF_NAME, DEFAULT_SG_ID, DEFAULT_SUBNET_ID

from materialize import scratch


def main() -> None:
    desc = scratch.MachineDesc(
        name="chbench monthly",
        launch_script="MZ_WORKERS=4 bin/mzcompose --mz-find chbench run cloud-load-test",
        instance_type="r5ad.4xlarge",
        ami="ami-0b29b6e62f2343b46",
        tags={
            "scrape_benchmark_numbers": "true",
            "lt_name": "monthly-chbench",
            "purpose": "load_test_monthly",
            "mzconduct_workflow": "cloud-load-test",
            "test": "chbench",
            "environment": "scratch",
        },
        size_gb=64,
    )
    now = datetime.datetime.utcnow()
    scratch.launch_cluster(
        [desc],
        now.replace(tzinfo=datetime.timezone.utc).isoformat(),
        DEFAULT_SUBNET_ID,
        None,
        DEFAULT_SG_ID,
        DEFAULT_INSTPROF_NAME,
        {},
        # Keep alive for at least a day
        int(datetime.datetime.now(datetime.timezone.utc).timestamp()) + 3600 * 24,
    )


if __name__ == "__main__":
    main()
