from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets import dbt_assets, create_dbt_manifest, create_manifest_job, transaction_logs_job

defs = Definitions(
    assets=[*dbt_assets, create_dbt_manifest],
    jobs=[create_manifest_job, transaction_logs_job],
    resources={
        "dbt": DbtCliResource(
            project_dir="/opt/dagster/app/abs_dbt",
            profiles_dir="/root/.dbt",
        ),
    },
)
