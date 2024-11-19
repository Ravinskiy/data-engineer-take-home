from dagster import AssetExecutionContext, asset, job, op
from dagster_dbt import DbtCliResource, load_assets_from_dbt_project, dbt_cli_resource

@asset
def create_dbt_manifest(context: AssetExecutionContext, dbt: DbtCliResource):
    """Run dbt commands to create manifest file"""
    dbt.cli(["deps"], context=context).wait()
    dbt.cli(["compile"], context=context).wait()
    return "Manifest created successfully"


# Explicitly specify the dbt_resource_key
dbt_assets = load_assets_from_dbt_project(
    project_dir="/opt/dagster/app/abs_dbt",
    profiles_dir="/root/.dbt",
    dbt_resource_key="dbt"
)

dbt_resource = dbt_cli_resource.configured({
    "project_dir": "/opt/dagster/app/abs_dbt",
    "profiles_dir": "/root/.dbt",
})

@job
def create_manifest_job():
    create_dbt_manifest()


@op(required_resource_keys={"dbt"})
def dbt_run_transaction_logs(context):
    context.resources.dbt.run(select=["joined_txs"])

@job(resource_defs={"dbt": dbt_resource})
def transaction_logs_job():
    dbt_run_transaction_logs()


@asset
def example_asset():
    """A simple example asset"""
    return {"hello": "world"}
