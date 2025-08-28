
from dagster import define_asset_job

full_pipeline_job = define_asset_job(
    name="full_pipeline",
    selection=[
        "meltano_ingestion",
        "*",  # all assets of dbt models
        "run_data_quality_tests",
        "exploratory_analysis",
    ],
)
