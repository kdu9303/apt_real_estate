import os
from pathlib import Path
from airflow.decorators import dag
from pendulum import datetime
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from airflow.sdk import Asset
from src.utils import send_failure_alert

rtms_svc_apt_trade_asset = Asset("rtms_svc_apt_trade_asset")

DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/dbt/dbt_real_estate"
DBT_EXECUTABLE_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"

profile_config = ProfileConfig(
    profile_name="dbt_real_estate",
    target_name="airflow",
    profiles_yml_filepath=f"{os.environ['AIRFLOW_HOME']}/dags/dbt/dbt_real_estate/profiles.yml",
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)

@dag(
    start_date=datetime(2025, 1, 1),
    schedule=[rtms_svc_apt_trade_asset],  # Asset 기반 스케줄링
    catchup=False,
    on_failure_callback=send_failure_alert,
    tags=["real-estate", "dbt", "rtms_svc_apt_trade"],
    default_args={"owner": "data-eng", "retries": 1},
    doc_md="""
    ### dbt 아파트 매매 실거래가 데이터 변환 DAG
    아파트 매매 실거래가 데이터 수집 완료 후 dbt 변환 작업을 수행하는 DAG입니다.
    Asset 기반 스케줄링으로 rtms_svc_apt_trade DAG 완료 시 자동 실행됩니다.
    """,
)
def dbt_rtms_svc_apt_trade():
    
    staging_apt_trade = DbtTaskGroup(
        group_id="staging_apt_trade",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            select=["tag:staging_apt_trade"],
        )
    )
    
    fct_apt_trade = DbtTaskGroup(
        group_id="fct_apt_trade",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            select=["tag:fct_apt_trade"],
        )
    )
    
    staging_apt_trade >> fct_apt_trade
    
dbt_rtms_svc_apt_trade()