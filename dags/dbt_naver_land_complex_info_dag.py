import os
from pathlib import Path
from airflow.decorators import dag
from pendulum import datetime
from cosmos import DbtDag, DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from airflow.sdk import Asset

# Asset 정의 - naver_land_complex_info_dag.py와 동일한 Asset 참조
naver_land_complex_info_asset = Asset("naver_land_complex_info_asset")

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
    schedule=[naver_land_complex_info_asset],  # Asset 기반 스케줄링
    catchup=False,
    tags=["real-estate", "dbt", "naver_land_complex_info"],
    default_args={"owner": "data-eng", "retries": 1},
    doc_md="""
    ### dbt 네이버 부동산 단지 정보 변환 DAG
    네이버 부동산 단지 정보 수집 완료 후 dbt 변환 작업을 수행하는 DAG입니다.
    Asset 기반 스케줄링으로 naver_land_complex_info DAG 완료 시 자동 실행됩니다.
    """,
)
def dbt_naver_land_complex_info():
    
    staging_naver_land_complex_info = DbtTaskGroup(
        group_id="staging_naver_land_complex_info",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            select=["tag:staging_naver_land_complex_info"],
        )
    )
    
    snapshot_naver_land_complex_info = DbtTaskGroup(
        group_id="snapshot_naver_land_complex_info",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            select=["tag:snapshot_naver_land_complex_info"],
            dbt_deps=True,
        ),
        operator_args={
            # snapshot 멀티 스레드 실행 방지
            # Iceberg의 메타데이터 커밋 과정에서 충돌이 발생할 수 있음
            "dbt_cmd_flags": ["snapshot", "--select", "tag:snapshot_naver_land_complex_info", "--threads", "1"],
        }
    )
    
    dim_naver_land_complex_info = DbtTaskGroup(
        group_id="dim_naver_land_complex_info",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            select=["dim_naver_land_complex_info"],  # 단일 모델명 직접 지정
        )
    )
    
    # 의존성 설정
    staging_naver_land_complex_info >> snapshot_naver_land_complex_info >> dim_naver_land_complex_info
    
dbt_naver_land_complex_info()