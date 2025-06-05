# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os
import logging
import polars as pl
from pendulum import datetime
from airflow.hooks.base import BaseHook
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.sdk import Asset
from pyiceberg.catalog.glue import GlueCatalog
from src.naver_land_complex_info import NaverLandComplexInfoList
from src.naver_land_complex_info_detail import NaverLandComplexInfoDetailList
from src.utils import (
    fetch_iceberg_table_to_polars,
    upload_data_to_obj_storage_polars,
    SGG_CD_DICT,
)

# Asset 정의
naver_land_complex_info_asset = Asset("naver_land_complex_info_asset")

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

os.environ["AWS_DEFAULT_REGION"] = "ap-northeast-2"

AWS_CREDS = BaseHook.get_connection("aws-conn")
STORAGE_OPTIONS = {
    "s3.region": os.getenv("AWS_DEFAULT_REGION"),
    "s3.access-key-id": AWS_CREDS.login,
    "s3.secret-access-key": AWS_CREDS.password,
}
CATALOG = GlueCatalog(
    "glue",
    **{
        "uri": "https://glue.ap-northeast-2.amazonaws.com",
        "region_name": os.getenv("AWS_DEFAULT_REGION"),
        "s3.access-key-id": AWS_CREDS.login,
        "s3.secret-access-key": AWS_CREDS.password,
    },
)


@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["real-estate", "naver_land_complex_info"],
    default_args={"owner": "data-eng", "retries": 1},
    doc_md="""
    ### 네이버 부동산 단지 정보 적재 DAG
    네이버 부동산 단지 정보를 수집 및 적재하는 DAG입니다.
    """,
)
def naver_land_complex_info():
    with TaskGroup(group_id="scrape_naver_land_complex_info") as scraping_tasks:

        @task
        def fetch_load_naver_land_complex_info():
            sggCd_list = list(SGG_CD_DICT.values())

            dim_stan_regin_cd_df = fetch_iceberg_table_to_polars(
                catalog=CATALOG,
                namespace="mart-real-estate",
                table_name="dim_stan_regin_cd",
                storage_options=STORAGE_OPTIONS,
            )

            naver_land_complex_list = NaverLandComplexInfoList()

            for sggCd in sggCd_list:
                address_df = (
                    dim_stan_regin_cd_df.select(
                        pl.col("region_cd"),
                        pl.col("locathigh_cd"),
                    )
                    .filter(pl.col("locathigh_cd") == str(sggCd))
                    .collect()
                )

                region_list = (
                    address_df.select(pl.col("region_cd")).to_series().to_list()
                )

                results_df = naver_land_complex_list.run_async_task(region_list)

                file_name = f"naver_land_complex_info_{sggCd}"

                upload_data_to_obj_storage_polars(
                    df=results_df,
                    endpoint_type="aws",
                    bucket_name="real-estate-raw",
                    dir_path="naver_land_complex_info",
                    file_name=file_name,
                    key=AWS_CREDS.login,
                    secret=AWS_CREDS.password,
                    partition_key=sggCd,
                )

                logger.info(f"{file_name} 처리 완료")

        @task
        def fetch_load_naver_land_complex_info_detail():
            sggCd_list = list(SGG_CD_DICT.values())

            naver_land_complex_detail_list = NaverLandComplexInfoDetailList()

            land_geo_df = fetch_iceberg_table_to_polars(
                catalog=CATALOG,
                namespace="mart-real-estate",
                table_name="dim_naver_land_geo_info",
                storage_options=STORAGE_OPTIONS,
            )

            for sggCd in sggCd_list:
                land_geo_df_filtered = (
                    land_geo_df.select(
                        pl.col("cortarno"),
                        pl.col("centerlat"),
                        pl.col("centerlon"),
                        pl.col("region_cd"),
                    )
                    .filter(pl.col("region_cd") == str(sggCd))
                    .collect()
                )

                region_list: list[dict] = land_geo_df_filtered.to_dicts()

                results_df = naver_land_complex_detail_list.run_async_task(region_list)

                file_name = f"naver_land_complex_info_detail_{sggCd}"

                upload_data_to_obj_storage_polars(
                    df=results_df,
                    endpoint_type="aws",
                    bucket_name="real-estate-raw",
                    dir_path="naver_land_complex_info_detail",
                    file_name=file_name,
                    key=AWS_CREDS.login,
                    secret=AWS_CREDS.password,
                    partition_key=sggCd,
                )

                logger.info(f"{file_name} 처리 완료")

        (
            fetch_load_naver_land_complex_info()
            >> fetch_load_naver_land_complex_info_detail()
        )

    with TaskGroup(group_id="trigger_glue_crawler") as trigger_glue_crawler:
        
        config = {
            "Name": "real-estate-raw-crawler",
        }
        
        # Asset을 업데이트하는 Glue Crawler 태스크
        glue_crawler_task = GlueCrawlerOperator(
            task_id="glue_crawler_task",
            config=config,
            aws_conn_id="aws-conn",
            region_name=os.getenv("AWS_DEFAULT_REGION"),
            outlets=[naver_land_complex_info_asset],  # Asset 업데이트
        )

        glue_crawler_task

    # task 그룹 순서 지정
    scraping_tasks >> trigger_glue_crawler


naver_land_complex_info()
