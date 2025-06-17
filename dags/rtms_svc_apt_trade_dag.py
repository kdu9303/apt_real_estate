import os
import logging
import polars as pl
from pendulum import datetime
from airflow.hooks.base import BaseHook
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.sdk import Asset
from airflow.models import Variable
from pyiceberg.catalog.glue import GlueCatalog
from src.rtms_svc_apt_trade import AptTradeList
from src.utils import (
    upload_data_to_obj_storage_polars,
    SGG_CD_DICT,
    send_failure_alert,
)


# Asset 정의
rtms_svc_apt_trade_asset = Asset("rtms_svc_apt_trade_asset")

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
    start_date=datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule="0 20 * * 3,6",  # 매주 수요일(3), 토요일(6) 저녁 8시(20시)
    catchup=False,
    on_failure_callback=send_failure_alert,
    tags=["real-estate", "rtms_svc_apt_trade"],
    default_args={"owner": "data-eng", "retries": 1},
    doc_md="""
    ### OPEN API 아파트 매매 실거래가 데이터 적재 DAG
    아파트 매매 실거래가 데이터를 수집 및 적재하는 DAG입니다.
    """,
)
def rtms_svc_apt_trade():
    
    # 기준 년도
    year = 2025
    
    @task
    def fetch_load_rtms_svc_apt_trade():
        
        apt_list = AptTradeList(Variable.get("openapi_api_key"))
    
        for sgg_name, sggCd in SGG_CD_DICT.items():
            sggCd = sggCd[:5]
            results_df = apt_list.concat_apt_trade_data_list(year, sggCd)
            
            if results_df.shape[0] == 0:
                continue
            
            file_name = f"apt_trade_{sgg_name}_{year}"
            
            upload_data_to_obj_storage_polars(
                    df=results_df,
                    endpoint_type="aws",
                    bucket_name="real-estate-raw",
                    dir_path="apt_trade",
                    file_name=file_name,
                    key=AWS_CREDS.login,
                    secret=AWS_CREDS.password,
                    partition_key=year,
                )
            logger.info(f"{file_name} 처리 완료")
    
    config = {
            "Name": "real-estate-raw-crawler",
        }
    
    glue_crawler_task = GlueCrawlerOperator(
            task_id="glue_crawler_task",
            config=config,
            aws_conn_id="aws-conn",
            region_name=os.getenv("AWS_DEFAULT_REGION"),
            outlets=[rtms_svc_apt_trade_asset],  # Asset 업데이트
        )
    
    fetch_load_rtms_svc_apt_trade() >> glue_crawler_task

rtms_svc_apt_trade()

