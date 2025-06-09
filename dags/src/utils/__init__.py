import os
import sys

# 현재 파일의 위치를 기준으로 프로젝트 루트 찾기
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.dirname(current_dir)  # src 디렉토리
dags_dir = os.path.dirname(src_dir)     # dags 디렉토리
project_root = os.path.dirname(dags_dir)  # 프로젝트 루트

# sys.path에 필요한 경로들 추가
paths_to_add = [project_root, dags_dir]
for path in paths_to_add:
    if path not in sys.path:
        sys.path.insert(0, path)

# utils 모듈들을 import
from .util import (
    create_yyyymm_form_date_list,
    create_hash_key,
    save_file_to_local,
    remove_file_from_local,
    read_file_path,
)
from .obj_storage_client import (
    upload_data_to_obj_storage_polars,
    trigger_aws_glue_crawler,
    read_data_from_obj_storage_polars,
)
from .iceberg_rest_client import create_catalog, fetch_iceberg_table_to_polars
from .const import SGG_CD_DICT

__all__ = [
    "create_yyyymm_form_date_list",
    "create_hash_key", 
    "save_file_to_local",
    "remove_file_from_local",
    "read_file_path",
    "upload_data_to_obj_storage_polars",
    "trigger_aws_glue_crawler",
    "read_data_from_obj_storage_polars",
    "fetch_iceberg_table_to_polars",
    "create_catalog",
    "SGG_CD_DICT",
]
