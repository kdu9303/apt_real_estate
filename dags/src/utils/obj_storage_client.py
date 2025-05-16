import os
import duckdb
from dotenv import load_dotenv
from utils.util import read_file_path

load_dotenv()


def upload_data_to_obj_storage(
    bucket_name: str,
    dir_path: str,
    file_name: str,
    partition_key: str,
    endpoint: str = "localhost:9000",
    storage_type: str = "minio",
):
    """
    DuckDB engine을 사용해 parquet 파일을 Object storage에 업로드합니다.
    """
    # 확장자 자동 추가
    if not file_name.endswith(".parquet"):
        file_name_with_ext = f"{file_name}.parquet"
    else:
        file_name_with_ext = file_name

    # DuckDB로 pyarrow Table 생성
    con = duckdb.connect()
    con.sql("INSTALL parquet;")
    con.sql("LOAD parquet;")
    con.sql("INSTALL httpfs;")
    con.sql("LOAD httpfs;")

    con.sql("SET s3_url_style = 'path';")
    con.sql(f"SET s3_endpoint='{endpoint}';")
    con.sql("SET s3_use_ssl = false;")

    if storage_type == "minio":
        con.sql(f"""
                CREATE SECRET (
                    TYPE s3,
                    KEY_ID '{os.getenv("MINIO_ACCESS_KEY")}',
                    SECRET '{os.getenv("MINIO_SECRET_KEY")}',
                    REGION 'us-east-1'
            );
            """)
    elif storage_type == "aws":
        con.sql(f"""
                CREATE SECRET (
                    TYPE s3,
                    KEY_ID '{os.getenv("AWS_ACCESS_KEY_ID")}',
                    SECRET '{os.getenv("AWS_SECRET_ACCESS_KEY")}',
                    REGION '{os.getenv("AWS_REGION")}'
            );
            """)

    local_file_path = read_file_path(file_name)

    # 업로드 경로
    s3_path = f"s3://{bucket_name}/{dir_path}/{partition_key}/{file_name_with_ext}"

    # DuckDB COPY 명령으로 parquet 파일 업로드
    con.sql(
        f"COPY (SELECT * FROM read_parquet('{local_file_path}')) TO '{s3_path}' (FORMAT PARQUET);"
    )

    print(f"파일이 Minio에 적재되었습니다: {s3_path}")
