import os
import duckdb
import s3fs
import boto3
import polars as pl
from dotenv import load_dotenv
from utils.util import read_file_path


load_dotenv()


def upload_data_to_obj_storage_duckdb(
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


def upload_data_to_obj_storage_polars(
    df: pl.DataFrame,
    bucket_name: str,
    dir_path: str,
    file_name: str,
    key: str,
    secret: str,
    endpoint_type: str = "minio",
    endpoint: str = "http://localhost:9000",
    partition_key: str = None,
):
    """
    partition_key -> column name
    """
    fs = s3fs.S3FileSystem(
        key=key,
        secret=secret,
        endpoint_url=endpoint if endpoint_type == "minio" else None,
    )
    s3_target_path = (
        f"s3://{bucket_name}/{dir_path}/{partition_key}/{file_name}.parquet"
    )

    if partition_key:
        with fs.open(s3_target_path, mode="wb", partition_by=partition_key) as f:
            df.write_parquet(f)
    else:
        with fs.open(s3_target_path, mode="wb") as f:
            df.write_parquet(f)

    print(f"파일이 {endpoint_type}에 적재되었습니다: {s3_target_path}")


def read_data_from_obj_storage_polars(
    s3_path: str,
    key: str,
    secret: str,
    aws_region: str = "us-east-1",
    endpoint: str = "http://localhost:9000",
) -> pl.DataFrame:
    storage_options = {
        "endpoint_url": endpoint,
        "aws_access_key_id": key,
        "aws_secret_access_key": secret,
        "aws_region": aws_region,
    }

    df = pl.scan_parquet(s3_path, storage_options=storage_options).collect()
    total_rows = df.shape[0]
    print(f"총 행 수: {total_rows}")
    return df


def trigger_aws_glue_crawler(
    crawler_name: str,
):
    region = os.getenv("AWS_REGION")
    glue = boto3.client("glue", region_name=region)
    try:
        response = glue.start_crawler(Name=crawler_name)
        print(f"Glue Crawler '{crawler_name}' started: {response}")
    except glue.exceptions.CrawlerRunningException:
        print(f"Glue Crawler '{crawler_name}' is already running.")
    except Exception as e:
        print(f"Glue Crawler '{crawler_name}' start failed: {e}")


if __name__ == "__main__":
    # s3_path = "s3://bronze/apt_trade/2021/apt_trade_송파구_2021.parquet"

    # s3_path = "s3://bronze/apt_trade/apt_trade_송파구_2021.parquet"
    # df = read_data_from_obj_storage_polars(
    #     s3_path=s3_path,
    #     key=os.getenv("MINIO_ACCESS_KEY"),
    #     secret=os.getenv("MINIO_SECRET_KEY"),
    # )
    # print(df.head(10))
    trigger_aws_glue_crawler(crawler_name="real-estate-rawcrawler")
