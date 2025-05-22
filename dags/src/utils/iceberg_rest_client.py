import os
from dotenv import load_dotenv
import polars as pl
from pyiceberg.catalog import load_catalog, Catalog
from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, IntegerType
from pyiceberg.exceptions import NoSuchTableError

load_dotenv()


def create_catalog(
    catalog_type: str = "iceberg-rest", rest_uri: str = "http://localhost:8181/"
):
    if catalog_type == "nessie":
        """
        uri: http://localhost:19120/iceberg
        """
        catalog = load_catalog(
            "rest",
            **{
                "uri": rest_uri,
                "s3.endpoint": "http://minio:9000",
                "s3.path-style-access": "true",
                "s3.access-key-id": "admin",
                "s3.secret-access-key": "admin1234",
                # "warehouse": "s3a://silver"
                # "aws.region": "us-east-1",
            },
        )
    elif catalog_type == "iceberg-rest":
        """
        uri: http://localhost:8181/
        """
        catalog = load_catalog(
            "rest",
            **{
                "uri": rest_uri,
                "s3.endpoint": "http://minio:9000",
            },
        )
    elif catalog_type == "glue":
        """
        uri: https://glue.us-east-1.amazonaws.com
        """
        catalog = GlueCatalog(
            "glue",
            **{
                "uri": "https://glue.ap-northeast-2.amazonaws.com",
                "region_name": "ap-northeast-2",
                "s3.access-key-id": os.getenv("AWS_ACCESS_KEY_ID"),
                "s3.secret-access-key": os.getenv("AWS_SECRET_ACCESS_KEY"),
            },
        )

    return catalog


def find_iceberg_metadata_path(
    catalog: Catalog, namespace: str, table_name: str
) -> str:
    """
    glue catalog에서 iceberg 테이블 메타데이터 조회
    """
    # 테이블 전체 이름 조합
    full_table_name = f"{namespace}.{table_name}"

    try:
        table = catalog.load_table(full_table_name)
        metadata_path = table.metadata_location
    except NoSuchTableError:
        print(f"Table {full_table_name} does not exist")

    return metadata_path


def fetch_iceberg_table_to_polars(
    catalog: Catalog,
    namespace: str,
    table_name: str,
    storage_options: dict,
) -> pl.LazyFrame:
    table_metadata_path = find_iceberg_metadata_path(
        catalog=catalog, namespace=namespace, table_name=table_name
    )

    df = pl.scan_iceberg(
        table_metadata_path,
        storage_options=storage_options,
    )

    return df


if __name__ == "__main__":
    catalog = create_catalog(catalog_type="glue")

    find_iceberg_metadata_path(
        catalog=catalog, namespace="mart-real-estate", table_name="dim_stan_regin_cd"
    )
