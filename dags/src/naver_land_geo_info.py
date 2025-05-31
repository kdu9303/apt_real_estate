import os
import json
import random
import asyncio
import logging
import polars as pl
from rich import print
from asyncio import sleep
from dotenv import load_dotenv
from dataclasses import dataclass
from curl_cffi.requests import AsyncSession
from utils import (
    chunk_list,
    create_hash_key,
    create_catalog,
    fetch_iceberg_table_to_polars,
    upload_data_to_obj_storage_polars,
    trigger_aws_glue_crawler,
)


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

load_dotenv()


@dataclass
class NaverLandGeoInfo:
    """
    네이버부동산의 동단위 위도경도정보
    """

    regionCd: str  # 시군구코드(10자리)
    cortarNo: str  # 법정동코드(10자리)
    centerLat: float  # 위도
    centerLon: float  # 경도
    cortarName: str  # 법정동명
    cortarType: str  # ???
    naver_land_geo_info_id: str = ""  # 유니크 해시 키(초기값 빈 문자열)


class NaverLandGeoInfoList:
    def __init__(self):
        self.base_url = "https://new.land.naver.com/api/regions/list?"
        self.headers = {
            "accept": "*/*",
            "accept-language": "ko,en-US;q=0.9,en;q=0.8,ko-KR;q=0.7,ja;q=0.6",
            "accept-encoding": "gzip, deflate, br, zstd",
            "referer": "https://new.land.naver.com/complexes",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Whale/4.31.304.16 Safari/537.36",
        }

    async def _fetch_geo_info(self, session, cortarNo):
        params = {
            "cortarNo": cortarNo,
        }
        await sleep(random.uniform(0.1, 0.6))
        response = await session.get(
            url=self.base_url,
            params=params,
            headers=self.headers,
            impersonate="chrome",
        )
        parsed_response = json.loads(response.text)

        if parsed_response.get("regionList") is None:
            logger.warning(f"법정동코드: {cortarNo}: 없거나 잘못된 요청입니다.")
            return

        logger.info(f"법정동코드: {cortarNo} 진행 중")

        return parsed_response.get("regionList", [])

    async def fetch_geo_info_async_task(self, cortarNo_list: list[str]) -> pl.DataFrame:
        async with AsyncSession() as session:
            tasks = [
                self._fetch_geo_info(session, cortarNo) for cortarNo in cortarNo_list
            ]
            results = await asyncio.gather(*tasks)
            # NoneType 무시하고 flatten
            flat_results = [
                item for sublist in results if sublist is not None for item in sublist
            ]

            for item in flat_results:
                item["naver_land_geo_info_id"] = self._create_unique_key(item)

        return pl.DataFrame(flat_results)

    def run_async_task(self, cortarNo_list):
        return asyncio.run(self.fetch_geo_info_async_task(cortarNo_list))

    def _create_unique_key(self, item_dict):
        for k, v in item_dict.items():
            if v is None:
                item_dict[k] = ""
        obj = NaverLandGeoInfo(**item_dict)
        return create_hash_key(obj)


if __name__ == "__main__":
    storage_options = {
        "s3.region": "ap-northeast-2",
        "s3.access-key-id": os.getenv("AWS_ACCESS_KEY_ID"),
        "s3.secret-access-key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    }

    naver_land_geo_info = NaverLandGeoInfoList()

    sigungu_cd_df = fetch_iceberg_table_to_polars(
        catalog=create_catalog("glue"),
        namespace="mart-real-estate",
        table_name="dim_stan_regin_cd_gu",
        storage_options=storage_options,
    )

    address_df = sigungu_cd_df.select(pl.col("region_cd"), pl.col("locallow_nm"))
    address_rows = list(address_df.collect().iter_rows())

    region_map = {region_cd: locallow_nm for region_cd, locallow_nm in address_rows}
    cortarNo_list = [region_cd for region_cd, _ in address_rows]

    for chunk in chunk_list(lst=cortarNo_list, chunk_size=10):
        results_df = naver_land_geo_info.run_async_task(chunk)

        # 첫/마지막 지역코드에 해당하는 지역명 추출
        first_name = region_map.get(chunk[0], str(chunk[0]))
        last_name = region_map.get(chunk[-1], str(chunk[-1]))
        file_name = f"naver_land_geo_info_{first_name}-{last_name}"

        upload_data_to_obj_storage_polars(
            df=results_df,
            endpoint_type="aws",
            bucket_name="real-estate-raw",
            dir_path="naver_land_geo_info",
            file_name=file_name,
            key=os.getenv("AWS_ACCESS_KEY_ID"),
            secret=os.getenv("AWS_SECRET_ACCESS_KEY"),
            partition_key=f"{first_name}-{last_name}",
        )
        print(f"{file_name} 처리 완료")

    # trigger_aws_glue_crawler(crawler_name="real-estate-raw-crawler")
