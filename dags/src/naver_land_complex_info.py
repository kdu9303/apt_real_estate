import os
import json
import asyncio
import random
import logging
import polars as pl
from rich import print
from dotenv import load_dotenv
from asyncio import sleep
from datetime import datetime
from dataclasses import dataclass
from curl_cffi.requests import AsyncSession
from curl_cffi import requests, CurlHttpVersion
from curl_cffi.requests.exceptions import Timeout
from utils import (
    create_hash_key,
    fetch_iceberg_table_to_polars,
    create_catalog,
    upload_data_to_obj_storage_polars,
    trigger_aws_glue_crawler,
)
from utils.const import SGG_CD_DICT


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


load_dotenv()


@dataclass
class NaverLandComplexInfo:
    """
    네이버부동산 단지 정보
    """

    complexNo: str  # 단지코드
    complexName: str  # 단지명
    cortarNo: str  # 법정동코드
    realEstateTypeCode: str  # 부동산유형코드
    realEstateTypeName: str  # 부동산유형명
    detailAddress: str  # 상세주소
    latitude: float  # 위도
    longitude: float  # 경도
    totalHouseholdCount: int  # 세대수
    totalBuildingCount: int  # 건물수
    highFloor: int  # 최고층
    lowFloor: int  # 최저층
    useApproveYmd: str  # 사용승인일자
    dealCount: int  # 매매 개수
    leaseCount: int  # 임대 개수
    rentCount: int  # 임대 개수
    shortTermRentCount: int  # 단기임대 개수
    cortarAddress: str  # 법정동 주소
    tourExist: bool  # 단지 투어 존재 여부
    createdAt: str = ""  # 생성일자
    naver_land_complex_info_id: str = ""  # 유니크 해시 키(초기값 빈 문자열)



class NaverLandComplexInfoList:
    def __init__(self):
        self.base_url = "https://new.land.naver.com/api/regions/complexes"
        self.session = requests.Session()

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


    async def _fetch_complex_info(
        self, session, cortarNo
    ) -> list[dict]:
        """
        네이버부동산 단지 정보 조회
        법정동 코드를 기준으로 단지 정보를 조회
        """

        params = {
            "cortarNo": cortarNo,  # 법정동코드 10자리
            "realEstateType": "APT:PRE:ABYG:JGC",
        }

        await sleep(random.uniform(0.3, 0.7))

        try:
            response = await session.get(
                url=self.base_url,
                params=params,
                impersonate="chrome",
                headers=self.headers,
                http_version=CurlHttpVersion.V1_1
            )
            parsed_response: list[dict] = json.loads(response.text)

            if not parsed_response:
                logger.warning(f"법정동코드: {cortarNo} 결과 없음")
                return

            logger.info(f"법정동코드: {cortarNo} 진행 중")

            return parsed_response["complexList"]
        except Timeout as e:
            logger.error(f"법정동코드: {cortarNo} - [IP 블로킹 가능성] Timeout 발생: {e}")
            return
        except Exception as e:
            logger.error(f"법정동코드: {cortarNo} - 기타 오류 발생: {e}")
            return

    async def fetch_complex_info_async_task(self, region_list: list):
        async with AsyncSession() as session:
            tasks = [
                self._fetch_complex_info(
                    session=session,
                    cortarNo=region,
                )
                for region in region_list
            ]
            
            results: list[list[dict]] = await asyncio.gather(*tasks)
            
            flat_results: list[dict] = [
                item for sublist in results if sublist is not None for item in sublist
            ]

            # 메타 정보 칼럼 추가
            for item in flat_results:
                item["naver_land_complex_info_id"] = self._create_unique_key(item)
                item["createdAt"] = datetime.now().strftime("%Y%m%d")

        return pl.DataFrame(flat_results)

    def run_async_task(self, region_list: list[dict]) -> pl.DataFrame:
        return asyncio.run(self.fetch_complex_info_async_task(region_list))

    def _create_unique_key(self, item_dict: dict) -> str:
        # 필요한 필드만 추출
        key_fields = [
            str(item_dict.get("complexNo", "")),
            str(item_dict.get("complexName", "")),
            str(item_dict.get("cortarNo", "")),
            str(item_dict.get("latitude", "")),
            str(item_dict.get("longitude", "")),
        ]
        return create_hash_key(key_fields, is_dataclass=False)


if __name__ == "__main__":
    storage_options = {
        "s3.region": "ap-northeast-2",
        "s3.access-key-id": os.getenv("AWS_ACCESS_KEY_ID"),
        "s3.secret-access-key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    }

    sggCd_list = list(SGG_CD_DICT.values())

    dim_stan_regin_cd_df = fetch_iceberg_table_to_polars(
        catalog=create_catalog("glue"),
        namespace="mart-real-estate",
        table_name="dim_stan_regin_cd",
        storage_options=storage_options,
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
        
        region_list = address_df.select(pl.col("region_cd")).to_series().to_list()
    
        results_df = naver_land_complex_list.run_async_task(region_list)
    
        file_name = f"naver_land_complex_info_{sggCd}"

        upload_data_to_obj_storage_polars(
            df=results_df,
            endpoint_type="aws",
            bucket_name="real-estate-raw",
            dir_path="naver_land_complex_info",
            file_name=file_name,
            key=os.getenv("AWS_ACCESS_KEY_ID"),
            secret=os.getenv("AWS_SECRET_ACCESS_KEY"),
            partition_key=sggCd,
        )

        print(f"{file_name} 처리 완료")

    trigger_aws_glue_crawler(crawler_name="real-estate-raw-crawler")
