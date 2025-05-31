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
from curl_cffi import requests
from dataclasses import dataclass
from curl_cffi.requests import AsyncSession
from utils import (
    create_hash_key,
    fetch_iceberg_table_to_polars,
    create_catalog,
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
class NaverLandComplexInfo:
    """
    네이버부동산 단지 정보
    """

    markerId: str  # 단지코드
    markerType: str  # 단지유형
    latitude: float  # 위도
    longitude: float  # 경도
    complexName: str  # 단지명
    cortarno: str  # 법정동코드
    realEstateTypeCode: str  # 부동산유형코드
    realEstateTypeName: str  # 부동산유형명
    completionYearMonth: str  # 준공년월
    totalDongCount: int  # 동수
    totalHouseholdCount: int  # 세대수
    floorAreaRatio: int  # 건폐율
    minDealUnitPrice: int  # 최저 매매단가
    maxDealUnitPrice: int  # 최고 매매단가
    minLeaseUnitPrice: int  # 최저 임대단가
    maxLeaseUnitPrice: int  # 최고 임대단가
    minLeaseRate: int  # 최저 임대율
    maxLeaseRate: int  # 최고 임대율
    minArea: float  # 최소 면적
    maxArea: float  # 최대 면적
    minDealPrice: int  # 최소 매매가격
    maxDealPrice: int  # 최대 매매가격
    minLeasePrice: int  # 최소 임대가격
    maxLeasePrice: int  # 최대 임대가격
    minRentPrice: int  # 최소 임대가격
    maxRentPrice: int  # 최대 임대가격
    minShortTermRentPrice: int  # 최소 단기임대가격
    maxShortTermRentPrice: int  # 최대 단기임대가격
    isDealShown: bool  # 매매 표시 여부
    isLeaseShown: bool  # 임대 표시 여부
    isRentShown: bool  # 임대 표시 여부
    isShortTermRentShown: bool  # 단기임대 표시 여부
    priceCount: int  # 가격 개수
    representativeArea: float  # 대표 면적
    medianDealUnitPrice: int  # 중앙 매매단가
    medianDealPrice: int  # 중앙 매매가격
    medianRentPrice: int  # 중앙 임대가격
    medianShortTermRentPrice: int  # 중앙 단기임대가격
    isPresales: bool  # 선분양 여부
    representativePhoto: str  # 대표 사진
    photoCount: int  # 사진 개수
    dealCount: int  # 매매 개수
    leaseCount: int  # 임대 개수
    rentCount: int  # 임대 개수
    shortTermRentCount: int  # 단기임대 개수
    totalArticleCount: int  # 총 게시글 개수
    existPriceTab: bool  # 가격 탭 존재 여부
    isComplexTourExist: bool  # 단지 투어 존재 여부
    createdAt: str = ""  # 생성일자
    naver_land_complex_info_id: str = ""  # 유니크 해시 키(초기값 빈 문자열)


class NaverLandComplexInfoList:
    def __init__(self):
        self.base_url = "https://new.land.naver.com/api/complexes/single-markers/2.0?tradeType=&oldBuildYears&recentlyBuildYears&minHouseHoldCount&maxHouseHoldCount"

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

    @staticmethod
    def _get_lat_lon_bounds(centerLat: float, centerLon: float):
        """
        주어진 위도, 경도를 기준으로 4면의 경도, 위도 범위를 반환
        """
        lon_delta = 0.0360489
        lat_delta = 0.01315735
        return {
            "leftLon": centerLon - lon_delta,
            "rightLon": centerLon + lon_delta,
            "topLat": centerLat + lat_delta,
            "bottomLat": centerLat - lat_delta,
        }

    async def _fetch_complex_info(
        self, session, cortarNo, centerLat, centerLon
    ) -> list[dict]:
        """
        네이버부동산 단지 정보 조회
        법정동 코드와 위도경도 정보를 기준으로 지도 면적에 포함되는 단지 정보를 조회

        Args:
            cortarNo: 법정동코드 10자리
            centerLat: 위도
            centerLon: 경도

        """

        # 위도 경도 범위 추출
        lat_lon_bounds = self._get_lat_lon_bounds(centerLat, centerLon)

        params = {
            "cortarNo": cortarNo,  # 법정동코드 10자리
            "markerType": "COMPLEX",
            "priceType": "RETAIL",
            "leftLon": lat_lon_bounds["leftLon"],
            "rightLon": lat_lon_bounds["rightLon"],
            "topLat": lat_lon_bounds["topLat"],
            "bottomLat": lat_lon_bounds["bottomLat"],
            "realEstateType": "APT:PRE:ABYG:JGC",
        }

        await sleep(random.uniform(0.1, 0.6))

        response = await session.get(
            url=self.base_url,
            params=params,
            impersonate="chrome",
            headers=self.headers,
        )
        parsed_response: list[dict] = json.loads(response.text)

        if not parsed_response:
            logger.warning(f"법정동코드: {cortarNo} 결과 없음")
            return

        logger.info(f"법정동코드: {cortarNo} 진행 중")

        # 법정동코드 일괄 추가
        parsed_response = [{**item, "cortarno": cortarNo} for item in parsed_response]

        return parsed_response

    async def fetch_complex_info_async_task(self, region_list: list[dict]):
        async with AsyncSession() as session:
            tasks = [
                self._fetch_complex_info(
                    session=session,
                    cortarNo=q["cortarno"],
                    centerLat=q["centerlat"],
                    centerLon=q["centerlon"],
                )
                for q in region_list
            ]
            results: list[list[list[dict]]] = await asyncio.gather(*tasks)
            flat_results = [
                item for sublist in results if sublist is not None for item in sublist
            ]

            # 메타 정보 칼럼 추가
            for item in flat_results:
                item["naver_land_complex_info_id"] = self._create_unique_key(item)
                item["createdAt"] = datetime.now().strftime("%Y-%m-%d")

        return pl.DataFrame(flat_results)

    def run_async_task(self, region_list):
        return asyncio.run(self.fetch_complex_info_async_task(region_list))

    def _create_unique_key(self, item_dict):
        # 필요한 필드만 추출
        key_fields = [
            str(item_dict.get("markerId", "")),
            str(item_dict.get("markerType", "")),
            str(item_dict.get("latitude", "")),
            str(item_dict.get("longitude", "")),
            str(item_dict.get("complexName", "")),
        ]
        return create_hash_key(key_fields, is_dataclass=False)


if __name__ == "__main__":
    storage_options = {
        "s3.region": "ap-northeast-2",
        "s3.access-key-id": os.getenv("AWS_ACCESS_KEY_ID"),
        "s3.secret-access-key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    }

    sggCd_dict = {
        "서초구": "1165000000",
        "송파구": "1171000000",
        "강남구": "1168000000",
        "강동구": "1174000000",
        "용산구": "1117000000",
        "광진구": "1121500000",
        "성동구": "1120000000",
    }

    sggCd_list = list(sggCd_dict.values())

    land_geo_df = fetch_iceberg_table_to_polars(
        catalog=create_catalog("glue"),
        namespace="mart-real-estate",
        table_name="dim_naver_land_geo_info",
        storage_options=storage_options,
    )

    # print(land_geo_df.collect_schema().names())

    naver_land_complex_list = NaverLandComplexInfoList()

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

        land_complex_df = naver_land_complex_list.run_async_task(region_list)

        file_name = f"naver_land_complex_info_{sggCd}"

        upload_data_to_obj_storage_polars(
            df=land_complex_df,
            endpoint_type="aws",
            bucket_name="real-estate-raw",
            dir_path="naver_land_complex_info",
            file_name=file_name,
            key=os.getenv("AWS_ACCESS_KEY_ID"),
            secret=os.getenv("AWS_SECRET_ACCESS_KEY"),
            partition_key=sggCd,
        )

        print(f"{file_name} 처리 완료")

    # trigger_aws_glue_crawler(crawler_name="real-estate-raw-crawler")
