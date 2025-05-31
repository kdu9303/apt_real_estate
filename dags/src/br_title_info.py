import os
import random
import logging
import xmltodict
import polars as pl
from time import sleep
from rich import print
from dotenv import load_dotenv
from curl_cffi import requests
from dataclasses import dataclass
from utils import (
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
class BrTitleInfo:
    """
    건축물대장 표제부 조회 데이터
    """

    rnum: str  # 순번
    platPlc: str  # 대지위치
    sigunguCd: str  # 시군구코드
    bjdongCd: str  # 법정동코드
    platGbCd: str  # 대지구분코드 0:대지 1:산 2:블록
    bun: str  # 번
    ji: str  # 지
    mgmBldrgstPk: str  # 관리건축물대장PK
    regstrGbCd: str  # 대장구분코드
    regstrGbCdNm: str  # 대장구분코드명
    regstrKindCd: str  # 대장종류코드
    regstrKindCdNm: str  # 대장종류코드명
    newPlatPlc: str  # 도로명대지위치
    bldNm: str  # 건물명
    splotNm: str  # 특수지명
    block: str  # 블록
    lot: str  # 로트
    bylotCnt: str  # 외필지수
    naRoadCd: str  # 새주소도로코드
    naBjdongCd: str  # 새주소법정동코드
    naUgrndCd: str  # 새주소지상지하코드
    naMainBun: str  # 새주소본번
    naSubBun: str  # 새주소부번
    dongNm: str  # 동명칭
    mainAtchGbCd: str  # 주부속구분코드
    mainAtchGbCdNm: str  # 주부속구분코드명
    platArea: str  # 대지면적(㎡)
    archArea: str  # 건축면적(㎡)
    bcRat: str  # 건폐율(%)
    totArea: str  # 연면적(㎡)
    vlRatEstmTotArea: str  # 용적률산정연면적(㎡)
    vlRat: str  # 용적률(%)
    strctCd: str  # 구조코드
    strctCdNm: str  # 구조코드명
    etcStrct: str  # 기타구조
    mainPurpsCd: str  # 주용도코드
    mainPurpsCdNm: str  # 주용도코드명
    etcPurps: str  # 기타용도
    roofCd: str  # 지붕코드
    roofCdNm: str  # 지붕코드명
    etcRoof: str  # 기타지붕
    hhldCnt: str  # 세대수(세대)
    fmlyCnt: str  # 가구수(가구)
    heit: str  # 높이(m)
    grndFlrCnt: str  # 지상층수
    ugrndFlrCnt: str  # 지하층수
    rideUseElvtCnt: str  # 승용승강기수
    emgenUseElvtCnt: str  # 비상용승강기수
    atchBldCnt: str  # 부속건축물수
    atchBldArea: str  # 부속건축물면적(㎡)
    totDongTotArea: str  # 총동연면적(㎡)

    indrMechUtcnt: str  # 옥내기계식대수(대)
    indrMechArea: str  # 옥내기계식면적(㎡)
    oudrMechUtcnt: str  # 외부기계식대수(대)
    oudrMechArea: str  # 외부기계식면적(㎡)
    indrAutoUtcnt: str  # 옥내자주식대수(대)
    indrAutoArea: str  # 옥내자주식면적(㎡)
    oudrAutoUtcnt: str  # 외부자주식대수(대)
    oudrAutoArea: str  # 외부자주식면적(㎡)
    pmsDay: str  # 허가일
    stcnsDay: str  # 착공일
    useAprDay: str  # 사용승인일
    pmsnoYear: str  # 허가번호년
    pmsnoKikCd: str  # 허가번호기관코드
    pmsnoKikCdNm: str  # 허가번호기관코드명
    pmsnoGbCd: str  # 허가번호구분코드
    pmsnoGbCdNm: str  # 허가번호구분코드명
    hoCnt: str  # 호수(호)
    engrGrade: str  # 에너지효율등급
    engrRat: str  # 에너지절감율
    engrEpi: str  # EPI점수
    gnBldGrade: str  # 친환경건축물등급
    gnBldCert: str  # 친환경건축물인증점수
    itgBldGrade: str  # 지능형건축물등급
    itgBldCert: str  # 지능형건축물인증점수
    crtnDay: str  # 생성일자
    rserthqkDsgnApplyYn: str  # 내진설계적용여부
    rserthqkAblty: str  # 내진능력
    br_title_info_id: str = ""  # 유니크 해시 키


class BrTitleInfoList:
    def __init__(self):
        self.service_key = os.getenv("OPENAPI_API_KEY")
        self.base_url = (
            "http://apis.data.go.kr/1613000/BldRgstHubService/getBrTitleInfo?"
        )

        self.session = requests.Session()

    def fetch_br_title_info(
        self, sigunguCd: str, bjdongCd: str, pageNo: int = 1, numOfRows: int = 100
    ) -> tuple[list[dict], int, int]:
        """
        건축물대장 표제부 조회 데이터 조회
        """
        params = {
            "sigunguCd": sigunguCd,
            "bjdongCd": bjdongCd,
            "pageNo": pageNo,
            "numOfRows": numOfRows,
            "serviceKey": self.service_key,
        }

        response = self.session.get(
            url=self.base_url,
            params=params,
            impersonate="chrome",
        )

        parsed_response = xmltodict.parse(response.text)
        response_header = parsed_response["response"]["header"]
        response_body = parsed_response["response"]["body"]

        total_count = int(response_body["totalCount"])
        total_pages = (total_count + numOfRows - 1) // numOfRows

        # 데이터가 없으면 None, 0, 빈 리스트 등으로 반환
        if (
            total_count == 0
            or response_body.get("items") is None
            or response_body["items"] is None
        ):
            return None, 0, 0

        if response_header["resultCode"] != "00":
            logger.error(f"Error Message: {response_header}")
            raise Exception("요청 데이터가 없거나 잘못된 요청입니다.")

        # items가 dict가 아닐 수도 있으니, 안전하게 처리
        items = response_body["items"].get("item")
        if items is None:
            return None, total_pages, total_count

        # 단일 dict일 수도 있고, list일 수도 있음
        if isinstance(items, dict):
            items = [items]

        return items, total_pages, total_count

    def _create_unique_key(self, item_dict):
        # BrTitleInfo의 모든 필드명 가져오기
        all_fields = BrTitleInfo.__dataclass_fields__.keys()
        # 누락된 필드는 빈 문자열로 채우기
        for field in all_fields:
            if field not in item_dict:
                item_dict[field] = ""
        # None 값도 빈 문자열로 변환
        for k, v in item_dict.items():
            if v is None:
                item_dict[k] = ""
        obj = BrTitleInfo(**item_dict)
        return create_hash_key(obj)

    def concat_br_title_info_data_list(self, address_df: pl.DataFrame) -> pl.DataFrame:
        """시군구코드 기준 모든 법정동 데이터를 합치는 함수"""
        br_title_info_data_list = []

        for row in address_df.iter_rows(named=True):
            bjdong_list, total_pages, total_count = self.fetch_br_title_info(
                sigunguCd=row["gu_cd"], bjdongCd=row["bjdong_cd"]
            )

            if bjdong_list is None:
                continue

            for page in range(1, total_pages + 1):
                bjdong_list, _, _ = self.fetch_br_title_info(
                    sigunguCd=row["gu_cd"],
                    bjdongCd=row["bjdong_cd"],
                    pageNo=page,
                    numOfRows=100,
                )

                if bjdong_list is None:
                    continue

                for item in bjdong_list:
                    item["br_title_info_id"] = self._create_unique_key(item)
                    br_title_info_data_list.append(item)

                logger.info(
                    f"시군구코드: {row['gu_cd']}, 법정동코드: {row['bjdong_cd']}, TotalPage: {total_pages},  currentPage: {page}, totalCount: {total_count}"
                )

                sleep(random.uniform(0.6, 0.9))

        return pl.DataFrame(br_title_info_data_list)


if __name__ == "__main__":
    storage_options = {
        "s3.region": "ap-northeast-2",
        "s3.access-key-id": os.getenv("AWS_ACCESS_KEY_ID"),
        "s3.secret-access-key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    }

    sigungu_cd_df = fetch_iceberg_table_to_polars(
        catalog=create_catalog("glue"),
        namespace="mart-real-estate",
        table_name="dim_stan_regin_cd",
        storage_options=storage_options,
    )

    sigungu_cd_df_filtered = sigungu_cd_df.select(
        pl.col("gu_cd"),
        pl.col("bjdong_cd"),
    ).collect()

    sggCd_dict = {
        "서초구": "11650",
        "송파구": "11710",
        "강남구": "11680",
        # "강동구": "11740",
        # "용산구": "11170",
        # "광진구": "11215",
        # "성동구": "11200",
    }

    br_title_info_list = BrTitleInfoList()

    for sgg_name, sggCd in sggCd_dict.items():
        print(f"시군구코드: {sgg_name} 진행중")
        address_df = sigungu_cd_df_filtered.filter(pl.col("gu_cd") == sggCd)
        print(address_df.shape)

        br_title_info_df = br_title_info_list.concat_br_title_info_data_list(
            address_df = address_df
        )

        file_name = f"br_title_info_{sgg_name}"

        upload_data_to_obj_storage_polars(
            df=br_title_info_df,
            endpoint_type="aws",
            bucket_name="real-estate-raw",
            dir_path="br_title_info",
            file_name=file_name,
            key=os.getenv("AWS_ACCESS_KEY_ID"),
            secret=os.getenv("AWS_SECRET_ACCESS_KEY"),
            partition_key=sggCd,
        )

    # trigger_aws_glue_crawler(crawler_name="real-estate-raw-crawler")
