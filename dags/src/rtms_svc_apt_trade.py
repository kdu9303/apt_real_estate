import os
import xmltodict
import polars as pl
from time import sleep
from rich import print
from dataclasses import dataclass
from dotenv import load_dotenv
from curl_cffi import requests
import logging
from utils import (
    create_yyyymm_form_date_list,
    create_hash_key,
    save_file_to_local,
    remove_file_from_local,
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
class RTMSDataSvcAptTrade:
    """
    아파트 매매 실거래가 데이터
    """

    aptDong: str  # 아파트 동명
    aptNm: str  # 아파트 이름
    buildYear: str  # 건축년도
    buyerGbn: str  # 매수자
    cdealDay: str  # 해제사유발생일
    cdealType: str  # 해제여부
    dealAmount: str  # 거래래 금액
    dealDay: str  # 계약 일자
    dealMonth: str  # 계약 월
    dealYear: str  # 계약 년도도
    dealingGbn: str  # 거래 유형
    estateAgentSggNm: str  # 중개사소재지
    excluUseAr: str  # 전용면적
    floor: str  # 층수
    jibun: str  # 지번
    landLeaseholdGbn: str  # 토지임대부 아파트 여부
    rgstDate: str  # 등기일자
    sggCd: str  # 지역코드
    slerGbn: str  # 매도자
    umdNm: str  # 법정동명
    apt_trade_id: str = ""  # 유니크 해시 키(초기값 빈 문자열)


class AptTradeList:
    def __init__(self):
        self.service_key = os.getenv("OPENAPI_API_KEY")
        self.base_url = (
            "http://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade?"
        )

        self.session = requests.Session()
        """
        self.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "ko,en;q=0.9,ko-KR;q=0.8,en-US;q=0.7",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
        }
        """

    def fetch_apt_trade(
        self, sggCd, deal_month_day, numOfRows: int = 1_000_000
    ) -> dict:
        params = {
            "LAWD_CD": sggCd,  # 지역코드
            "DEAL_YMD": deal_month_day,  # 계약월
            "numOfRows": numOfRows,
            "serviceKey": self.service_key,
        }
        response = self.session.get(
            url=self.base_url,
            params=params,
            impersonate="chrome",
        )
        parsed_response = xmltodict.parse(response.text)
        response_body = parsed_response["response"]["body"]

        logger.info(
            f"Year Month: {deal_month_day}, totalCount: {response_body['totalCount']}"
        )

        if response_body["totalCount"] == "0":
            logger.info("해당 요청에 대한 데이터가 없습니다.")
            return

        if parsed_response["response"]["header"]["resultCode"] != "000":
            logger.error(f"Error Message: {parsed_response['response']['header']}")
            raise Exception("해당 요청은 잘못된 요청입니다.")

        return response_body["items"]["item"]

    def _create_unique_key(self, item_dict):
        # dict를 RTMSDataSvcAptTrade dataclass로 변환 후 해시 생성
        # None 값은 빈 문자열로 변환
        for k, v in item_dict.items():
            if v is None:
                item_dict[k] = ""
        obj = RTMSDataSvcAptTrade(**item_dict)
        return create_hash_key(obj)

    def concat_apt_trade_data_list(self, year: int, sggCd: str) -> pl.DataFrame:
        """1년치의 데이터를 모두 합치는 함수"""
        apt_data_list = []
        year_month_list = create_yyyymm_form_date_list(year)

        for year_month in year_month_list:
            api_data = self.fetch_apt_trade(sggCd, year_month)
            if api_data is None:
                continue

            sleep(0.8)
            # api_data가 리스트 또는 단일 dict일 수 있음
            if isinstance(api_data, list):
                for item in api_data:
                    item["apt_trade_id"] = self._create_unique_key(item)
                    apt_data_list.append(item)
            else:
                api_data["apt_trade_id"] = self._create_unique_key(api_data)
                apt_data_list.append(api_data)

        # return apt_data_list
        return pl.DataFrame(apt_data_list)


if __name__ == "__main__":
    apt_list = AptTradeList()

    sggCd_dict = {
        "서초구": "11650",
        "송파구": "11710",
        "강남구": "11680",
        "강동구": "11740",
        "용산구": "11170",
        "광진구": "11215",
        "성동구": "11200",
    }

    years = [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
    # years = [2025]

    for year in years:
        for sgg_name, sggCd in sggCd_dict.items():
            apt_data_list = apt_list.concat_apt_trade_data_list(year, sggCd)
            print(apt_data_list.shape)

            file_name = f"apt_trade_{sgg_name}_{year}"
            # save_file_to_local(data=apt_data_list, file_name=file_name)

            # upload_data_to_obj_storage_polars(
            #     df=apt_data_list,
            #     bucket_name="bronze",
            #     dir_path="apt_trade",
            #     file_name=file_name,
            #     key=os.getenv("MINIO_ACCESS_KEY"),
            #     secret=os.getenv("MINIO_SECRET_KEY"),
            #     partition_key=year,
            # )
            upload_data_to_obj_storage_polars(
                df=apt_data_list,
                endpoint_type="aws",
                bucket_name="real-estate-raw",
                dir_path="apt_trade",
                file_name=file_name,
                key=os.getenv("AWS_ACCESS_KEY_ID"),
                secret=os.getenv("AWS_SECRET_ACCESS_KEY"),
                partition_key=year,
            )
            
    trigger_aws_glue_crawler(crawler_name="real-estate-raw-crawler")

    # remove_file_from_local()
