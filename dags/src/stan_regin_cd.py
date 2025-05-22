import os
import json
import logging
import polars as pl
from rich import print
from dotenv import load_dotenv
from curl_cffi import requests
from dataclasses import dataclass
from utils import (
    create_hash_key,
    save_file_to_local,
    upload_data_to_obj_storage_polars,
    remove_file_from_local,
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

load_dotenv()


@dataclass
class StanReginCd:
    """
    법정동코드 데이터
    """

    region_cd: str  # 지역코드
    sido_cd: str  # 시도코드
    sgg_cd: str  # 시군구코드
    umd_cd: str  # 읍면동코드
    ri_cd: str  # 리코드
    locatjumin_cd: str  # 지역주민코드
    locatjijuk_cd: str  # 지역코드_지적
    locatadd_nm: str  # 지역주소명
    locat_order: int  # 서열
    locat_rm: str  # 비고
    locathigh_cd: str  # 상위지역코드
    locallow_nm: str  # 최하위지역명
    adpt_de: str  # 생성일(YYYYMMDD)
    stan_regin_cd_id: str = ""  # 유니크 해시 키


class StanReginCdList:
    def __init__(self):
        self.service_key = os.getenv("OPENAPI_API_KEY")
        self.base_url = "http://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList?"

        self.session = requests.Session()

    def fetch_stan_regin_cd(
        self, locatadd_nm, pageNo: int, numOfRows: int = 1000
    ) -> tuple[list[dict], int]:
        params = {
            "locatadd_nm": locatadd_nm,  # 지역주소명
            "flag": "Y",  # 신규API 여부
            "type": "json",
            "pageNo": pageNo,
            "numOfRows": numOfRows,
            "serviceKey": self.service_key,
        }

        response = self.session.get(
            url=self.base_url,
            params=params,
            impersonate="chrome",
        )

        parsed_response = json.loads(response.text)

        if parsed_response.get("StanReginCd") == None:
            logger.error(f"Error Code: {parsed_response.get('RESULT')}")
            raise Exception("잘못된 요청입니다.")

        response_body = parsed_response["StanReginCd"]

        # # response_body가 리스트인지 확인 후 head 추출
        if isinstance(response_body, list):
            # 첫 번째 요소에서 head 추출
            head = response_body[0]["head"]
        else:
            head = response_body["head"]

        total_count = head[0]["totalCount"]
        total_pages = (total_count + numOfRows - 1) // numOfRows

        # logger.info(f"totalCount: {total_count}, totalPages: {total_pages}")

        if total_count == "0":
            logger.error(f"Error Code: {parsed_response.get('RESULT')}")
            raise Exception("요청 데이터가 없습니다.")

        return response_body[-1]["row"], total_pages

    def _create_unique_key(self, item_dict):
        # dict를 StanReginCd dataclass로 변환 후 해시 생성
        # None 값은 빈 문자열로 변환
        for k, v in item_dict.items():
            if v is None:
                item_dict[k] = ""
        obj = StanReginCd(**item_dict)
        return create_hash_key(obj)

    def concat_stan_regin_cd_list(self, locatadd_nm: str) -> pl.DataFrame:
        stan_regin_cd_list, total_pages = self.fetch_stan_regin_cd(
            locatadd_nm=locatadd_nm, pageNo=1
        )

        for pageNo in range(2, total_pages + 1):
            extend_stan_regin_cd_list, _ = self.fetch_stan_regin_cd(
                locatadd_nm=locatadd_nm, pageNo=pageNo
            )
            stan_regin_cd_list.extend(extend_stan_regin_cd_list)

        for item in stan_regin_cd_list:
            item["stan_regin_cd_id"] = self._create_unique_key(item)

        logger.info(f"totalCount: {len(stan_regin_cd_list)}, totalPages: {total_pages}")

        return pl.DataFrame(stan_regin_cd_list)


if __name__ == "__main__":
    region_dict = {
        "서울특별시": "seoul",
        "경기도": "gyeonggi",
    }

    stan_regin_cd_list = StanReginCdList()
    for region, region_eng in region_dict.items():
        concat_list = stan_regin_cd_list.concat_stan_regin_cd_list(region)

        file_name = f"stan_regin_cd_{region_eng}"
        # save_file_to_local(data=concat_list, file_name=file_name)

        upload_data_to_obj_storage_polars(
            df=concat_list,
            endpoint_type="aws",
            bucket_name="real-estate-raw",
            dir_path="stan_regin_cd",
            file_name=file_name,
            key=os.getenv("AWS_ACCESS_KEY_ID"),
            secret=os.getenv("AWS_SECRET_ACCESS_KEY"),
            partition_key=region_eng,
        )
    # remove_file_from_local()
