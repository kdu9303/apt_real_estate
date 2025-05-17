import os
import json
import logging
import xmltodict
from rich import print
from dotenv import load_dotenv
from curl_cffi import requests
from dataclasses import dataclass
from utils import (
    create_hash_key,
    save_file_to_local,
    upload_data_to_obj_storage,
    remove_file_from_local,
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
    platGbCd: str  # 대지구분코드
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

    def fetch_br_title_info(
        self, sigunguCd: str, bjdongCd: str, pageNo: int = 1, numOfRows: int = 1_000_000
    ) -> tuple[list[dict], int]:
        """
        건축물대장 표제부 조회 데이터 조회
        """
        params = {
            "sigunguCd": sigunguCd,  # 시군구코드
            "bjdongCd": bjdongCd,  # 법정동코드
            "pageNo": pageNo,
            "numOfRows": numOfRows,
            "serviceKey": self.service_key,
        }
        s = requests.Session()

        response = s.get(
            url=self.base_url,
            params=params,
            impersonate="chrome",
        )

        parsed_response = xmltodict.parse(response.text)
        response_header = parsed_response["response"]["header"]
        response_body = parsed_response["response"]["body"]

        logger.info(
            f"시군구코드: {sigunguCd}, 법정동코드: {bjdongCd}, 총 건수: {response_body['totalCount']}"
        )

        if response_body["totalCount"] == "0" or response_header["resultCode"] != "00":
            logger.error(f"Error Message: {response_header}")
            raise Exception("요청 데이터가 없거나 잘못된 요청입니다.")

        return response_body["items"]["item"]

    def _create_unique_key(self, item_dict):
        # dict를 BrTitleInfo dataclass로 변환 후 해시 생성
        # None 값은 빈 문자열로 변환
        for k, v in item_dict.items():
            if v is None:
                item_dict[k] = ""
        obj = BrTitleInfo(**item_dict)
        return create_hash_key(obj)

    def concat_br_title_info_data_list(
        self, sigunguCd: str, bjdongCd_list: list[str]
    ) -> list[BrTitleInfo]:
        """시군구코드 기준 모든 법정동 데이터를 합치는 함수"""
        br_title_info_data_list = []

        for bjdongCd in bjdongCd_list:
            bjdong_list = self.fetch_br_title_info(sigunguCd, bjdongCd)
            for item in bjdong_list:
                item["br_title_info_id"] = self._create_unique_key(item)
                br_title_info_data_list.append(item)

        return br_title_info_data_list


if __name__ == "__main__":
    br_title_info_list = BrTitleInfoList()

    print(
        br_title_info_list.fetch_br_title_info(
            sigunguCd="11680", bjdongCd="10300", pageNo=2, numOfRows=100_000_000
        )
    )
