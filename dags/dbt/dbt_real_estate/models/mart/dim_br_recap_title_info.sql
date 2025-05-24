with stg_br_recap_title_info as (
    select * from {{ ref('stg_br_recap_title_info') }}
),
final as (
    SELECT
        a.sigunguCd as "시군구코드" -- 시군구코드
        , a.bjdongCd as "법정동코드" -- 법정동코드
        , a.bun as "지번_번" -- 번
        , a.ji as "지번_지" -- 지
        , a.platPlc as "대지위치" -- 대지위치
        , a.platGbCd as "대지구분코드" -- 대지구분코드
        , a.bldNm as "건물명" -- 건물명
        , a.mgmBldrgstPk as "관리건축물대장PK" -- 관리건축물대장PK
        , a.regstrGbCd as "대장구분코드" -- 대장구분코드
        , a.regstrGbCdNm as "대장구분코드명" -- 대장구분코드명
        , a.regstrKindCd as "대장종류코드" -- 대장종류코드
        , a.regstrKindCdNm as "대장종류코드명" -- 대장종류코드명
        , a.newOldRegstrGbCd as "신구대장구분코드" -- 신구대장구분코드
        , a.newOldRegstrGbCdNm as "신구대장구분코드명" -- 신구대장구분코드명
        , a.newPlatPlc as "도로명대지위치" -- 도로명대지위치
        , a.splotNm as "특수지명" -- 특수지명
        , a.block as "블록" -- 블록
        , a.lot as "로트" -- 로트
        , a.bylotCnt as "외필지수" -- 외필지수
        , a.naRoadCd as "새주소도로코드" -- 새주소도로코드
        , a.naBjdongCd as "새주소법정동코드" -- 새주소법정동코드
        , a.naUgrndCd as "새주소지상지하코드" -- 새주소지상지하코드
        , a.naMainBun as "새주소본번" -- 새주소본번
        , a.naSubBun as "새주소부번" -- 새주소부번
        , a.platArea as "대지면적" -- 대지면적(㎡)
        , a.archArea as "건축면적" -- 건축면적(㎡)
        , a.bcRat as "건폐율" -- 건폐율(%)
        , a.totArea as "연면적" -- 연면적(㎡)
        , a.vlRatEstmTotArea as "용적률산정연면적" -- 용적률산정연면적(㎡)
        , a.vlRat as "용적률" -- 용적률(%)
        , a.mainPurpsCd as "주용도코드" -- 주용도코드
        , a.mainPurpsCdNm as "주용도코드명" -- 주용도코드명
        , a.etcPurps as "기타용도" -- 기타용도
        , a.hhldCnt as "세대수" -- 세대수(세대)
        , a.fmlyCnt as "가구수" -- 가구수(가구)
        , a.mainBldCnt as "주건축물수" -- 주건축물수
        , a.atchBldCnt as "부속건축물수" -- 부속건축물수
        , a.atchBldArea as "부속건축물면적" -- 부속건축물면적(㎡)
        , a.totPkngCnt as "총주차수" -- 총주차수
        , a.indrMechUtcnt as "옥내기계식대수" -- 옥내기계식대수(대)
        , a.indrMechArea as "옥내기계식면적" -- 옥내기계식면적(㎡)
        , a.oudrMechUtcnt as "외부기계식대수" -- 외부기계식대수(대)
        , a.oudrMechArea as "외부기계식면적" -- 외부기계식면적(㎡)
        , a.indrAutoUtcnt as "옥내자주식대수" -- 옥내자주식대수(대)
        , a.indrAutoArea as "옥내자주식면적" -- 옥내자주식면적(㎡)
        , a.oudrAutoUtcnt as "외부자주식대수" -- 외부자주식대수(대)
        , a.oudrAutoArea as "외부자주식면적" -- 외부자주식면적(㎡)
        , a.pmsDay as "허가일자" -- 허가일
        , a.pmsDay_parsed as "허가일자_parsed" -- 허가일
        , a.stcnsDay as "착공일자" -- 착공일
        , a.stcnsDay_parsed as "착공일자_parsed" -- 착공일
        , a.useAprDay as "사용승인일자" -- 사용승인일
        , a.useAprDay_parsed as "사용승인일자_parsed" -- 사용승인일
        , a.pmsnoYear as "허가번호년" -- 허가번호년
        , a.pmsnoKikCd as "허가번호기관코드" -- 허가번호기관코드
        , a.pmsnoKikCdNm as "허가번호기관코드명" -- 허가번호기관코드명
        , a.pmsnoGbCd as "허가번호구분코드" -- 허가번호구분코드
        , a.pmsnoGbCdNm as "허가번호구분코드명" -- 허가번호구분코드명
        , a.hoCnt as "호수" -- 호수(호)
        , a.engrGrade as "에너지효율등급" -- 에너지효율등급
        , a.engrRat as "에너지절감율" -- 에너지절감율
        , a.engrEpi as "EPI점수" -- EPI점수
        , a.gnBldGrade as "친환경건축물등급" -- 친환경건축물등급
        , a.gnBldCert as "친환경건축물인증점수" -- 친환경건축물인증점수
        , a.itgBldGrade as "지능형건축물등급" -- 지능형건축물등급
        , a.itgBldCert as "지능형건축물인증점수" -- 지능형건축물인증점수
        , a.crtnDay as "생성일자" -- 생성일자
        , a.crtnDay_parsed as "생성일자_parsed" -- 생성일자
    from stg_br_recap_title_info a
)
select * from final