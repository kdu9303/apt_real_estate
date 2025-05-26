with stg_br_recap_title_info as (
    select * from {{ ref('stg_br_recap_title_info') }}
),
final as (
    SELECT
        a.sigunguCd -- 시군구코드
        , a.bjdongCd -- 법정동코드
        , a.bun -- 번
        , a.ji -- 지
        , a.platPlc -- 대지위치
        , a.platGbCd -- 대지구분코드
        , a.bldNm -- 건물명
        , a.mgmBldrgstPk -- 관리건축물대장PK
        , a.regstrGbCd -- 대장구분코드
        , a.regstrGbCdNm -- 대장구분코드명
        , a.regstrKindCd -- 대장종류코드
        , a.regstrKindCdNm -- 대장종류코드명
        , a.newOldRegstrGbCd -- 신구대장구분코드
        , a.newOldRegstrGbCdNm -- 신구대장구분코드명
        , a.newPlatPlc -- 도로명대지위치
        , a.splotNm -- 특수지명
        , a.block -- 블록
        , a.lot -- 로트
        , a.bylotCnt -- 외필지수
        , a.naRoadCd -- 새주소도로코드
        , a.naBjdongCd -- 새주소법정동코드
        , a.naUgrndCd -- 새주소지상지하코드
        , a.naMainBun -- 새주소본번
        , a.naSubBun -- 새주소부번
        , a.platArea -- 대지면적(㎡)
        , a.archArea -- 건축면적(㎡)
        , a.bcRat -- 건폐율(%)
        , a.totArea -- 연면적(㎡)
        , a.vlRatEstmTotArea -- 용적률산정연면적(㎡)
        , a.vlRat -- 용적률(%)
        , a.mainPurpsCd -- 주용도코드
        , a.mainPurpsCdNm -- 주용도코드명
        , a.etcPurps -- 기타용도
        , a.hhldCnt -- 세대수(세대)
        , a.fmlyCnt -- 가구수(가구)
        , a.mainBldCnt -- 주건축물수
        , a.atchBldCnt -- 부속건축물수
        , a.atchBldArea -- 부속건축물면적(㎡)
        , a.totPkngCnt -- 총주차수
        , a.indrMechUtcnt -- 옥내기계식대수(대)
        , a.indrMechArea -- 옥내기계식면적(㎡)
        , a.oudrMechUtcnt -- 외부기계식대수(대)
        , a.oudrMechArea -- 외부기계식면적(㎡)
        , a.indrAutoUtcnt -- 옥내자주식대수(대)
        , a.indrAutoArea -- 옥내자주식면적(㎡)
        , a.oudrAutoUtcnt -- 외부자주식대수(대)
        , a.oudrAutoArea -- 외부자주식면적(㎡)
        , a.pmsDay -- 허가일자
        , a.pmsDay_parsed -- 허가일자 date형식
        , a.stcnsDay -- 착공일
        , a.stcnsDay_parsed -- 착공일 date형식
        , a.useAprDay -- 사용승인일
        , a.useAprDay_parsed -- 사용승인일 date형식
        , a.pmsnoYear -- 허가번호년
        , a.pmsnoKikCd -- 허가번호기관코드
        , a.pmsnoKikCdNm -- 허가번호기관코드명
        , a.pmsnoGbCd -- 허가번호구분코드
        , a.pmsnoGbCdNm -- 허가번호구분코드명
        , a.hoCnt -- 호수(호)
        , a.engrGrade -- 에너지효율등급
        , a.engrRat -- 에너지절감율
        , a.engrEpi -- EPI점수
        , a.gnBldGrade -- 친환경건축물등급
        , a.gnBldCert -- 친환경건축물인증점수
        , a.itgBldGrade -- 지능형건축물등급
        , a.itgBldCert -- 지능형건축물인증점수
        , a.crtnDay -- 생성일자
        , a.crtnDay_parsed -- 생성일자 date형식
    from stg_br_recap_title_info a
)
select * from final