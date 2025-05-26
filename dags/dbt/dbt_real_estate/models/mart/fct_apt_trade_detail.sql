with fct_apt_trade as (
    select * from {{ ref('fct_apt_trade') }}
),
dim_br_recap_title_info as (
    select * from {{ ref('dim_br_recap_title_info') }}
),
dim_stan_regin_cd_gu as (
    select * from {{ ref('dim_stan_regin_cd_gu') }}
),
final as (
    SELECT distinct
        a.sggCd -- 지역코드
        , a.bjdong_cd -- 법정동코드
        , c.locallow_nm as gu_nm
        , a.umdNm -- 법정동명
        , b.bldNm -- 건물명(총괄표제부 테이블 기준)
        , a.aptNm -- 아파트 이름(실매매 테이블 기준)
        , a.aptDong -- 아파트 동명
        , a.buildYear -- 건축년도
        , a.buyerGbn -- 매수자
        , a.cdealDay -- 해제사유발생일자
        , a.cdealType -- 해제여부
        , a.dealAmount -- 거래 금액
        , a.dealDate -- 거래일자
        , a.dealDay -- 계약 일자
        , a.dealMonth -- 계약 월
        , a.dealYear -- 계약 년도
        , a.dealingGbn -- 거래 유형
        , a.estateAgentSggNm -- 중개사소재지
        , a.excluUseAr -- 전용면적
        , a.floor -- 층수
        , a.bun -- 지번_번
        , a.ji -- 지번_지
        , a.landLeaseholdGbn -- 토지임대부아파트여부
        , a.rgstDate -- 등기일자
        , a.slerGbn -- 매도자
        , b.platArea -- 대지면적
        , b.archArea -- 건축면적
        , b.bcRat -- 건폐율
        , b.vlRatEstmTotArea -- 용적률산정연면적
        , b.vlRat -- 용적률
        , b.hhldCnt -- 세대수
		, a.apt_trade_id -- 유니크 해시 키
    FROM fct_apt_trade a
    left JOIN dim_br_recap_title_info b
        ON a.sggCd = b.sigunguCd
        AND a.bjdong_cd = b.bjdongCd
        AND a.bun = b.bun
        AND a.ji = b.ji
        AND b.etcpurps = '공동주택(아파트)'
    left JOIN dim_stan_regin_cd_gu c
        ON a.sggCd = c.gu_cd
)
select * from final

