{{ 
  config(
    tags = ["real-estate", "fct_apt_trade"]
  ) 
}}
with stg_apt_trade as (
    select * from {{ ref('stg_apt_trade') }}
),
stg_stan_regin_cd as (
    select * from {{ ref('stg_stan_regin_cd') }}
),
final as (
	SELECT 
		a.sggCd -- 지역코드
		, b.bjdong_cd -- 법정동코드
	    , a.umdNm -- 법정동명
	    , a.aptNm -- 아파트 이름
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
	    , a.jibun -- 지번
		, a.bun -- 번
		, a.ji -- 지
	    , a.landLeaseholdGbn -- 토지임대부 아파트 여부
	    , a.rgstDate -- 등기일자
	    , a.slerGbn -- 매도자
		, a.apt_trade_id -- 유니크 해시 키
	FROM stg_apt_trade a    
	LEFT JOIN stg_stan_regin_cd b
		ON b.locathigh_cd != '0000000000'
		AND a.sggCd = b.gu_cd
		AND a.umdNm = b.locallow_nm
)
select * from final 