with stg_apt_trade as (
    select * from {{ ref('stg_apt_trade') }}
),
stg_stan_regin_cd as (
    select * from {{ ref('stg_stan_regin_cd') }}
),
final as (
	SELECT 
		a.sggCd as "지역코드" -- 지역코드
		, b.bjdong_cd as "법정동코드"
	    , a.umdNm as "법정동명" -- 법정동명
	    , a.aptNm as "아파트이름" -- 아파트 이름
		, a.aptDong as "아파트동명" -- 아파트 동명
	    , a.buildYear as "건축년도" -- 건축년도
	    , a.buyerGbn as "매수자" -- 매수자
	    , a.cdealDay as "해제사유발생일자" -- 해제사유발생일자
	    , a.cdealType as "해제여부" -- 해제여부
	    , a.dealAmount as "거래금액" -- 거래 금액
	    , a.dealDate as "거래일자" -- 거래일자
	    , a.dealDay as "계약일자" -- 계약 일자
	    , a.dealMonth as "계약월" -- 계약 월
	    , a.dealYear as "계약년도" -- 계약 년도
	    , a.dealingGbn as "거래유형" -- 거래 유형
	    , a.estateAgentSggNm as "중개사소재지" -- 중개사소재지
	    , a.excluUseAr as "전용면적" -- 전용면적
	    , a.floor as "층수" -- 층수
	    , a.jibun as "지번" -- 지번
		, a.bun as "지번_번" -- 번
		, a.ji as "지번_지" -- 지
	    , a.landLeaseholdGbn as "토지임대부아파트여부" -- 토지임대부 아파트 여부
	    , a.rgstDate as "등기일자" -- 등기일자
	    , a.slerGbn as "매도자" -- 매도자
	FROM stg_apt_trade a    
	LEFT JOIN stg_stan_regin_cd b
		ON b.locathigh_cd != '0000000000'
		AND a.sggCd = b.gu_cd
		AND a.umdNm = b.locallow_nm
)
select * from final 