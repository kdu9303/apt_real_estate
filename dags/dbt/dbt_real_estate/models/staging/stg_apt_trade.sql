with source_apt_trade as (
    select * from {{ source('src_trino','source_apt_trade') }}
),
nullif_apt_trade as (
	SELECT 
		a.aptTradeId  -- 해시 키
		, nullif(trim(a.aptDong), '') aptDong -- 아파트 동명
	    , nullif(trim(a.aptNm), '') aptNm -- 아파트 이름
	    , a.buildYear -- 건축년도
	    , nullif(a.buyerGbn, '') buyerGbn -- 매수자
	    , nullif(a.cdealDay, '') cdealDay -- 해제사유발생일
	    , nullif(a.cdealType, '') cdealType -- 해제여부
	    , replace(a.dealAmount,',','') dealAmount -- 거래래 금액
	    , a.dealDay -- 계약 일자
	    , a.dealMonth -- 계약 월
	    , a.dealYear -- 계약 년도도
	    , nullif(a.dealingGbn, '') dealingGbn -- 거래 유형
	    , nullif(trim(a.estateAgentSggNm), '') estateAgentSggNm -- 중개사소재지
	    , nullif(a.excluUseAr, '') excluUseAr -- 전용면적
	    , nullif(a.floor, '') floor -- 층수
	    , nullif(a.jibun, '') jibun -- 지번
	    , nullif(a.landLeaseholdGbn, '') landLeaseholdGbn -- 토지임대부 아파트 여부
	    , nullif(a.rgstDate, '') rgstDate -- 등기일자
	    , a.sggCd -- 지역코드
	    , nullif(a.slerGbn, '') slerGbn -- 매도자
	    , trim(a.umdNm) umdNm -- 법정동명
	FROM source_apt_trade a
),
typecast_apt_trade as (
	SELECT 
		b.aptTradeId  -- 해시 키
		, b.aptDong -- 아파트 동명
	    , b.aptNm -- 아파트 이름
	    , b.buildYear -- 건축년도
	    , b.buyerGbn -- 매수자
	    , cast(date_parse(b.cdealDay, '%y.%m.%d') AS date) AS cdealDay -- 해제사유발생일
	    , b.cdealType -- 해제여부
	    , cast(b.dealAmount as int) dealAmount -- 거래래 금액
	    , b.dealDay -- 계약 일자
	    , b.dealMonth -- 계약 월
	    , b.dealYear -- 계약 년도도
	    , b.dealingGbn -- 거래 유형
	    , b.estateAgentSggNm -- 중개사소재지
	    , cast(b.excluUseAr as double) excluUseAr -- 전용면적
	    , b.floor -- 층수
	    , b.jibun -- 지번
	    , b.landLeaseholdGbn -- 토지임대부 아파트 여부
	    , cast(date_parse(b.rgstDate, '%y.%m.%d') as date) rgstDate-- 등기일자
	    , b.sggCd -- 지역코드
	    , b.slerGbn -- 매도자
	    , b.umdNm -- 법정동명
	FROM nullif_apt_trade b
),
final as (
	-- 칼럼 순서 정렬
	SELECT 
		c.aptTradeId  -- 해시 키
		, c.sggCd -- 지역코드
	    , c.umdNm -- 법정동명
	    , c.aptNm -- 아파트 이름
		, c.aptDong -- 아파트 동명
	    , c.buildYear -- 건축년도
	    , c.buyerGbn -- 매수자
	    , c.cdealDay -- 해제사유발생일
	    , c.cdealType -- 해제여부
	    , c.dealAmount -- 거래래 금액
	    , cast(
			c.dealYear || '-' || lpad(c.dealMonth, 2, '0') || '-' || lpad(c.dealDay, 2, '0')
			AS date
		) AS dealDate
	    , c.dealDay -- 계약 일자
	    , c.dealMonth -- 계약 월
	    , c.dealYear -- 계약 년도도
	    , c.dealingGbn -- 거래 유형
	    , c.estateAgentSggNm -- 중개사소재지
	    , c.excluUseAr -- 전용면적
	    , c.floor -- 층수
	    , c.jibun -- 지번
	    , c.landLeaseholdGbn -- 토지임대부 아파트 여부
	    , c.rgstDate -- 등기일자
	    , c.slerGbn -- 매도자
	FROM typecast_apt_trade c
)
select * from final

