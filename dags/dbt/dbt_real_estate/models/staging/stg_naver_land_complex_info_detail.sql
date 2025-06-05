{{ 
  config(
    tags = ["real-estate", "staging_naver_land_complex_info"]
  ) 
}}

with source_naver_land_complex_info_detail as (
    select * from {{ source('src_trino', 'source_naver_land_complex_info_detail') }}
),
typecast_naver_land_complex_info_detail as (
    SELECT 
        a.markerId as complexNo -- 단지코드
        , a.markerType -- 단지유형
        , a.latitude -- 위도
        , a.longitude -- 경도
        , trim(a.complexName) complexName -- 단지명
        , a.realEstateTypeCode -- 부동산유형코드
        , a.realEstateTypeName -- 부동산유형명
        , a.completionYearMonth -- 준공년월
        , a.totalDongCount -- 동수
        , a.totalHouseholdCount -- 세대수
        , a.floorAreaRatio -- 용적률
        , a.minDealUnitPrice -- 매매 최저평당가
        , a.maxDealUnitPrice -- 매매 최고평당가
        , a.minLeaseUnitPrice -- 전세 최저평당가
        , a.maxLeaseUnitPrice -- 전세 최고평당가
        , a.minLeaseRate -- 최저 매매가 대비 전세가격 비율
        , a.maxLeaseRate -- 최고 매매가 대비 전세가격 비율
        , a.minArea -- 최소 공급면적(㎡)
        , a.maxArea -- 최대 공급면적(㎡)
        , a.minDealPrice -- 최저 매매가(만원)
        , a.maxDealPrice -- 최고 매매가(만원)
        , a.minLeasePrice -- 최저 전세가(만원)
        , a.maxLeasePrice -- 최고 전세가(만원)
        , a.minRentPrice -- 최저 월세가(만원)
        , a.maxRentPrice -- 최고 월세가(만원)
        , a.minShortTermRentPrice -- 최저 단기임대가(만원)
        , a.maxShortTermRentPrice -- 최대 단기임대가격
        , a.isDealShown -- 매매 표시 여부
        , a.isLeaseShown -- 임대 표시 여부
        , a.isRentShown -- 임대 표시 여부
        , a.isShortTermRentShown -- 단기임대 표시 여부
        , a.priceCount -- 가격 개수
        , a.representativeArea -- 최근 거래된 면적(㎡)
        , a.medianDealUnitPrice -- 최근 거래된 매매가
        , a.medianDealPrice -- 최근 거래된 매매가(만원)
        , a.medianRentPrice -- 최근 거래된 월세가(만원)
        , a.medianShortTermRentPrice -- 최근 거래된 단기가(만원)
        , a.isPresales -- 선분양 여부
        , a.representativePhoto -- 대표 사진
        , a.photoCount -- 사진 개수
        , a.dealCount -- 매매 개수
        , a.leaseCount -- 전세 개수
        , a.rentCount -- 월세 개수
        , a.shortTermRentCount -- 단기임대 개수
        , a.totalArticleCount -- 총 게시글 개수
        , a.existPriceTab -- 가격 탭 존재 여부
        , a.isComplexTourExist -- 단지 투어 존재 여부
        , a.createdAt -- 생성일자(자료 추출 기준)
        , a.naver_land_complex_info_detail_id -- 유니크 해시 키
        , CAST(
			SUBSTRING(a.createdAt, 1, 4) || '-' ||
			SUBSTRING(a.createdAt, 5, 2) || '-' ||
			SUBSTRING(a.createdAt, 7, 2) || ' 00:00:00'
			AS TIMESTAMP
        ) AS createdAt_timestamp
    FROM source_naver_land_complex_info_detail a
),
final as (
    SELECT 
        b.complexNo -- 단지코드
        , b.markerType -- 단지유형
        , b.latitude -- 위도
        , b.longitude -- 경도
        , b.complexName -- 단지명
        , b.realEstateTypeCode -- 부동산유형코드
        , b.realEstateTypeName -- 부동산유형명
        , b.completionYearMonth -- 준공년월
        , b.totalDongCount -- 동수
        , b.totalHouseholdCount -- 세대수
        , b.floorAreaRatio -- 용적률
        , b.minDealUnitPrice -- 매매 최저평당가
        , b.maxDealUnitPrice -- 매매 최고평당가
        , b.minLeaseUnitPrice -- 전세 최저평당가
        , b.maxLeaseUnitPrice -- 전세 최고평당가
        , b.minLeaseRate -- 최저 매매가 대비 전세가격 비율
        , b.maxLeaseRate -- 최고 매매가 대비 전세가격 비율
        , b.minArea -- 최소 공급면적(㎡)
        , b.maxArea -- 최대 공급면적(㎡)
        , b.minDealPrice -- 최저 매매가(만원)
        , b.maxDealPrice -- 최고 매매가(만원)
        , b.minLeasePrice -- 최저 전세가(만원)
        , b.maxLeasePrice -- 최고 전세가(만원)
        , b.minRentPrice -- 최저 월세가(만원)
        , b.maxRentPrice -- 최고 월세가(만원)
        , b.minShortTermRentPrice -- 최저 단기임대가(만원)
        , b.maxShortTermRentPrice -- 최대 단기임대가격
        , b.isDealShown -- 매매 표시 여부
        , b.isLeaseShown -- 임대 표시 여부
        , b.isRentShown -- 임대 표시 여부
        , b.isShortTermRentShown -- 단기임대 표시 여부
        , b.priceCount -- 가격 개수
        , b.representativeArea -- 최근 거래된 면적(㎡)
        , b.medianDealUnitPrice -- 최근 거래된 매매가
        , b.medianDealPrice -- 최근 거래된 매매가(만원)
        , b.medianRentPrice -- 최근 거래된 월세가(만원)
        , b.medianShortTermRentPrice -- 최근 거래된 단기가(만원)
        , b.isPresales -- 선분양 여부
        , b.representativePhoto -- 대표 사진
        , b.photoCount -- 사진 개수
        , b.dealCount -- 매매 개수
        , b.leaseCount -- 전세 개수
        , b.rentCount -- 월세 개수
        , b.shortTermRentCount -- 단기임대 개수
        , b.totalArticleCount -- 총 게시글 개수
        , b.existPriceTab -- 가격 탭 존재 여부
        , b.isComplexTourExist -- 단지 투어 존재 여부
        , b.createdAt -- 생성일자(자료 추출 기준)
        , b.naver_land_complex_info_detail_id -- 유니크 해시 키
        , b.createdAt_timestamp -- 생성일자(자료 추출 기준)
    from
        (
            select 
                row_number() over (partition by a.complexNo order by a.medianShortTermRentPrice) as seq
                , a.* 
            from typecast_naver_land_complex_info_detail a
        ) b
    where b.seq = 1
)
select * from final