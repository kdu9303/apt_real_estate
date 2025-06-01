with source_naver_land_complex_info as (
    select * from {{ source('src_trino', 'source_naver_land_complex_info') }}
),
typecast_naver_land_complex_info as (
    SELECT 
        a.complexNo -- 단지코드
        , a.complexName -- 단지명
        , a.cortarNo -- 법정동코드
        , a.realEstateTypeCode -- 부동산유형코드
        , a.realEstateTypeName -- 부동산유형명
        , a.detailAddress -- 상세주소
        , a.latitude -- 위도
        , a.longitude -- 경도
        , a.totalHouseholdCount -- 세대수
        , a.totalBuildingCount -- 건물수
        , a.highFloor -- 최고층
        , a.lowFloor -- 최저층
        , a.useApproveYmd -- 사용승인일자
        , {{ parse_valid_date('useApproveYmd') }} useApproveYmd_parsed -- 사용승인일자
        , a.dealCount -- 매매 개수
        , a.leaseCount -- 임대 개수
        , a.rentCount -- 임대 개수
        , a.shortTermRentCount -- 단기임대 개수
        , a.cortarAddress -- 법정동 주소
        , a.tourExist -- 단지 투어 존재 여부
        , a.createdAt -- 생성일자(자료 추출 기준)
        , a.naver_land_complex_info_id -- 유니크 해시 키
        , CAST(
			SUBSTRING(a.createdAt, 1, 4) || '-' ||
			SUBSTRING(a.createdAt, 5, 2) || '-' ||
			SUBSTRING(a.createdAt, 7, 2) || ' 00:00:00'
			AS TIMESTAMP
        ) AS createdAt_timestamp
    FROM source_naver_land_complex_info a
),
final as (
    SELECT 
        b.complexNo -- 단지코드
        , b.complexName -- 단지명
        , b.cortarNo -- 법정동코드
        , b.realEstateTypeCode -- 부동산유형코드
        , b.realEstateTypeName -- 부동산유형명
        , b.detailAddress -- 상세주소
        , b.latitude -- 위도
        , b.longitude -- 경도
        , b.totalHouseholdCount -- 세대수
        , b.totalBuildingCount -- 건물수
        , b.highFloor -- 최고층
        , b.lowFloor -- 최저층
        , b.useApproveYmd -- 사용승인일자
        , b.useApproveYmd_parsed -- 사용승인일자
        , extract(year from b.useApproveYmd_parsed) useApproveYmd_year -- 사용승인년도
        , extract(month from b.useApproveYmd_parsed) useApproveYmd_month -- 사용승인월
        , b.dealCount -- 매매 개수
        , b.leaseCount -- 임대 개수
        , b.rentCount -- 임대 개수
        , b.shortTermRentCount -- 단기임대 개수
        , b.cortarAddress -- 법정동 주소
        , b.tourExist -- 단지 투어 존재 여부
        , b.createdAt -- 생성일자(자료 추출 기준)
        , b.naver_land_complex_info_id -- 유니크 해시 키
        , b.createdAt_timestamp -- 생성일자(자료 추출 기준)
    FROM typecast_naver_land_complex_info b
)

select * from final