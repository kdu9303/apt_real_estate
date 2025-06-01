{{config(
        materialized='view'
    )
}}

with snapshot_naver_land_complex_info as (
    select * from {{ ref('snapshot_naver_land_complex_info') }}
),
snapshot_naver_land_complex_info_detail as (
    select * from {{ ref('snapshot_naver_land_complex_info_detail') }}
),
stg_stan_regin_cd as (
    select * from {{ ref('stg_stan_regin_cd') }}
),
dim_stan_regin_cd_gu as (
    select * from {{ ref('dim_stan_regin_cd_gu') }}
),
final as (
    select
        a.complexNo -- 단지코드
        , a.complexName -- 단지명
        , a.cortarNo -- 법정동코드
        , a.realEstateTypeCode -- 부동산유형코드
        , a.realEstateTypeName -- 부동산유형명
        , e.locallow_nm as locathigh_nm -- 상위지역명
        , c.locallow_nm  -- 법정동명
        , a.cortarAddress -- 법정동주소
        , a.detailAddress -- 상세주소
        , a.latitude -- 위도
        , a.longitude -- 경도
        , b.totalDongCount -- 동수
        , a.totalHouseholdCount -- 세대수
        , a.highFloor -- 최고층
        , a.lowFloor -- 최저층
        , a.useApproveYmd -- 사용승인일자
        , a.useApproveYmd_parsed -- 사용승인일자
        , a.useApproveYmd_year -- 사용승인년도
        , a.useApproveYmd_month -- 사용승인월
        , a.dealCount -- 매매 개수
        , a.leaseCount -- 전세 개수
        , a.rentCount -- 월세 개수
        , a.shortTermRentCount -- 단기임대 개수
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
        , b.representativeArea -- 최근 거래된 면적(㎡)
        , b.medianDealUnitPrice -- 최근 거래된 매매가
        , b.medianDealPrice -- 최근 거래된 매매가(만원)
        , b.medianRentPrice -- 최근 거래된 월세가(만원)
        , b.medianShortTermRentPrice -- 최근 거래된 단기가(만원)
        , a.createdAt -- 생성일자(자료 추출 기준)
    
    from snapshot_naver_land_complex_info a
    left join snapshot_naver_land_complex_info_detail b
        on a.complexNo = b.complexNo
        and b.dbt_valid_to is null
    left join stg_stan_regin_cd c
        on a.cortarno = c.region_cd
    left join dim_stan_regin_cd_gu e
        on c.locathigh_cd = e.region_cd
    where a.dbt_valid_to is null
)

select * from final