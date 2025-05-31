with source_naver_land_geo_info as (
    select * from {{ source('src_trino', 'source_naver_land_geo_info') }}
),
final as (
    select 
        a.cortarNo,
        a.centerLat,
        a.centerLon,
        a.cortarName,
        a.cortarType,
        a.naver_land_geo_info_id
    from source_naver_land_geo_info a
)
select * from final