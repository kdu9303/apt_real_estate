with stg_naver_land_geo_info as (
    select * from {{ ref('stg_naver_land_geo_info') }}
),
stg_stan_regin_cd as (
    select * from {{ ref('stg_stan_regin_cd') }}
),
final as (
    select 
        b.locathigh_cd as region_cd,
        a.cortarNo,
        a.centerLat,
        a.centerLon,
        a.cortarName,
        a.cortarType
    from stg_naver_land_geo_info a
    left join stg_stan_regin_cd b
        on a.cortarNo = b.region_cd
        
)
select * from final