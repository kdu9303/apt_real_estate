with stg_stan_regin_cd as (
    select * from {{ ref('stg_stan_regin_cd') }}
),
final as (
    SELECT 
        a.region_cd -- 지역코드
        , a.gu_cd -- 중위지역코드(다른테이블의 join key)
        , b.locallow_nm as "gu_nm" -- 중위지역코드명
        , a.bjdong_cd -- 법정동코드(다른테이블의 join key)
        , a.locallow_nm -- 최하위지역명
        , a.locatadd_nm -- 지역주소명
        , a.sido_cd -- 시도코드
        , a.sgg_cd -- 시군구코드
        , a.umd_cd -- 읍면동코드
        , a.ri_cd -- 리코드
        , a.locatjumin_cd -- 지역주민코드
        , a.locatjijuk_cd -- 지역코드_지적
        , a.locat_order -- 서열
        , a.locat_rm -- 비고
        , a.locathigh_cd -- 상위지역코드
        , a.adpt_de -- 생성일(YYYYMMDD)
    FROM stg_stan_regin_cd a
    LEFT JOIN stg_stan_regin_cd b
    	ON b.bjdong_cd = '00000'
    	AND b.locathigh_cd != '0000000000'
    	AND a.gu_cd = b.gu_cd
)
select * from final
