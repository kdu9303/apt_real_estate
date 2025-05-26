with stg_stan_regin_cd as (
    select * from {{ ref('stg_stan_regin_cd') }}
),
final as (
    SELECT 
        a.region_cd -- 지역코드
	    , a.gu_cd -- 중위지역코드(다른테이블의 join key)
	    , a.bjdong_cd -- 법정동코드(다른테이블의 join key)
	    , a.locallow_nm -- 최하위지역명
	    , a.locatadd_nm -- 지역주소명
	    , a.locatjumin_cd -- 지역주민코드
	    , a.locatjijuk_cd -- 지역코드_지적
	    , a.locathigh_cd -- 상위지역코드
	    , a.adpt_de -- 생성일(YYYYMMDD)
    FROM stg_stan_regin_cd a
    WHERE a.bjdong_cd = '00000'
    AND a.locathigh_cd != '0000000000'
)
select * from final
