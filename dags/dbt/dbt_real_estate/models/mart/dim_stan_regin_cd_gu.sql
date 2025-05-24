with stg_stan_regin_cd as (
    select * from {{ ref('stg_stan_regin_cd') }}
),
final as (
    SELECT 
        a.region_cd as "지역코드" -- 지역코드
	    , a.gu_cd as "중위지역코드" -- 중위지역코드(다른테이블의 join key)
	    , a.bjdong_cd as "법정동코드" -- 법정동코드(다른테이블의 join key)
	    , a.locallow_nm as "최하위지역명" -- 최하위지역명
	    , a.locatadd_nm as "지역주소명" -- 지역주소명
	    , a.locatjumin_cd as "지역주민코드" -- 지역주민코드
	    , a.locatjijuk_cd as "지역코드_지적" -- 지역코드_지적
	    , a.locathigh_cd as "상위지역코드" -- 상위지역코드
	    , a.adpt_de as "생성일자" -- 생성일(YYYYMMDD)
    FROM stg_stan_regin_cd a
    WHERE a.bjdong_cd = '00000'
    AND a.locathigh_cd != '0000000000'
)
select * from final
