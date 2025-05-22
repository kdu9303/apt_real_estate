with source_stan_regin_cd as (
    select * from {{ source('src_trino','source_stan_regin_cd') }}
),
nullif_stan_regin_cd as (
	SELECT 
	    a.region_cd -- 지역코드
	    , a.sido_cd -- 시도코드
	    , a.sgg_cd -- 시군구코드
	    , a.umd_cd -- 읍면동코드
	    , a.ri_cd -- 리코드
	    , a.locatjumin_cd -- 지역주민코드
	    , a.locatjijuk_cd -- 지역코드_지적
	    , trim(a.locatadd_nm) locatadd_nm -- 지역주소명
	    , a.locat_order -- 서열
	    , nullif(a.locat_rm, '') locat_rm -- 비고
	    , a.locathigh_cd -- 상위지역코드
	    , trim(a.locallow_nm) locallow_nm -- 최하위지역명
	    , nullif(a.adpt_de, '') adpt_de -- 생성일(YYYYMMDD)
	    , a.stan_regin_cd_id -- 유니크 해시 키
	FROM source_stan_regin_cd a
),
typecast_stan_regin_cd as (
	SELECT 
	    b.region_cd -- 지역코드
	    , b.sido_cd -- 시도코드
	    , b.sgg_cd -- 시군구코드
	    , b.umd_cd -- 읍면동코드
	    , b.ri_cd -- 리코드
	    , b.locatjumin_cd -- 지역주민코드
	    , b.locatjijuk_cd -- 지역코드_지적
	    , b.locatadd_nm -- 지역주소명
	    , b.locat_order -- 서열
	    , b.locat_rm -- 비고
	    , b.locathigh_cd -- 상위지역코드
	    , b.locallow_nm -- 최하위지역명
	    , cast(date_parse(b.adpt_de, '%Y%m%d') as date) adpt_de -- 생성일(YYYYMMDD)
	    , b.stan_regin_cd_id -- 유니크 해시 키
	FROM nullif_stan_regin_cd b
),
final as (
	SELECT 
	    c.region_cd -- 지역코드
	    , substr(c.region_cd, 1, 5) gu_cd -- 구코드(다른테이블의 join key)
	    , substr(c.region_cd, 6) bjdong_cd -- 법정동코드(다른테이블의 join key)
	    , c.locallow_nm -- 최하위지역명
	    , c.locatadd_nm -- 지역주소명
	    , c.sido_cd -- 시도코드
	    , c.sgg_cd -- 시군구코드
	    , c.umd_cd -- 읍면동코드
	    , c.ri_cd -- 리코드
	    , c.locatjumin_cd -- 지역주민코드
	    , c.locatjijuk_cd -- 지역코드_지적
	    , c.locat_order -- 서열
	    , c.locat_rm -- 비고
	    , c.locathigh_cd -- 상위지역코드
	    , c.adpt_de -- 생성일(YYYYMMDD)
	    , c.stan_regin_cd_id -- 유니크 해시 키
	FROM typecast_stan_regin_cd c
)
select * from final

