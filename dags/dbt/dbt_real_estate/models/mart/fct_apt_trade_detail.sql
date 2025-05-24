with fct_apt_trade as (
    select * from {{ ref('fct_apt_trade') }}
),
dim_br_recap_title_info as (
    select * from {{ ref('dim_br_recap_title_info') }}
),
dim_stan_regin_cd_gu as (
    select * from {{ ref('dim_stan_regin_cd_gu') }}
),
final as (
    SELECT 
        a."지역코드"
        , a."법정동코드"
        , c."최하위지역명" as "상위지역명"
        , a."법정동명"
        , a."아파트이름"
        , a."아파트동명"
        , a."건축년도"
        , a."매수자"
        , a."해제사유발생일자"
        , a."해제여부"
        , a."거래금액"
        , a."거래일자"
        , a."계약일자"
        , a."계약월"
        , a."계약년도"
        , a."거래유형"
        , a."중개사소재지"
        , a."전용면적"
        , a."층수"
        , a."지번_번"
        , a."지번_지"
        , a."토지임대부아파트여부"
        , a."등기일자"
        , a."매도자"
        , b."대지면적"
        , b."건축면적"
        , b."건폐율"
        , b."용적률산정연면적"
        , b."용적률"
        , b."세대수"
    FROM fct_apt_trade a
    left JOIN dim_br_recap_title_info b
        ON a."지역코드" = b."시군구코드"
        and a."법정동코드" = b."법정동코드"
        AND a."지번_번" = b."지번_번"
        AND a."지번_지" = b."지번_지"
        and b."대장구분코드명" = '집합'
    left JOIN dim_stan_regin_cd_gu c
        ON a."지역코드" = c."중위지역코드"
)
select * from final

