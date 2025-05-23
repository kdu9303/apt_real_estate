{% macro parse_valid_date(date_str) %}
    case
        -- NULL 처리
        when {{ date_str }} is null then null

        -- 8자리 날짜 (YYYYMMDD)
        when length({{ date_str }}) = 8 and regexp_like({{ date_str }}, '^[0-9]{8}$')
        then try(
            cast(date_parse(
                substr({{ date_str }}, 1, 4) ||
                lpad(
                    case when cast(substr({{ date_str }}, 5, 2) as integer) between 1 and 12
                        then substr({{ date_str }}, 5, 2)
                        else '01' end, 2, '0') ||
                lpad(
                    case when cast(substr({{ date_str }}, 7, 2) as integer) between 1 and 31
                        then substr({{ date_str }}, 7, 2)
                        else '01' end, 2, '0'),
                '%Y%m%d') as date)
        )

        -- 6자리 (YYYYMM) : 앞 4자리가 연도(19xx~20xx), 뒤 2자리가 월
        when length({{ date_str }}) = 6
            and regexp_like({{ date_str }}, '^[0-9]{6}$')
            and cast(substr({{ date_str }}, 1, 4) as integer) between 1900 and 2099
            and cast(substr({{ date_str }}, 5, 2) as integer) between 1 and 12
        then try(
            cast(date_parse(
                substr({{ date_str }}, 1, 4) || substr({{ date_str }}, 5, 2) || '01',
                '%Y%m%d') as date)
        )

        -- 6자리 (YYMMDD) : 앞 2자리가 연도
        when length({{ date_str }}) = 6 and regexp_like({{ date_str }}, '^[0-9]{6}$')
        then try(
            cast(date_parse(
                (case when cast(substr({{ date_str }}, 1, 2) as integer) > 50
                    then '19' else '20' end) ||
                substr({{ date_str }}, 1, 2) ||
                lpad(
                    case when cast(substr({{ date_str }}, 3, 2) as integer) between 1 and 12
                        then substr({{ date_str }}, 3, 2)
                        else '01' end, 2, '0') ||
                lpad(
                    case when cast(substr({{ date_str }}, 5, 2) as integer) between 1 and 31
                        then substr({{ date_str }}, 5, 2)
                        else '01' end, 2, '0'),
                '%Y%m%d') as date)
        )

        -- 4자리 날짜 (YYYY)
        when length({{ date_str }}) = 4 and regexp_like({{ date_str }}, '^[0-9]{4}$')
        then try(
            cast(date_parse(concat({{ date_str }}, '0101'), '%Y%m%d') as date)
        )

        -- 7자리 날짜 (YYYYDDD) + 일수 유효성 (1~366)
        when length({{ date_str }}) = 7 and regexp_like({{ date_str }}, '^[0-9]{7}$')
            and cast(substr({{ date_str }}, 5, 3) as integer) between 1 and 366
        then try(
            cast(date_parse(
                concat(substr({{ date_str }}, 1, 4), lpad(substr({{ date_str }}, 5, 3), 3, '0')),
                '%Y%j') as date)
        )

        -- 7자리 날짜 (YYYYDDD) + 일수 범위 벗어나면 001로 대체
        when length({{ date_str }}) = 7 and regexp_like({{ date_str }}, '^[0-9]{7}$')
            and (cast(substr({{ date_str }}, 5, 3) as integer) < 1 or cast(substr({{ date_str }}, 5, 3) as integer) > 366)
        then try(
            cast(date_parse(
                concat(substr({{ date_str }}, 1, 4), '001'),
                '%Y%j') as date)
        )

        else null
    end
{% endmacro %} 