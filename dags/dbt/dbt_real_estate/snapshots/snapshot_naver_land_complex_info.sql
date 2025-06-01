{% snapshot snapshot_naver_land_complex_info %}
    {{config(
        target_schema='snapshots-real-estate',
        unique_key='naver_land_complex_info_id',
        strategy='timestamp',
        updated_at='createdAt_timestamp',
        hard_deletes='new_record'
    )
}}

SELECT * FROM {{ ref('stg_naver_land_complex_info') }} 

{% endsnapshot %}