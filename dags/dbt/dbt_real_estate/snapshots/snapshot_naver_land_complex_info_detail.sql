{% snapshot snapshot_naver_land_complex_info_detail %}
    {{config(
        target_schema='snapshots-real-estate',
        unique_key='naver_land_complex_info_detail_id',
        strategy='timestamp',
        updated_at='createdAt_timestamp',
        hard_deletes='ignore',
        tags = ["real-estate", "snapshot_naver_land_complex_info_detail"]
    )
}}

SELECT * FROM {{ ref('stg_naver_land_complex_info_detail') }} 

{% endsnapshot %}