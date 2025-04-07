{{
    config(
        materialized='incremental',
        driver_key=['id','date_key'],
        incremental_strategy='manual_dml',
        timestamp_column='etl_loaded_at',
        exclude_columns=['etl_loaded_at', 'last_modified','effective_timestamp'],
        temp_relation='raw_customers'
    )
}}

select
    id,
    name,
    email,
    address,
    date_key,
    last_modified as effective_timestamp,  -- Source timestamp
    etl_loaded_at as last_modified                       -- Audit column
from {{ ref('raw_customers') }}

{% if is_incremental() %}
where etl_loaded_at > (  -- Filter on source system timestamp
    select coalesce(max(last_modified), '2015-01-01'::timestamp)
    from {{ this }}       -- Compare against target's timestamp column
)
{% endif %}