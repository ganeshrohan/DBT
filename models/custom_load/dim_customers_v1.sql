{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge',
    alias='dim_customers_v1',
) }}

with source as (

    select
        id,
        name,
        email,
        address,
        NULL as hash_key,
        last_modified as effective_timestamp,
        etl_loaded_at as last_modified

    from {{ ref('raw_customers') }}

    {% if is_incremental() %}
      -- get only changed or new rows
      where etl_loaded_at > (select COALESCE(max(last_modified),'2015-01-01') from {{ this }})
    {% endif %}
)

select * from source
