{{
    config(
        materialized='incremental',
        unique_key='ID'
    )
}}

select
    *

from {{ ref('table1') }}

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses >= to include records whose timestamp occurred since the last run of this model)
  where system_start_ts >= (select COALESCE(max(system_start_ts), '2015-01-01') from {{ this }} )

{% endif %}