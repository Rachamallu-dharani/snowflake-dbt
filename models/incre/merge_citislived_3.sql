{{
    config(
        materialized = 'incremental',
        unique_key='citislived_surrogate_id',
        incremental_strategy='merge',
        merge_update_columns = ['city_name'],
        on_schema_change='append_new_columns'
) }}
WITH target_dim_emp_citiesLived_3 AS (
    SELECT * FROM {{ ref('target_dim_emp_citiesLived_3') }}
)
SELECT 
  *
  FROM target_dim_emp_citiesLived_3
{% if is_incremental() %}
  where updated_date > (select max(updated_date) from {{ this }})
{% endif %}
