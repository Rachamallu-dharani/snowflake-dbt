{{
    config(
        materialized = 'incremental',
        unique_key='yearslived_surrogate_id',
        incremental_strategy='delete+insert',
        on_schema_change='append_new_columns'
) }}
WITH target_dim_emp_yearsLived_3 AS (
    SELECT * FROM {{ ref('target_dim_emp_yearsLived_3') }}
)
SELECT
  *
  FROM target_dim_emp_yearsLived_3
{% if is_incremental() %}
  where updated_date > (select max(updated_date) from {{ this }})
{% endif %}