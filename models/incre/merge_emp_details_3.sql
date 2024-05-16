{{
    config(
        materialized = 'incremental',
        unique_key='surrogate_id',
        incremental_strategy='merge',
        merge_exclude_columns = ['emp_id','kind','updated_date'],
        on_schema_change='append_new_columns'
) }}

WITH target_dim_emp_details_3 AS (
    SELECT * FROM {{ ref('target_dim_emp_details_3') }}
)
SELECT 
  *
FROM target_dim_emp_details_3
{% if is_incremental() %}
  where updated_date > (select max(updated_date) from {{ this }})
{% endif %}