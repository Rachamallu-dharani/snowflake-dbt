{{
    config(
        materialized = 'incremental',
        unique_key='child_surrogate_id',
        incremental_strategy='merge',
        merge_exclude_columns = ['empid','updated_date'],
        on_schema_change='fail'
) }}
WITH target_dim_emp_children_3 AS (
    SELECT * FROM {{ ref('target_dim_emp_children_3') }}
)
SELECT 
  *
  FROM target_dim_emp_children_3
{% if is_incremental() %}
  where updated_date > (select max(updated_date) from {{ this }})
{% endif %}