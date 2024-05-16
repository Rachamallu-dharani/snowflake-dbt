{{
    config(
        materialized = 'incremental',
        unique_key='child_surrogate_id',
        incremental_strategy='append',
        on_schema_change='append_new_columns'
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