{{
    config(
        materialized = 'incremental',
        unique_key='yearslived_surrogate_id',
        on_schema_change='fail'
) }}
WITH src_staging_emp_data3 AS (
  SELECT
    *
  FROM
    {{ ref('src_staging_emp_data3') }}
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['empid' ,'year',]) }} as yearslived_surrogate_id,
    {{ emp_create_date_updated_date_3()}}
    empid,
    year
FROM
    src_staging_emp_data3
group by raw_id,empid,year,created_date,updated_date
{% if is_incremental() %}
  having updated_date > (select max(updated_date) from {{ this }})
{% endif %}