{{
    config(
        materialized = 'incremental',
        unique_key='citislived_surrogate_id',
        on_schema_change='fail'
) }}
WITH src_staging_emp_data3 AS (
  SELECT
    *
  FROM
    {{ ref('src_staging_emp_data3') }}
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['empid' ,'city_name']) }} as citislived_surrogate_id,
    raw_id,
    empid,
    city_name,
	updated_date
FROM
    src_staging_emp_data3
group by raw_id,empid,city_name,updated_date
{% if is_incremental() %}
  having updated_date > (select max(updated_date) from {{ this }})
{% endif %}
