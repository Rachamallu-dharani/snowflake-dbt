{{
    config(
        materialized = 'incremental',
        unique_key='child_surrogate_id',
        on_schema_change='fail'
) }}
WITH src_staging_emp_data3 AS (
  SELECT
    *
  FROM
    {{ ref('src_staging_emp_data3') }}
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['empid' ,'child_name','child_gender','child_age']) }} as child_surrogate_id,
    {{ emp_create_date_updated_date_3()}}
    empid,
    child_name,
    {{ emp_transform_gender_3('child_gender') }} as child_gender,
	  child_age
FROM
    src_staging_emp_data3
group by raw_id,empid,child_name,child_gender,child_age,created_date,updated_date
{% if is_incremental() %}
  having updated_date > (select max(updated_date) from {{ this }})
{% endif %}