{{
    config(
        materialized = 'incremental',
        unique_key='surrogate_id',
        on_schema_change='fail'
) }}
WITH src_staging_emp_data3 AS (
  SELECT
    *
  FROM
    {{ ref('src_staging_emp_data3') }}
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['empid' ,'fullname','age','gender']) }} as surrogate_id,
    raw_id,
    empid,
    kind,
	 fullname,
	 age,
   {{ emp_transform_gender_3('gender') }} as gender,
    areacode,
	  phonenumber,
    updated_date 
FROM
    src_staging_emp_data3
group by raw_id,empid,kind,fullname,age,gender,areacode,phonenumber,updated_date
{% if is_incremental() %}
  having updated_date > (select max(updated_date) from {{ this }})
{% endif %}