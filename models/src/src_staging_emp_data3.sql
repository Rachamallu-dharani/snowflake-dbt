WITH stage_raw_table_3 AS (
    SELECT * FROM AIRBNB.RAW.stage_raw_table_3
)
SELECT
    raw_id,
    emp_id as empid,
    emp_json_payload:kind::string as kind,
    emp_json_payload:fullName::string as fullName,
    emp_json_payload:age::number as age,
    emp_json_payload:gender::string as gender,
    emp_json_payload:phoneNumber:areaCode::number as areaCode,
    emp_json_payload:phoneNumber:number::number as phoneNumber,
    child.value:name::string as child_name,
    child.value:gender::string as child_gender,
    child.value:age::number as child_age,
    city.value:place::string as city_name,
    year.value::number as year,
    created_date,
    updated_date,
FROM stage_raw_table_3,
    LATERAL FLATTEN(
        COALESCE(
            emp_json_payload:children, array_construct(
                object_construct('name', 'Unknown', 'gender', 'Unknown', 'age', 0)
                )
        )
    ) as child,
    LATERAL FLATTEN(emp_json_payload:citiesLived, outer => true) as city,
    LATERAL FLATTEN(city.value:yearsLived, outer => true) as year