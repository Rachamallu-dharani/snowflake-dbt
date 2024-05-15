{% macro emp_transform_gender_3(gender_column) %}
        case 
            when {{ gender_column }} = 'Male' then 'M' 
            when {{ gender_column }} = 'Female' then 'F' 
            else 'Unknown' 
        end
{% endmacro %}
