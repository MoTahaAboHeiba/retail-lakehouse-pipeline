SELECT 
    DISTINCT
    employee_id,
    first_name AS employee_first_name,
    last_name AS employee_last_name,
    email AS employee_email,
    job_title,
    salary,
    store_id,
    is_active AS employee_is_active
    updated_timestamp AS employee_updated_timestamp
FROM 
    {{ ref('employees_tech') }}
