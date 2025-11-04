-- group by company
-- Every company currently appears only once
-- meaning merged dataset was created by matching 
-- one person to one company by index, so there is no natural grouping here
SELECT 
    company_name,
    COUNT(*) AS employee_count
FROM 
    week8_demo.employees
GROUP BY 
    company_name
ORDER BY 
    employee_count DESC
LIMIT 20;


-- Most Common Email Provider (Communication Pattern)
SELECT 
    SPLIT_PART(email, '@', 2) AS email_domain,
    COUNT(*) AS num_users
FROM week8_demo.employees
GROUP BY email_domain
ORDER BY num_users DESC
LIMIT 15;


-- Most Common Last Names (Demographics Insight)
SELECT 
    lastname,
    COUNT(*) AS name_count
FROM week8_demo.employees
GROUP BY lastname
ORDER BY name_count DESC
LIMIT 10;

-- SELECT firstname, COUNT(*) AS name_count
SELECT firstname, COUNT(*) AS name_count
FROM week8_demo.employees
GROUP BY firstname
ORDER BY name_count DESC
LIMIT 10;

-- Company Name Structure Patterns
SELECT
    CASE
        WHEN company_name ILIKE '%LLC%' THEN 'LLC'
        WHEN company_name ILIKE '%Group%' THEN 'Group'
        WHEN company_name ILIKE '%Inc%' THEN 'Inc'
        WHEN company_name ILIKE '%PLC%' THEN 'PLC'
        WHEN company_name ILIKE '%and%' THEN 'Family / Partnership Name'
        ELSE 'Other'
    END AS company_type,
    COUNT(*) AS count
FROM week8_demo.employees
GROUP BY company_type
ORDER BY count DESC;



