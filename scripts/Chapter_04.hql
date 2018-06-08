--Apache Hive Essentials 
--Chapter 4 Code - Hive SELECT, JOIN, and UNION
--Query all columns in the table
SELECT * FROM employee;

--Select only one column
SELECT name FROM employee;

--List columns meet java regular expression
SET hive.support.quoted.identifiers = none;
SELECT `^work.*` FROM employee;

--Select unique rows
SELECT DISTINCT name, work_place FROM employee;

--Select with UDF, IF, and CASE WHEN
SELECT 
CASE WHEN gender_age.gender = 'Female' THEN 'Ms.'
ELSE 'Mr.' END as title,
name, 
IF(array_contains(work_place, 'New York'), 'US', 'CA') as country
FROM employee;

--Nest SELECT after the FROM
SELECT name, gender_age.gender AS gender
FROM(
SELECT * FROM employee
WHERE gender_age.gender = 'Male'
) t1;

--Nest SELECT using CTE
WITH t1 AS (
SELECT * FROM employee
WHERE gender_age.gender = 'Male')
SELECT name, gender_age.gender AS gender FROM t1;

--Select with expression
SELECT concat('1','+','3','=',cast((1 + 3) as string)) as res;

--Filter data with limit
SELECT name FROM employee LIMIT 2;

--Filter with Where
SELECT name, work_place FROM employee WHERE name = 'Michael';

--Filter with Where and Limit
SELECT name, work_place FROM employee WHERE name = 'Michael' LIMIT 1;

--Filter with in
SELECT name FROM employee WHERE gender_age.age in (27, 30);

--In for multiple columns Works after v2.1.0
SELECT 
name, gender_age
FROM employee 
WHERE (gender_age.gender , gender_age.age) IN (('Female', 27), ('Male', 27 + 3));

--Subquery in
SELECT name, gender_age.gender AS gender
FROM employee a
WHERE a.name IN (SELECT name FROM employee WHERE gender_age.gender = 'Male');

--Subquery exists
SELECT name, gender_age.gender AS gender
FROM employee a
WHERE EXISTS
(SELECT * FROM employee b WHERE a.gender_age.gender = b.gender_age.gender AND b.gender_age.gender = 'Male');
 
--Prepare another table for join and load data
CREATE TABLE IF NOT EXISTS employee_hr
(
  name string,
  employee_id int,
  sin_number string,
  start_date date
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

LOAD DATA INPATH '/tmp/hivedemo/data/employee_hr.txt' OVERWRITE INTO TABLE employee_hr;

--Equal JOIN between two tables
SELECT emp.name, emph.sin_number
FROM employee emp
JOIN employee_hr emph ON emp.name = emph.name;

--Unequal join returns more rows
SELECT 
emp.name, emph.sin_number
FROM employee emp 
JOIN employee_hr emph ON emp.name != emph.name;

--Join with complex expression - conditional join
SELECT 
emp.name, emph.sin_number
FROM employee emp
JOIN employee_hr emph ON 
IF(emp.name = 'Will', '1', emp.name) = CASE WHEN emph.name = 'Will' THEN '0' ELSE emph.name END;

-- Use Where to limit the output of join
SELECT 
emp.name, emph.sin_number
FROM employee emp
JOIN employee_hr emph ON emp.name = emph.name
WHERE
emp.name = 'Will';

--JOIN between more tables
SELECT emp.name, empi.employee_id, emph.sin_number
FROM employee emp
JOIN employee_hr emph ON emp.name = emph.name
JOIN employee_id empi ON emp.name = empi.name;

--Self join is used when the data in the table has nest logic
SELECT emp.name
FROM employee emp
JOIN employee emp_b
ON emp.name = emp_b.name;

--Implicit join, which support since Hive 0.13.0
SELECT emp.name, emph.sin_number
FROM employee emp, employee_hr emph
WHERE emp.name = emph.name;

--Join using different columns will create additional mapreduce
SELECT emp.name, empi.employee_id, emph.sin_number
FROM employee emp
JOIN employee_hr emph ON emp.name = emph.name
JOIN employee_id empi ON emph.employee_id = empi.employee_id;

--Streaming tables 
SELECT /*+ STREAMTABLE(employee_hr) */
emp.name, empi.employee_id, emph.sin_number
FROM employee emp
JOIN employee_hr emph ON emp.name = emph.name
JOIN employee_id empi ON emph.employee_id = empi.employee_id;

--Left JOIN
SELECT emp.name, emph.sin_number
FROM employee emp
LEFT JOIN employee_hr emph ON emp.name = emph.name;

--Right JOIN
SELECT emp.name, emph.sin_number
FROM employee emp
RIGHT JOIN employee_hr emph ON emp.name = emph.name;

--Full OUTER JOIN
SELECT emp.name, emph.sin_number
FROM employee emp
FULL JOIN employee_hr emph ON emp.name = emph.name;

--CROSS JOIN in different ways
SELECT emp.name, emph.sin_number
FROM employee emp
CROSS JOIN employee_hr emph;

SELECT emp.name, emph.sin_number
FROM employee emp
JOIN employee_hr emph;

SELECT emp.name, emph.sin_number
FROM employee emp
JOIN employee_hr emph on 1=1;

--unequal JOIN
SELECT emp.name, emph.sin_number
FROM employee emp
CROSS JOIN employee_hr emph WHERE emp.name <> emph.name;

--An example MAP JOIN enabled by query hint
SELECT /*+ MAPJOIN(employee) */ emp.name, emph.sin_number
FROM employee emp
CROSS JOIN employee_hr emph WHERE emp.name <> emph.name;

--BUCKET Map Join settings
SET hive.optimize.bucketmapjoin = true; 
SET hive.optimize.bucketmapjoin.sortedmerge = true;
SET hive.input.format=org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat; 

--LEFT SEMI JOIN
SELECT a.name
FROM employee a
WHERE EXISTS
(SELECT * FROM employee_id b
WHERE a.name = b.name);

SELECT a.name
FROM employee a
LEFT SEMI JOIN employee_id b
ON a.name = b.name;

--UNION ALL including duplications
SELECT a.name as nm
FROM employee a
UNION ALL
SELECT b.name as nm
FROM employee_hr b;

--UNION
SELECT a.name as nm
FROM employee a
UNION
SELECT b.name as nm
FROM employee_hr b;

--Order with UNION
SELECT a.name as nm FROM employee a
UNION ALL
SELECT b.name as nm FROM employee_hr b
ORDER BY nm;

--Table employee implements INTERCEPT employee_hr
SELECT a.name 
FROM employee a
JOIN employee_hr b
ON a.name = b.name;

--Table employee implements MINUS employee_hr
SELECT a.name 
FROM employee a
LEFT JOIN employee_hr b
ON a.name = b.name
WHERE b.name IS NULL;
