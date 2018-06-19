--Apache Hive Essentials
--Chapter 6 Code - Hive Data Aggregation and Sampling

--Aggregation without GROUP BY columns
SELECT count(*) as rowcnt1, count(1) AS rowcnt2 FROM employee;

--Aggregation with GROUP BY columns
SELECT gender_age.gender, count(*) AS row_cnt FROM employee
GROUP BY gender_age.gender;

--The column age is not in the group by columns, 
--FAILED: SemanticException [Error 10002]: Line 1:15 Invalid column reference 'age'
--SELECT gender_age.age, gender_age.gender, count(*) AS row_cnt
--FROM employee GROUP BY gender_age.gender;

--Multiple aggregate functions are called in the same SELECT
SELECT gender_age.gender, AVG(gender_age.age) AS avg_age,
count(*) AS row_cnt FROM employee GROUP BY gender_age.gender;

--Aggregate functions are used with CASE WHEN 
SELECT sum(CASE WHEN gender_age.gender = 'Male' THEN gender_age.age
ELSE 0 END)/count(CASE WHEN gender_age.gender = 'Male' THEN 1
ELSE NULL END) AS male_age_avg FROM employee;

--Aggregate functions are used with COALESCE and IF 
SELECT
sum(coalesce(gender_age.age,0)) AS age_sum,
sum(if(gender_age.gender = 'Female',gender_age.age,0))
AS female_age_sum FROM employee;

--Nested aggregate functions are not allowed
--FAILED: SemanticException [Error 10128]: Line 1:11 Not yet supported place for UDAF 'count'
--SELECT avg(count(*)) AS row_cnt FROM employee; 

--Aggregate functions cannot apply to null
--SELECT sum(null), avg(null); 

--Aggregation across columns with NULL value.
SELECT max(null), min(null), count(null);
---Prepare a table for testing
CREATE TABLE t (val1 int, val2 int);
INSERT INTO TABLE t VALUES (1, 2),(null,2),(2,3);
----Check the table rows 
SELECT * FROM t;
----The 2nd row (NULL, 2) are ignored when doing sum(val1+val2)
SELECT sum(val1), sum(val1+val2) FROM t;                   
SELECT sum(coalesce(val1,0)), sum(coalesce(val1,0)+val2) FROM t;

--Aggregate functions can be also used with DISTINCT keyword to do aggregation on unique values.
SELECT count(distinct gender_age.gender) AS gender_uni_cnt, count(distinct name) AS name_uni_cnt FROM employee;

--Use max/min struct
SELECT gender_age.gender,
max(struct(gender_age.age, name)).col1 as age,
max(struct(gender_age.age, name)).col2 as name
FROM employee
GROUP BY gender_age.gender;

--Trigger single reducer during the whole processing
SELECT count(distinct gender_age.gender) AS gender_uni_cnt FROM employee;

--Use subquery to select unique value before aggregations for better performance
SELECT count(*) AS gender_uni_cnt FROM (SELECT distinct gender_age.gender FROM employee) a;

--Grouping Set
SELECT 
name, 
start_date,
count(sin_number) as sin_cnt 
FROM employee_hr
GROUP BY name, start_date 
GROUPING SETS((name, start_date));
--||-- equals to
SELECT 
name, 
start_date, 
count(sin_number) AS sin_cnt 
FROM employee_hr
GROUP BY name, start_date;

SELECT 
name, start_date, count(sin_number) as sin_cnt 
FROM employee_hr
GROUP BY name, start_date 
GROUPING SETS(name, start_date);
--||-- equals to
SELECT 
name, null as start_date, count(sin_number) as sin_cnt 
FROM employee_hr
GROUP BY name
UNION ALL
SELECT 
null as name, start_date, count(sin_number) as sin_cnt 
FROM employee_hr
GROUP BY start_date;

SELECT 
name, start_date, count(sin_number) as sin_cnt 
FROM employee_hr
GROUP BY name, start_date 
GROUPING SETS((name, start_date), name);
--||-- equals to
SELECT 
name, start_date, count(sin_number) as sin_cnt 
FROM employee_hr
GROUP BY name, start_date
UNION ALL
SELECT 
name, null as start_date, count(sin_number) as sin_cnt 
FROM employee_hr
GROUP BY name;

SELECT 
name, start_date, count(sin_number) as sin_cnt 
FROM employee_hr
GROUP BY name, start_date 
GROUPING SETS((name, start_date), name, start_date, ());
--||-- equals to
SELECT 
name, start_date, count(sin_number) AS sin_cnt 
FROM employee_hr
GROUP BY name, start_date
UNION ALL
SELECT 
name, null as start_date, count(sin_number) AS sin_cnt 
FROM employee_hr
GROUP BY name
UNION ALL
SELECT 
null as name, start_date, count(sin_number) AS sin_cnt 
FROM employee_hr
GROUP BY start_date
UNION ALL
SELECT 
null as name, null as start_date, count(sin_number) AS sin_cnt 
FROM employee_hr

--GROUPING__ID and grouping
SELECT 
name, start_date, count(employee_id) as emp_id_cnt,
GROUPING__ID,
grouping(name) as gp_name, grouping(start_date) as gp_sd
FROM employee_hr 
GROUP BY name, start_date 
WITH CUBE ORDER BY name, start_date;

--Aggregation condition – HAVING
SELECT gender_age.age FROM employee GROUP BY gender_age.age HAVING count(*)=1;
SELECT gender_age.age, count(*) as cnt FROM employee GROUP BY gender_age.age HAVING cnt=1;

--If we do not use HAVING, we can use subquery as follows. 
SELECT a.age
FROM
(SELECT count(*) as cnt, gender_age.age
FROM employee GROUP BY gender_age.age
) a WHERE a.cnt<=1;

--Prepare table and data for demonstration
CREATE TABLE IF NOT EXISTS employee_contract
(
name string,
dept_num int,
employee_id int,
salary int,
type string,
start_date date
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

LOAD DATA INPATH
'/tmp/hivedemo/data/employee_contract.txt' 
OVERWRITE INTO TABLE employee_contract;

--window aggregate functions
SELECT 
name, 
dept_num as deptno, 
salary,
count(*) OVER (PARTITION BY dept_num) as cnt,
count(distinct dept_num) OVER (PARTITION BY dept_num) as dcnt,
sum(salary) OVER(PARTITION BY dept_num ORDER BY dept_num) as sum1,
sum(salary) OVER(ORDER BY dept_num) as sum2,
sum(salary) OVER(ORDER BY dept_num, name) as sum3
FROM employee_contract
ORDER BY deptno, name;

--window sorting functions
SELECT 
name, 
dept_num as deptno, 
salary,
row_number() OVER () as rnum,
rank() OVER (PARTITION BY dept_num ORDER BY salary) as rk, 
dense_rank() OVER (PARTITION BY dept_num ORDER BY salary) as drk,
percent_rank() OVER(PARTITION BY dept_num ORDER BY salary) as prk,
ntile(4) OVER(PARTITION BY dept_num ORDER BY salary) as ntile
FROM employee_contract
ORDER BY deptno, name;

--aggregate in over clause
SELECT
dept_num,
rank() OVER (PARTITION BY dept_num ORDER BY sum(salary)) as rk
FROM employee_contract
GROUP BY dept_num;

--window analytics function
SELECT 
name,
dept_num as deptno,
salary,
round(cume_dist() OVER (PARTITION BY dept_num ORDER BY salary), 2) as cume,
lead(salary, 2) OVER (PARTITION BY dept_num ORDER BY salary) as lead,
lag(salary, 2, 0) OVER (PARTITION BY dept_num ORDER BY salary) as lag,
first_value(salary) OVER (PARTITION BY dept_num ORDER BY salary) as fval,
last_value(salary) OVER (PARTITION BY dept_num ORDER BY salary) as lvalue,
last_value(salary) OVER (PARTITION BY dept_num ORDER BY salary RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lvalue2
FROM employee_contract 
ORDER BY deptno, salary;

--window expression preceding and following
SELECT 
name, dept_num as dno, salary AS sal,
max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) win1,
max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING) win2,
max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING) win3,
max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) win4,
max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) win5,
max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS 2 PRECEDING) win6,
max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS UNBOUNDED PRECEDING) win7
FROM employee_contract
ORDER BY dno, name;

--window expression current_row
SELECT 
name, dept_num as dno, salary AS sal,
max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN CURRENT ROW AND CURRENT ROW) win8,
max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) win9,
max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) win10,
max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) win11,
max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) win12,
max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) win13,
max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) win14
FROM employee_contract
ORDER BY dno, name;

--window reference
SELECT name, dept_num, salary,
MAX(salary) OVER w1 AS win1,
MAX(salary) OVER w2 AS win2,
MAX(salary) OVER w3 AS win3
FROM employee_contract
WINDOW
w1 as (PARTITION BY dept_num ORDER BY name ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),
w2 as w3,
w3 as (PARTITION BY dept_num ORDER BY name ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING)
;

--window with range type
SELECT 
dept_num, start_date, name, salary, 
max(salary) OVER (PARTITION BY dept_num ORDER BY salary
RANGE BETWEEN 500 PRECEDING AND 1000 FOLLOWING) win1,
max(salary) OVER (PARTITION BY dept_num ORDER BY salary
RANGE BETWEEN 500 PRECEDING AND CURRENT ROW) win2
FROM employee_contract
order by dept_num, start_date;

--random sampling
SELECT name FROM employee_hr DISTRIBUTE BY rand() SORT BY rand() LIMIT 2;

--Bucket table sampling example
--based on whole row
SELECT name FROM employee_trans TABLESAMPLE(BUCKET 1 OUT OF 2 ON rand()) a;
--based on bucket column
SELECT name FROM employee_trans TABLESAMPLE(BUCKET 1 OUT OF 2 ON emp_id) a;

--Block sampling - Sample by rows
SELECT name FROM employee TABLESAMPLE(1 ROWS) a;

--Sample by percentage of data size
SELECT name FROM employee TABLESAMPLE(50 PERCENT) a;

--Sample by data size
SELECT name FROM employee TABLESAMPLE(3b) a;   
