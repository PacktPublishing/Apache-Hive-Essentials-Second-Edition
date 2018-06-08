--Apache Hive Essentials
--Chapter 8 Code - Extensibility Considerations
   
--UDF deployment 
CREATE TEMPORARY FUNCTION tmptoUpper 
as 'com.packtpub.hive.essentials.hiveudf.toupper';
USING JAR 'hdfs:///app/hive/function/hiveudf-1.0.jar';

CREATE FUNCTION toUpper
as 'hive.essentials.hiveudf.ToUpper' 
USING JAR 'hdfs:///app/hive/function/hiveudf-1.0.jar';

SHOW FUNCTIONS ToUpper;
DESCRIBE FUNCTION ToUpper;
DESCRIBE FUNCTION EXTENDED ToUpper;

RELOAD FUNCTION;

SELECT name, toUpper(name) as cap_name, tmptoUpper(name) as cname FROM employee;

DROP TEMPORARY FUNCTION IF EXISTS tmptoUpper;
DROP FUNCTION IF EXISTS toUpper;
 
--Streaming, call the script in Hive CLI from HQL.
ADD FILE /tmp/upper.py;
SELECT TRANSFORM (name,work_place[0]) 
USING 'python upper.py' AS (CAP_NAME,CAP_PLACE) 
FROM employee;

--LazySimpleSerDe
CREATE TABLE test_serde_lz
STORED as TEXTFILE as
SELECT name from employee;

--ColumnarSerDe
CREATE TABLE test_serde_rc
STORED as RCFile as
SELECT name from employee;

--ColumnarSerDe
CREATE TABLE test_serde_orc
STORED as ORC as
SELECT name from employee;

--RegexSerDe-Parse , seperate fields
CREATE TABLE test_serde_rex(
name string,
gender string,
age string
)
ROW FORMAT SERDE
'org.apache.hadoop.hive.contrib.serde2.RegexSerDe'
WITH SERDEPROPERTIES(
'input.regex' = '([^,]*),([^,]*),([^,]*)',
'output.format.string' = '%1$s %2$s %3$s'
)
STORED as TEXTFILE;

--HBaseSerDe. Make sure you have HBase installed before running query below.
CREATE TABLE test_serde_hb(
id string,
name string,
gender string,
age string
)
ROW FORMAT SERDE
'org.apache.hadoop.hive.hbase.HBaseSerDe'
STORED BY
'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
"hbase.columns.mapping"=
":key,info:name,info:gender,info:age"
)
TBLPROPERTIES("hbase.table.name" = "test_serde");

--AvroSerDe 3 ways
CREATE TABLE test_serde_avro( -- Specify schema directly 
name string,
gender string,
age string
)
STORED as AVRO;

CREATE TABLE test_serde_avro2 -- Specify schema from properties 
STORED as AVRO
TBLPROPERTIES (
  'avro.schema.literal'='{
   "type":"record",
   "name":"user",
   "fields":[ 
   {"name":"name", "type":"string"}, 
   {"name":"gender", "type":"string", "aliases":["gender"]},
   {"name":"age", "type":"string", "default":"null"}
   ]
  }'
);

-- Using schema file below is more flexiable
CREATE TABLE test_serde_avro3 -- Specify schema from schema file 
STORED as AVRO
TBLPROPERTIES (
'avro.schema.url'='/tmp/schema/test_avro_schema.avsc'
);

--ParquetHiveSerDe
CREATE TABLE test_serde_parquet
STORED as PARQUET as
SELECT name from employee;

--OpenCSVSerDe
CREATE TABLE test_serde_csv(
name string,
gender string,
age string
)
ROW FORMAT SERDE
'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = "\t",
  "quoteChar" = "'",
  "escapeChar" = "\\"
) 
STORED as TEXTFILE;

--JSONSerDe
CREATE TABLE test_serde_js(
name string,
gender string,
age string
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED as TEXTFILE;
