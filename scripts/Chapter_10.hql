--Apache Hive Essentials 
--Chapter 10 Code - Work with Other Tools

--Hive HBase integration
CREATE EXTERNAL TABLE hbase_table_sample(
id int,
value1 string,
value2 string,
map_value map<string, string>
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf1:val,cf2:val,cf3:")
TBLPROPERTIES ("hbase.table.name" = "table_name_in_hbase");

--Hive Mongo integration
ADD JAR mongo-hadoop-core-2.0.2.jar;
CREATE EXTERNAL TABLE mongodb_table_sample(
id int,
value1 string,
value2 string
)
STORED BY 'com.mongodb.hadoop.hive.MongoStorageHandler'
WITH SERDEPROPERTIES (
'mongo.columns.mapping'='{"id":"_id","value1":"value1","value2":"value2"}')
TBLPROPERTIES(
'mongo.uri'='mongodb://localhost:27017/default.mongo_sample'
);
