
--drop database if exists
drop database if exists s3_to_snowflake;

--Database Creation 
create database if not exists s3_to_snowflake;

--Use the database
use s3_to_snowflake;

--Table Creation
create or replace table s3_to_snowflake.PUBLIC.Iris_dataset1 (Id number(10,0),sepal_length number(10,4),sepal_width number(10,4),petal_length number(10,4)  ,petal_width number(10,4),class varchar(200));
                                  


--create the file format
CREATE OR REPLACE FILE FORMAT sf_tut_parquet_format
  TYPE = parquet;

--create the external stage
create or replace stage s3_to_snowflake.PUBLIC.Snow_stage url="s3://s3-etl-storage/publish/" 
credentials=(aws_key_id=''
aws_secret_key='')
file_format = sf_tut_parquet_format;

list @Snow_stage;



--Create the Pipe
create or replace pipe s3_to_snowflake.PUBLIC.for_iris_one
auto_ingest=true as 
copy into s3_to_snowflake.PUBLIC.Iris_dataset1
from 
(select $1:"Id"::number,
$1:SEPAL_LENGTH::VARCHAR,
$1:SEPAL_WIDTH::VARCHAR,
$1:PETAL_LENGTH::VARCHAR,
$1:PETAL_WIDTH::VARCHAR,
$1:CLASS_NAME::VARCHAR
from @s3_to_snowflake.PUBLIC.Snow_stage);


show pipes;

select * from s3_to_snowflake.PUBLIC.Iris_dataset1;