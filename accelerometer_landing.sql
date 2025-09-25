CREATE EXTERNAL TABLE `accelerometer_landing`(
  `user` string COMMENT 'from deserializer', 
  `x` string COMMENT 'from deserializer', 
  `y` string COMMENT 'from deserializer', 
  `z` string COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES ( 
  'case.insensitive'='TRUE', 
  'dots.in.keys'='FALSE', 
  'ignore.malformed.json'='FALSE', 
  'mapping'='TRUE') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://stedi-lake-house021511869602/accelerometer/landing'
TBLPROPERTIES (
  'classification'='json', 
  'transient_lastDdlTime'='1758635811')
