CREATE EXTERNAL TABLE IF NOT EXISTS `stedilakehouse`.`machine_learning_curated` (
  `timestamp` bigint,
  `user` string,
  `x` float,
  `y` float,
  `z` float,
  `sensorreadingtime` bigint,
  `serialnumber` string,
  `distancefromobject` int
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://stedilakehouse/steptrainer/curated/'
TBLPROPERTIES ('classification' = 'json');