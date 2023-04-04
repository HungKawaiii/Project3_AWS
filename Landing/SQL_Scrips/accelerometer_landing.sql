CREATE EXTERNAL TABLE accelerometer_landing(
`timeStamp` bigint,
user string,
x float,
y float,
z float)
  
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
   'serialization.format' = '1'
)
LOCATION 's3://hungnq-lake-house/accelerometer/landing/'
TBLPROPERTIES ('has_encrypted_data'='false')