CREATE EXTERNAL TABLE customer_trusted(
serialnumber string,
sharewithpublicasofdate bigint,
birthday string,
registrationdate bigint,
sharewithresearchasofdate bigint,
customername string,
email string,
lastupdatedate bigint,
phone string,
sharewithfriendsasofdate bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
'serialization.format' = '1')
LOCATION 's3://hungnq-lake-house/customer/trusted/'
TBLPROPERTIES ('has_encrypted_data'='false')