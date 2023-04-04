import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing
CustomerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://hungnq-lake-house/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1",
)

# Script generated for node Apply Filter
SqlQuery0 = """
select * from myDataSource where sharewithresearchasofdate is null
"""
ApplyFilter_node2 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": CustomerLanding_node1},
    transformation_ctx="ApplyFilter_node2",
)

# Script generated for node Trusted Customer Zone
TrustedCustomerZone_node3 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyFilter_node2,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://hungnq-lake-house/customer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="TrustedCustomerZone_node3",
)

job.commit()
