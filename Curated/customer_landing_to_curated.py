import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1680595213824 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://hungnq-lake-house/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrustedZone_node1680595213824",
)

# Script generated for node Accelerometer Trusted Zone
AccelerometerTrustedZone_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://hungnq-lake-house/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrustedZone_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = Join.apply(
    frame1=AccelerometerTrustedZone_node1,
    frame2=CustomerTrustedZone_node1680595213824,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Drop Fields
DropFields_node1680595472734 = DropFields.apply(
    frame=ApplyMapping_node2,
    paths=["x", "y", "user", "timeStamp", "z"],
    transformation_ctx="DropFields_node1680595472734",
)

# Script generated for node Customer Curated Zone
CustomerCuratedZone_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1680595472734,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://hungnq-lake-house/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCuratedZone_node3",
)

job.commit()
