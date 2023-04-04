import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Step Trainer Landing Zone
StepTrainerLandingZone_node1680595213824 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://hungnq-lake-house/step_trainer/landing/"],
            "recurse": True,
        },
        transformation_ctx="StepTrainerLandingZone_node1680595213824",
    )
)

# Script generated for node Customer Curated Zone
CustomerCuratedZone_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://hungnq-lake-house/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCuratedZone_node1",
)

# Script generated for node Rename Field Serial Number
RenameFieldSerialNumber_node1680602081704 = RenameField.apply(
    frame=StepTrainerLandingZone_node1680595213824,
    old_name="serialNumber",
    new_name="serial_number",
    transformation_ctx="RenameFieldSerialNumber_node1680602081704",
)

# Script generated for node Drop Duplicates Customer
DropDuplicatesCustomer_node1680600772451 = DynamicFrame.fromDF(
    CustomerCuratedZone_node1.toDF().dropDuplicates(["serialNumber", "email"]),
    glueContext,
    "DropDuplicatesCustomer_node1680600772451",
)

# Script generated for node Join
Join_node2 = Join.apply(
    frame1=DropDuplicatesCustomer_node1680600772451,
    frame2=RenameFieldSerialNumber_node1680602081704,
    keys1=["serialNumber"],
    keys2=["serial_number"],
    transformation_ctx="Join_node2",
)

# Script generated for node Drop Fields
DropFields_node1680595472734 = DropFields.apply(
    frame=Join_node2,
    paths=[
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
        "customerName",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "shareWithPublicAsOfDate",
        "birthDay",
        "email",
        "serialNumber",
    ],
    transformation_ctx="DropFields_node1680595472734",
)

# Script generated for node Step Trainer Trusted Zone
StepTrainerTrustedZone_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1680595472734,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://hungnq-lake-house/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrustedZone_node3",
)

job.commit()
