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

# Script generated for node customer_curated
customer_curated_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedilakehouse/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="customer_curated_node1",
)

# Script generated for node step_trainer_landing
step_trainer_landing_node1688232849749 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedilakehouse/steptrainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_landing_node1688232849749",
)

# Script generated for node Join
Join_node1688233059975 = Join.apply(
    frame1=customer_curated_node1,
    frame2=step_trainer_landing_node1688232849749,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1688233059975",
)

# Script generated for node Drop Fields
DropFields_node1688233207157 = DropFields.apply(
    frame=Join_node1688233059975,
    paths=[
        "serialNumber",
        "birthDay",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "sensorReadingTime",
        "email",
        "lastUpdateDate",
        "distanceFromObject",
        "phone",
        "shareWithPublicAsOfDate",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1688233207157",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1688233207157,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedilakehouse/steptrainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="step_trainer_trusted_node3",
)

job.commit()
