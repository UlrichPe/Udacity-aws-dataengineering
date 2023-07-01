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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1688228685898 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedilakehouse/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1688228685898",
)

# Script generated for node customer_trusted
customer_trusted_node1688229983015 = glueContext.create_dynamic_frame.from_catalog(
    database="stedilakehouse",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node1688229983015",
)

# Script generated for node Join
Join_node1688228690260 = Join.apply(
    frame1=AccelerometerLanding_node1688228685898,
    frame2=customer_trusted_node1688229983015,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1688228690260",
)

# Script generated for node Drop Fields
DropFields_node1688228696517 = DropFields.apply(
    frame=Join_node1688228690260,
    paths=[
        "sharewithfriendsasofdate",
        "sharewithpublicasofdate",
        "sharewithresearchasofdate",
        "lastupdatedate",
        "registrationdate",
        "serialnumber",
        "birthday",
        "phone",
        "email",
        "customername",
    ],
    transformation_ctx="DropFields_node1688228696517",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1688228696517,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedilakehouse/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="accelerometer_trusted_node3",
)

job.commit()
