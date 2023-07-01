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

# Script generated for node steptrainer_trusted
steptrainer_trusted_node1688236139103 = glueContext.create_dynamic_frame.from_catalog(
    database="stedilakehouse",
    table_name="steptrainer_trusted",
    transformation_ctx="steptrainer_trusted_node1688236139103",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1688233607331 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedilakehouse/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_trusted_node1688233607331",
)

# Script generated for node Join
Join_node1688233608418 = Join.apply(
    frame1=accelerometer_trusted_node1688233607331,
    frame2=steptrainer_trusted_node1688236139103,
    keys1=["timeStamp"],
    keys2=["sensorreadingtime"],
    transformation_ctx="Join_node1688233608418",
)

# Script generated for node machine_learning_curated
machine_learning_curated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1688233608418,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedilakehouse/steptrainer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="machine_learning_curated_node3",
)

job.commit()
