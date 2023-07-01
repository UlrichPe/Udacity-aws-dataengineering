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

# Script generated for node customer_trusted
customer_trusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedilakehouse/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted_node1",
)

# Script generated for node steptrainer_landing
steptrainer_landing_node1688232305358 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedilakehouse/steptrainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="steptrainer_landing_node1688232305358",
)

# Script generated for node Join
Join_node1688232355499 = Join.apply(
    frame1=steptrainer_landing_node1688232305358,
    frame2=customer_trusted_node1,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1688232355499",
)

# Script generated for node customer_curated
customer_curated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1688232355499,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedilakehouse/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="customer_curated_node3",
)

job.commit()
