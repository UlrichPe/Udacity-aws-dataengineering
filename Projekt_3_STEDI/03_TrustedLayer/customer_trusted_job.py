import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node customer_landing
customer_landing_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedilakehouse/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="customer_landing_node1",
)

# Script generated for node Filter
Filter_node1688227620269 = Filter.apply(
    frame=customer_landing_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="Filter_node1688227620269",
)

# Script generated for node customer_trusted
customer_trusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Filter_node1688227620269,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedilakehouse/customer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="customer_trusted_node3",
)

job.commit()
