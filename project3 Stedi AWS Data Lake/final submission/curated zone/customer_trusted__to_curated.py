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

# Script generated for node customer trusted
customertrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://20230128-data-lake/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customertrusted_node1",
)

# Script generated for node accelerometer trusted
accelerometertrusted_node1674893394412 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://20230128-data-lake/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometertrusted_node1674893394412",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1674903452842 = DynamicFrame.fromDF(
    accelerometertrusted_node1674893394412.toDF().dropDuplicates(["user"]),
    glueContext,
    "DropDuplicates_node1674903452842",
)

# Script generated for node Join
Join_node2 = Join.apply(
    frame1=customertrusted_node1,
    frame2=DropDuplicates_node1674903452842,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node2",
)

# Script generated for node Drop Fields
DropFields_node1674893424144 = DropFields.apply(
    frame=Join_node2,
    paths=["z", "timeStamp", "user", "y", "x"],
    transformation_ctx="DropFields_node1674893424144",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1674893424144,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://20230128-data-lake/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
