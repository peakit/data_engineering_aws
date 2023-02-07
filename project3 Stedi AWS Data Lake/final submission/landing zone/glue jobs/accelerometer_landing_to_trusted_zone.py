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

# Script generated for node customer trusted
customertrusted_node1674892626021 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://20230128-data-lake/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customertrusted_node1674892626021",
)

# Script generated for node accelerometer landing
accelerometerlanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://20230128-data-lake/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="accelerometerlanding_node1",
)

# Script generated for node acc_landing JOIN cust_trusted
acc_landingJOINcust_trusted_node2 = Join.apply(
    frame1=customertrusted_node1674892626021,
    frame2=accelerometerlanding_node1,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="acc_landingJOINcust_trusted_node2",
)

# Script generated for node Drop Fields
DropFields_node1674892731779 = DropFields.apply(
    frame=acc_landingJOINcust_trusted_node2,
    paths=[
        "serialNumber",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithPublicAsOfDate",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1674892731779",
)

# Script generated for node accelerometer trusted
accelerometertrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1674892731779,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://20230128-data-lake/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="accelerometertrusted_node3",
)

job.commit()
