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

# Script generated for node customer curated
customercurated_node1674912972069 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://20230128-data-lake/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="customercurated_node1674912972069",
)

# Script generated for node step-trainer trusted
steptrainertrusted_node1674912653880 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://20230128-data-lake/step-trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="steptrainertrusted_node1674912653880",
)

# Script generated for node accelerometer trusted
accelerometertrusted_node1674910234864 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://20230128-data-lake/accelerometer/trusted/"]},
    transformation_ctx="accelerometertrusted_node1674910234864",
)

# Script generated for node acc-cust
acccust_node2 = Join.apply(
    frame1=accelerometertrusted_node1674910234864,
    frame2=customercurated_node1674912972069,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="acccust_node2",
)

# Script generated for node Renamed keys for acc-step
Renamedkeysforaccstep_node1674913227937 = ApplyMapping.apply(
    frame=steptrainertrusted_node1674912653880,
    mappings=[
        ("serialNumber", "string", "`(right) serialNumber`", "string"),
        ("sensorReadingTime", "long", "`(right) sensorReadingTime`", "long"),
        ("distanceFromObject", "int", "`(right) distanceFromObject`", "int"),
    ],
    transformation_ctx="Renamedkeysforaccstep_node1674913227937",
)

# Script generated for node acc-step
accstep_node1674913152786 = Join.apply(
    frame1=acccust_node2,
    frame2=Renamedkeysforaccstep_node1674913227937,
    keys1=["serialNumber", "timeStamp"],
    keys2=["`(right) serialNumber`", "`(right) sensorReadingTime`"],
    transformation_ctx="accstep_node1674913152786",
)

# Script generated for node Drop Fields
DropFields_node1674912820484 = DropFields.apply(
    frame=accstep_node1674913152786,
    paths=["`(right) serialNumber`", "`(right) sensorReadingTime`"],
    transformation_ctx="DropFields_node1674912820484",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1674912820484,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://20230128-data-lake/step-trainer/machine_learning/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
