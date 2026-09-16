import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame


args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# Read transformed Parquet data from S3
input_path = (
    "s3://aws-customer-pipeline-mohanraj-2026/"
    "transformed/customer_orders/"
)

customer_order_df = spark.read.parquet(input_path)

print("Input record count:", customer_order_df.count())

customer_order_df.printSchema()

customer_order_df.show(5, truncate=False)


# Convert DataFrame to DynamicFrame
customer_order_dyf = DynamicFrame.fromDF(
    customer_order_df,
    glueContext,
    "customer_order_dyf"
)


# MongoDB Atlas destination
connection_options = {
    "connectionName": "mongodb_atlas_connection",
    "database": "customer_order_migration",
    "collection": "customers",
    "ssl": "true",
    "ssl.domain_match": "false"
}


# Write data to MongoDB Atlas
glueContext.write_dynamic_frame.from_options(
    frame=customer_order_dyf,
    connection_type="mongodb",
    connection_options=connection_options
)


job.commit()