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


raw_afl_data = glueContext.create_dynamic_frame.from_catalog(
    database="afl-database",
    table_name="afl_data_raw",
    transformation_ctx="raw_afl_data",
)


curated_afl_data = glueContext.write_dynamic_frame.from_options(
    frame=raw_afl_data,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": "s3://afl-data-curated", "partitionKeys": ["year"]},
    format_options={"compression": "snappy"},
    transformation_ctx="curated_afl_data",
)

job.commit()
