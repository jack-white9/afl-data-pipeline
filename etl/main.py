import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


raw_afl_data_node = glueContext.create_dynamic_frame.from_catalog(
    database="afl-database",
    table_name="afl_data_raw",
    transformation_ctx="raw_afl_data",
)


transform_afl_data_query = """
select 
    date,
    abehinds as away_behinds,
    agoals as away_goals,
    ateam as away_team,
    hbehinds as home_behinds,
    hgoals as home_goals,
    hteam as home_team,
    is_final,
    is_grand_final,
    round,
    roundname as round_name,
    venue,
    winner,
    year
from raw_afl_data;
"""


transform_afl_data_node = sparkSqlQuery(
    glueContext,
    query=transform_afl_data_query,
    mapping={"raw_afl_data": raw_afl_data_node},
    transformation_ctx="TransformAFLdata_node1693133314528",
)


curated_afl_data_node = glueContext.write_dynamic_frame.from_options(
    frame=transform_afl_data_node,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": "s3://afl-data-curated", "partitionKeys": ["year"]},
    format_options={"compression": "snappy"},
    transformation_ctx="curated_afl_data_node",
)


job.commit()
