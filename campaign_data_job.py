import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME", "bucket_name"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node member_data_table
member_data_table_node1640829180942 = glueContext.create_dynamic_frame.from_catalog(
    database="discover",
    table_name="member",
    transformation_ctx="member_data_table_node1640829180942",
)

# Script generated for node campaign_data_table
campaign_data_table_node1641332227691 = glueContext.create_dynamic_frame.from_catalog(
    database="discover",
    table_name="campaign",
    transformation_ctx="campaign_data_table_node1641332227691",
)

# Script generated for node transform_member_data
transform_member_data_node1640829253031 = ApplyMapping.apply(
    frame=member_data_table_node1640829180942,
    mappings=[
        ("member_id", "string", "member_member_id", "string"),
        ("ssn", "string", "member_ssn", "string"),
        ("first_name", "string", "member_first_name", "string"),
        ("last_name", "string", "member_last_name", "string"),
        ("gender", "string", "member_gender", "string"),
        ("race", "string", "member_race", "string"),
        ("language", "string", "member_language", "string"),
        ("phone", "string", "member_phone", "string"),
        ("email", "string", "member_email", "string"),
        ("address_line_1", "string", "member_address_line_1", "string"),
        ("address_line_2", "long", "member_address_line_2", "long"),
        ("city", "string", "member_city", "string"),
        ("state", "string", "member_state", "string"),
        ("zip", "long", "member_zip", "long"),
    ],
    transformation_ctx="transform_member_data_node1640829253031",
)

# Script generated for node transform_campaign_data
transform_campaign_data_node1641332423680 = ApplyMapping.apply(
    frame=campaign_data_table_node1641332227691,
    mappings=[
        ("address_line_1", "string", "address_line_1", "string"),
        ("address_line_2", "int", "address_line_2", "string"),
        ("campaign_date", "string", "campaign_date", "string"),
        ("city", "string", "city", "string"),
        ("email", "string", "email", "string"),
        ("existing_member", "boolean", "existing_member", "boolean"),
        ("first_name", "string", "first_name", "string"),
        ("gender", "string", "gender", "string"),
        ("last_name", "string", "last_name", "string"),
        ("member_id", "string", "member_id", "string"),
        ("phone", "string", "phone", "string"),
        ("state", "string", "state", "string"),
        ("zip", "int", "zip", "int"),
    ],
    transformation_ctx="transform_campaign_data_node1641332423680",
)

# Script generated for node LeftJoin
transform_campaign_data_node1641332423680DF = (
    transform_campaign_data_node1641332423680.toDF()
)
transform_member_data_node1640829253031DF = (
    transform_member_data_node1640829253031.toDF()
)
LeftJoin_node1640829204661 = DynamicFrame.fromDF(
    transform_campaign_data_node1641332423680DF.join(
        transform_member_data_node1640829253031DF,
        (
            transform_campaign_data_node1641332423680DF["member_id"]
            == transform_member_data_node1640829253031DF["member_member_id"]
        ),
        "left",
    ),
    glueContext,
    "LeftJoin_node1640829204661",
)

# Script generated for node sql
SqlQuery0 = """
select 
coalesce(member_first_name, first_name) as first_name,
coalesce(member_last_name, last_name) as last_name,
member_ssn as ssn,
member_race as race,
member_language as language,
case 
when trim(coalesce(member_gender, gender)) = "F" OR trim(coalesce(member_gender, gender)) = "Female" then "Female"
when trim(coalesce(member_gender, gender)) = "M" OR trim(coalesce(member_gender, gender)) = "Male" then "Male"
else null
end
as gender,
coalesce(member_phone, phone) as phone,
coalesce(member_email, email) as email,
coalesce(member_address_line_1, address_line_1) as address_line_1,
coalesce(member_address_line_2, address_line_2) as address_line_2,
coalesce(member_city, city) as city,
coalesce(member_state, state) as state,
coalesce(member_zip, zip) as zip,
member_member_id as member_id,
campaign_date
from ds
"""
sql_node1640829404186 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"ds": LeftJoin_node1640829204661},
    transformation_ctx="sql_node1640829404186",
)

# Script generated for node target_S3_location
target_S3_location_node1640829510544 = glueContext.getSink(
    path=args["bucket_name"].join(("s3://", "/output/campaign/")),
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="target_S3_location_node1640829510544",
)
target_S3_location_node1640829510544.setCatalogInfo(
    catalogDatabase="discover", catalogTableName="transformed_campaign_data"
)
target_S3_location_node1640829510544.setFormat("glueparquet")
target_S3_location_node1640829510544.writeFrame(sql_node1640829404186)
job.commit()
