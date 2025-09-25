import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Customer Trusted
CustomerTrusted_node1758645076627 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1758645076627")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1758645078723 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1758645078723")

# Script generated for node Join
Join_node1758645167761 = Join.apply(frame1=CustomerTrusted_node1758645076627, frame2=AccelerometerLanding_node1758645078723, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1758645167761")

# Script generated for node Drop Fields and Duplicates
SqlQuery2763 = '''
select distinct customername , email , phone , birthday , serialnumber , registrationdate , lastupdatedate , sharewithresearchasofdate , sharewithpublicasofdate  from myDataSource

'''
DropFieldsandDuplicates_node1758647770533 = sparkSqlQuery(glueContext, query = SqlQuery2763, mapping = {"myDataSource":Join_node1758645167761}, transformation_ctx = "DropFieldsandDuplicates_node1758647770533")

# Script generated for node Customer Curated
EvaluateDataQuality().process_rows(frame=DropFieldsandDuplicates_node1758647770533, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758643616244", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerCurated_node1758645412285 = glueContext.getSink(path="s3://stedi-lake-house021511869602/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1758645412285")
CustomerCurated_node1758645412285.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
CustomerCurated_node1758645412285.setFormat("json")
CustomerCurated_node1758645412285.writeFrame(DropFieldsandDuplicates_node1758647770533)
job.commit()