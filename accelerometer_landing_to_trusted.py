import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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

# Script generated for node Drop Fields
DropFields_node1758645330925 = DropFields.apply(frame=Join_node1758645167761, paths=["email", "phone", "customername", "birthdate", "serialnumber", "registrationdate", "lastupdatedate", "sharewithresearchasofdate", "sharewithpublicasofdate"], transformation_ctx="DropFields_node1758645330925")

# Script generated for node Accelerometer Trusted
EvaluateDataQuality().process_rows(frame=DropFields_node1758645330925, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758643616244", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrusted_node1758645412285 = glueContext.getSink(path="s3://stedi-lake-house021511869602/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1758645412285")
AccelerometerTrusted_node1758645412285.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1758645412285.setFormat("json")
AccelerometerTrusted_node1758645412285.writeFrame(DropFields_node1758645330925)
job.commit()