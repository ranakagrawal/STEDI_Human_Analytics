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

# Script generated for node Customer_Curated
Customer_Curated_node1758777152909 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="Customer_Curated_node1758777152909")

# Script generated for node Step_Trainer_Landing
Step_Trainer_Landing_node1758777154510 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house021511869602/step_trainer/landing/"], "recurse": True}, transformation_ctx="Step_Trainer_Landing_node1758777154510")

# Script generated for node SQL Query
SqlQuery3722 = '''
select distinct stepTrainer_landing.*
FROM stepTrainer_landing
INNER join customer_curated
ON customer_curated.serialNumber = stepTrainer_landing.serialNumber

'''
SQLQuery_node1758777772901 = sparkSqlQuery(glueContext, query = SqlQuery3722, mapping = {"customer_curated":Customer_Curated_node1758777152909, "stepTrainer_landing":Step_Trainer_Landing_node1758777154510}, transformation_ctx = "SQLQuery_node1758777772901")

# Script generated for node Step_Trainer_Trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758777772901, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758774575363", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Step_Trainer_Trusted_node1758778062759 = glueContext.getSink(path="s3://stedi-lake-house021511869602/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="Step_Trainer_Trusted_node1758778062759")
Step_Trainer_Trusted_node1758778062759.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
Step_Trainer_Trusted_node1758778062759.setFormat("json")
Step_Trainer_Trusted_node1758778062759.writeFrame(SQLQuery_node1758777772901)
job.commit()