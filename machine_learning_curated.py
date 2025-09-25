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

# Script generated for node Step_Trainer_Trusted
Step_Trainer_Trusted_node1758778563956 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="Step_Trainer_Trusted_node1758778563956")

# Script generated for node Accelerometer_Trusted
Accelerometer_Trusted_node1758778561848 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="Accelerometer_Trusted_node1758778561848")

# Script generated for node SQL Query
SqlQuery3727 = '''
select accelerometer_trusted.user, step_trainer_trusted.*,accelerometer_trusted.x
From step_trainer_trusted
inner join accelerometer_trusted
on accelerometer_trusted.timestamp = step_trainer_trusted.sensorreadingtime;

'''
SQLQuery_node1758778826645 = sparkSqlQuery(glueContext, query = SqlQuery3727, mapping = {"accelerometer_trusted":Accelerometer_Trusted_node1758778561848, "step_trainer_trusted":Step_Trainer_Trusted_node1758778563956}, transformation_ctx = "SQLQuery_node1758778826645")

# Script generated for node Machine Learning Curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758778826645, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758778509175", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MachineLearningCurated_node1758779169228 = glueContext.getSink(path="s3://stedi-lake-house021511869602/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1758779169228")
MachineLearningCurated_node1758779169228.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1758779169228.setFormat("json")
MachineLearningCurated_node1758779169228.writeFrame(SQLQuery_node1758778826645)
job.commit()