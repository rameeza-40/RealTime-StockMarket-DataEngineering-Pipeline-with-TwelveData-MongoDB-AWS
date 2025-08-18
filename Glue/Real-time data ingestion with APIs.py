import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

def sparkAggregate(glueContext, parentFrame, groups, aggs, transformation_ctx) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs) if len(groups) > 0 else parentFrame.toDF().agg(*aggsFuncs)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Realtime StockMarket
RealtimeStockMarket_node1755356101633 = glueContext.create_dynamic_frame.from_catalog(database="rameezadb", table_name="realtimeguviproject", transformation_ctx="RealtimeStockMarket_node1755356101633")
from pyspark.sql.functions import col, window

df = df.withColumn(
    "datetime_30min",
    ( (col("datetime").cast("long") / (30*60)).cast("long") * (30*60) ).cast("timestamp")
)

# Script generated for node Aggregate_30mins
Aggregate_30mins_node1755356951116 = sparkAggregate(glueContext, parentFrame = RealtimeStockMarket_node1755356101633, groups = ["datetime"], aggs = [["open", "first"], ["high", "max"], ["low", "min"], ["close", "last"], ["volume", "sum"]], transformation_ctx = "Aggregate_30mins_node1755356951116")

job.commit()