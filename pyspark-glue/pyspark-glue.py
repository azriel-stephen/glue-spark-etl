
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
dyf = glueContext.create_dynamic_frame.from_catalog(database='pyspark_tutorial_db', table_name='customers')
dyf.printSchema()
df = dyf.toDF()
df.show()
dynamicFrameCustomers = dyf
dynamicFrameCustomers.printSchema()
dynamicFrameCustomers.show(10)
dynamicFrameCustomers.count()
dynamicFrameCustomers.select_fields(['customerid']).show(10)
job.commit()