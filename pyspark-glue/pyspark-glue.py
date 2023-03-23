
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
# s3output = glueContext.getSink(
#   path="s3://bucket_name/folder_name",
#   connection_type="s3",
#   updateBehavior="UPDATE_IN_DATABASE",
#   partitionKeys=[],
#   compression="snappy",
#   enableUpdateCatalog=True,
#   transformation_ctx="s3output",
# )
# s3output.setCatalogInfo(
#   catalogDatabase="demo", catalogTableName="populations"
# )
# s3output.setFormat("glueparquet")
# s3output.writeFrame(DyF)
dyf.printSchema()
dyf.count()
dyf.show(50)
fullnames = dyf.select_fields(paths=['customerid', 'fullname'], transformation_ctx = 'get_names', info='get_names')
fullnames.printSchema()
fullnames.show()
fullnames.toDF().show()
only_full_names = dyf.drop_fields(paths = ['firstname', 'lastname'], transformation_ctx = 'drop_first_last', info = 'drop_first_last')
only_full_names.toDF().show()
only_full_names.count()
map_renamed_dyf = dyf.apply_mapping(mappings = [('fullname','string','official_name','string')])
map_renamed_dyf.show()
map_renamed_dyf.printSchema()
dyf.printSchema()
renamed_dyf = dyf.rename_field(oldName='fullname', newName='off_name')
renamed_dyf.printSchema()
dyf.printSchema()
dyf_adams = dyf.filter(f = lambda x : x['lastname'] in 'Adams')
dyf_adams.show()
dyf_adams.printSchema()
orders_dyf = glueContext.create_dynamic_frame.from_catalog(database = 'pyspark_tutorial_db', table_name = 'orders')
orders_dyf.show(10)
print('Schema for orders:')
orders_dyf.printSchema()
job.commit()