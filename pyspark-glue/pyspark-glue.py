
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
customers_dyf = dyf
dyfjoin = customers_dyf.join(paths1 = ['customerid'], paths2 = ['customerid'], frame2 = orders_dyf)
dyfjoin.show()
dyfjoin.printSchema()
dyfjoin.toDF().show()
glueContext.write_dynamic_frame.from_options(
                            frame = customers_dyf,
                            connection_type = 's3',
                            connection_options = {"path": "s3://pyspark-glue-s3/temp_sink"},
                            format = 'csv',
                            format_options = {
                                'separator':',',
                                'optimizePerformance':True
                            },
                            transformation_ctx = 'writing_to_s3'
)
glueContext.write_dynamic_frame.from_options(
                            frame = customers_dyf,
                            connection_type = 's3',
                            connection_options = {"path": "s3://pyspark-glue-s3/temp_sink/"},
                            format = "csv",
                            format_options = {
                                'separator':','
                            },
                            transformation_ctx = 'writing_to_s3'
)
glueContext.write_dynamic_frame.from_catalog(
    frame = customers_dyf,
    name_space = "pyspark_tutorial_db",
    table_name = "customers_write_dyf"
)
sparkDF = dyf.toDF()
sparkDF.show()
dfselect = sparkDF.select("customerid", "fullname")
dfselect.show()
# creating a new column 
from pyspark.sql.functions import lit

# add new column with literal value
dfNewColumn = sparkDF.withColumn("date", lit("2023-03-23"))
dfNewColumn.show()
# create a new column and add two strings together
from pyspark.sql.functions import concat

dfNewFullName = sparkDF.withColumn("new_full_name", concat("firstname",concat(lit(" "), "lastname")))
dfNewFullName.show()
# drop columns 
dfDropCol = sparkDF.drop("firstname","lastname")
dfDropCol.show()
# renaming columns
dfRenameCols = sparkDF.withColumnRenamed("fullname","full_name_new").show()
# grouping df
sparkDF.groupBy("lastname").count().show()
# filter on lastname Adams
sparkDF.filter(sparkDF["lastname"] == "Adams").show()
sparkDF.filter("lastname = 'Adams'").show()
sparkDF.filter(sparkDF.lastname == "Adams").show()
sparkDF.where(sparkDF.lastname == "Adams").show()
sparkDF.where("lastname = 'Adams'").show()
# Joining spark dataframes

df_orders = orders_dyf.toDF()
sparkDF.join(other = df_orders, on = sparkDF.customerid == df_orders.customerid).show()
# Import Dynamic DataFrame class
from awsglue.dynamicframe import DynamicFrame

#Convert from Spark Data Frame to Glue Dynamic Frame
dyfCustomersConvert = DynamicFrame.fromDF(sparkDF, glueContext, "convert")

#Show converted Glue Dynamic Frame
dyfCustomersConvert.show()
# write down the data in converted Dynamic Frame to S3 location. 
glueContext.write_dynamic_frame.from_options(
                            frame = dyfCustomersConvert,
                            connection_type="s3", 
                            connection_options = {"path": "s3://pyspark-glue-s3/customers_write_dyf/"}, 
                            format = "csv", 
                            format_options={
                                "separator": ","
                                },
                            transformation_ctx = "datasink2")
# write data from the converted to customers_write_dyf table using the meta data stored in the glue data catalog 
glueContext.write_dynamic_frame.from_catalog(
    frame = dyfCustomersConvert,
    database = "pyspark_tutorial_db",  
    table_name = "customers_write_dyf")
job.commit()