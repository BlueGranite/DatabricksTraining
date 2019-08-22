# Databricks notebook source
# DBTITLE 1,Exercise 1a (answer)
#this is the path to the real-time orders feed.
#files are stored in Avro format and are found in a deep recursive directory structure

#spark allows wildcards in a path, which will instruct Spark to traverse all paths at that level
ordersInputPath = "dbfs:/mnt/training-sources/bgdbxworkshop/initechorders/*/*/*/*/*/*/*.avro"


#the schema property of the data frame can be used to set schema in another data frame definition
#enter the correct **format** and **location** to read the data into a batch data frame and extract the schema
messageSchema = spark.read.format("avro").load(ordersInputPath).schema

# COMMAND ----------

# DBTITLE 1,Exercise 1b (answer)
#edit the code below to configure the connection to the real-time orders
#hint: Remember the files are stored in Avro format
ordersInputStream = (
    spark.readStream
    .schema(messageSchema) \
    .option("maxFilesPerTrigger", 1)
    .format("avro")
    .load(ordersInputPath)
)

# COMMAND ----------

# DBTITLE 1,Exercise 2 (answer)
from pyspark.sql.types import *
from pyspark.sql.functions import *

#build the command to select the body of the message
#and cast it to a string

#Helpful functions:
# select()
# cast()

ordersMessageStream = ordersInputStream.select(ordersInputStream.Body.cast("string"))

# COMMAND ----------

# DBTITLE 1,Exercise 3 (answer)
#create the message schema
#fill in the missing values

from pyspark.sql.types import *

orderMessageSchema = StructType([
    StructField("SalesOrderID", IntegerType()),
    StructField("ProductID", IntegerType()),
    StructField("rowguid", StringType()),
    StructField("OrderQty", IntegerType()),
    StructField("UnitPriceDiscount", FloatType()),
    StructField("ModifiedDate", StringType())
])

#now construct the final data frame that will parse the message
#use from_json to convert the string to a json structure
#then select all of the attributes from that structure
RealTimeOrdersStream = (
    ordersMessageStream
      .select(from_json(col("Body"), orderMessageSchema).alias("orders"))
      .select("orders.*")
)

#prepare the stream configuration and present as a Temporary View
#this will enable data profiling with SQL

#replace userXX with your user number
RealTimeOrdersStream.createOrReplaceTempView("userXX_RealTimeOrders")

# COMMAND ----------

# DBTITLE 1,Exercise 5a (answer)
#Complete the code below tocreate a data frame containing product attributes. 
#hint: product attributes are available in the userXX.products table 
products = spark.read.table("userxx.products")

#now combine the raw orders with the product attributes
joinedRealTimeOrders = RealTimeOrdersStream.join(products, RealTimeOrdersStream.ProductID == products.product_id)

# COMMAND ----------

# DBTITLE 1,Exercise 5b (answer)
#Next, you'll need to modify the data frame to ensure that it contains only the
#following fields, in this order

projectedRealTimeOrders = joinedRealTimeOrders.select("rowguid", "ModifiedDate", "brand", "category", "model", "price", "OrderQty", "UnitPriceDiscount")

# COMMAND ----------

# DBTITLE 1,Exercise 5c (answer)
# finally  calcuate the [totalLineItemAmount] metric
# the formula to calucate [totalLineItemAmount] is 
# ItemQty * ((1 - UnitPriceDiscount) * UnitPrice)


RealTimeOrders = projectedRealTimeOrders.withColumn("TotalLineItemAmt", col("OrderQty") * ((1 - col("UnitPriceDiscount")) * col("price")))

#review your work
display(RealTimeOrders)

#replace the temporary view created in Lab 1
RealTimeOrders.createOrReplaceTempView("userXX_RealTimeOrders") #be sure to replace userXX with your user number

# COMMAND ----------

# DBTITLE 1,Exercise 6 (answer)
#first we have to set up a helper function that will merge 
#each microbatch into the delta table

def upsertToDelta(microBatchDF, batchId):
  
  #create a temp view based on the microbatch so
  #the MERGE sql command can be executed
  microBatchDF.createOrReplaceTempView("orderUpdates")
  
  #edit the merge statement below to update the correct orders
  sqlMERGECommand = """
  MERGE INTO userxx.orders dest USING orderUpdates src
    ON src.rowguid = dest.rowguid
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *"""

  #execute the microbatch MEREGE using the context of the microbatch
  (
  microBatchDF._jdf.sparkSession()
    .sql(sqlMERGECommand)
  )
  
#now that the helper function that executes the MERGE is created
#begin writing the Stream to the Delta table.
(
  RealTimeOrders
  .writeStream
  .foreachBatch(upsertToDelta)
  .outputMode("update")
  .start()
)

# COMMAND ----------

# MAGIC %sql SELECT * FROM userxx.orders ORDER BY ModifiedDate DESC