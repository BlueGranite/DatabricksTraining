# Databricks notebook source
# DBTITLE 1,Exercise 1 (answer)
# edit the following code to create the histOrders dataframe

histOrders = (
  spark.read
  .format("parquet")
  .load("/mnt/training-sources/initech/orders")
)


# COMMAND ----------

# DBTITLE 1,Exercise 2 (answer)
#edit this code to read the data in the products table
products = (
  spark.read.table("userXX.products")
)

# COMMAND ----------

# DBTITLE 1,Exercise 3 (answer)
from pyspark.sql.functions import *
from pyspark.sql.types import *
#complete the code below to join histOrders and products
enrichedOrders = histOrders.join(products, histOrders.ProductID == products.product_id)

# COMMAND ----------

# DBTITLE 1,Exercise 4 (answer)
#edit the code below to create the the data frame with the specified columns
projectedOrders = enrichedOrders.select("rowguid", "ModifiedDate", "brand", "category", "model", "price", "OrderQty", "UnitPriceDiscount")

#we will also accept the more verbose (but perhapse more flexible)
cols = ["rowguid", "ModifiedDate", "brand", "category", "model", "price", "OrderQty", "UnitPriceDiscount"]
projectedOrders = enrichedOrders.select(cols)

# COMMAND ----------

# DBTITLE 1,Exercise 5 (answer)
#Edit the code below to create the finalOrders
finalOrders = projectedOrders.withColumn("TotalLineItemAmt", col("OrderQty") * ((1 - col("UnitPriceDiscount")) * col("price")))
# display(finalOrders)


# COMMAND ----------

# DBTITLE 1,Exercise 6 (answer)
locationName = "/data/userXX/orders" #edit the userXX database to match your user number
tableName = "userxx.orders" #replace xx with your user number

(
  finalOrders #add the correct parameters to adjust the location and table name
    .write
    .format("delta")
    .mode("overwrite")
    .option("location", locationName)
    .saveAsTable(tableName)
)

# COMMAND ----------

# MAGIC %sql SELECT * FROM userxx.orders 

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE userxx.orders ZORDER BY (rowguid);