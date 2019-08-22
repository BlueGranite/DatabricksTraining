# Databricks notebook source
# DBTITLE 1,Exercise 1 (answer)
#Answer the following questions about the file

# 1.) What is the delimiter?
#     comma (,)
#
# 2.) Does the file have headers?
#     Yes
#
# 3.) Are there quoted strings in the file?
#     Yes. quoted and escaped (")
#

# COMMAND ----------

# DBTITLE 1,Exercise 2 (answer)
# A reference to our csv file
inPath = "/mnt/training-sources/initech/productsCsv/"

productDF = (
    spark.read  # The DataFrameReader
    .option("header", True)
    .option("inferSchema", True)
    .option("quote", "\"") #bonus
    .option("escape", "\"") #bonus
    .csv(inPath)  # Creates a DataFrame from CSV after reading in the file
)

# COMMAND ----------

# DBTITLE 1,Exercise 3 (answer)
# complete the following code to create the products temporary view

tmpViewName = "userXX_products" #replace "XX" in this variable name with your user number
productDF.createOrReplaceTempView(tmpViewName) # Edit this line to create the temporary view

# COMMAND ----------

# DBTITLE 1,Exercise 4a (answer)
# MAGIC %sql
# MAGIC 
# MAGIC -- How many products are found in the data file?
# MAGIC SELECT COUNT(1) AS NumProducts FROM userXX_products;

# COMMAND ----------

# DBTITLE 1,Exercise 4b (answer)
# MAGIC %sql
# MAGIC 
# MAGIC -- How much is the least expensive product? How much is the most expensive? What is the average product price?
# MAGIC 
# MAGIC SELECT MIN(price) AS minPrice, MAX(price) AS maxPrice, AVG(price) AS avgPrice FROM userXX.products;

# COMMAND ----------

# DBTITLE 1,Exercise 4c (answer)
# MAGIC %sql
# MAGIC 
# MAGIC -- How many Microsoft models are in the data and what is their average price?
# MAGIC 
# MAGIC SELECT 
# MAGIC   COUNT (DISTINCT model) AS modelCount,
# MAGIC   AVG(price) as avgPrice
# MAGIC FROM
# MAGIC   userXX_products
# MAGIC WHERE
# MAGIC   brand = 'Microsoft'
# MAGIC GROUP BY 
# MAGIC   brand

# COMMAND ----------

# DBTITLE 1,Exercise 5 (answer)
# MAGIC %sql
# MAGIC --Hint: Use IF NOT EXISTS to prevent errors when creating a database that may already exist
# MAGIC CREATE DATABASE IF NOT EXISTS userXX;

# COMMAND ----------

# DBTITLE 1,Exercise 6 (answer)
# edit the following code to write data to your table
locationName = "/data/userXX/products"
tableName = "products"

(
  productDF.write
  .option("location", locationName)
  .mode("overwrite")
  .saveAsTable("userXX.products")
)