# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://cdn2.hubspot.net/hubfs/257922/BGLogoHeader.png" alt="Blue-Granite">

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC &copy; 2019 BlueGranite, Inc. All rights reserved.<br/>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Building a Data Lake
# MAGIC ## Part 1 - First Steps
# MAGIC 
# MAGIC In this workshop, you will take the role of an everyday Data Analyst at a mid-tier electronics retailer, Initech.  Initech has enjoyed relative stability over the last few decades, serving local customers in their chain of Midwest-based retail stores, as well as a budding online presence through their own shopping portal. Initech sells about $350M nationwide in retail sales, combined with $175M in online orders. As happens when one generation takes over from their predecessor, new management styles are beginning to emerge across the organization and a push towards self-service and agile product delivery is taking hold.
# MAGIC 
# MAGIC As one of the 2 data analysts at Initech, who support all data (re: Databases but mostly Access), reporting (re: data dump request grantor) and analytics (re: home grown ODS), you have been asked to figure out how to enable innovation, foster a fail-fast-fail-well mentality towards analytics, and provide secure access to required data and existing operational reports. Oh, and while you're at it, some of that AI would be great too.  You've done your research and are pretty sure that a Data Lake will provide the business value and innovation your executive team is demanding.
# MAGIC 
# MAGIC *Where will you begin?*
# MAGIC 
# MAGIC ### Challenges
# MAGIC * Data is all over the place - reports, KPIs and DS is hard with bigger data from disparate sources on exiting tools
# MAGIC 
# MAGIC ### Why Initech Needs a Data Lake
# MAGIC * Store both big and varied data in one location for all personas - Data Engineering, Data Science, Analysts 
# MAGIC * They need to access this data in diffrent languages and tools - SQL, Python, Scala, Java, R with Notebooks, IDE, Power BI, Tableau, JDBC/ODBC
# MAGIC 
# MAGIC ### Azure Databricks Solutions
# MAGIC * Azure Storage or Azure Data Lake - Is a place to store all data, big and small
# MAGIC * Access both big (TB to PB) and small data easily with Databricks' scaleable clusters
# MAGIC * Use Python, Scala, R, SQL, Java
# MAGIC ----
# MAGIC ## Lab Overview
# MAGIC In this lab, you will learn how to connect to and manage a data source in Azure Databricks
# MAGIC 
# MAGIC ## Technical Accomplishments:
# MAGIC - Read data into a dataframe using pyspark
# MAGIC - Profile and query data using spark sql
# MAGIC - Write data out to a databricks table

# COMMAND ----------

# DBTITLE 1,Azure Modern Data Platform Reference Architecture
# MAGIC %md 
# MAGIC <img src="/files/img/DBXWorkshopDiagram.png"/>

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading and Writing Data
# MAGIC 
# MAGIC **Technical Accomplishments:**
# MAGIC - Read data from CSV
# MAGIC   - `spark.read`
# MAGIC   - `.format`
# MAGIC - Write data to Spark table

# COMMAND ----------

# MAGIC %md
# MAGIC ### The Data Source
# MAGIC * For this example, we will be using a file called **small.csv**.
# MAGIC * The data represents flight statistics.
# MAGIC * We can use **&percnt;head ...** to view the first few lines of the file.

# COMMAND ----------

# DBTITLE 1,Review file structure
# MAGIC %fs head /databricks-datasets/asa/small/small.csv

# COMMAND ----------

# MAGIC %md
# MAGIC # Example 1 
# MAGIC ## Read A CSV File
# MAGIC Let's start with the bare minimum by specifying that the file is delimited and the location of the file:  
# MAGIC The default delimiter for `spark.read.csv( )` is comma but it can be changed by specifying the optional delimiter parameter.

# COMMAND ----------

#create a data frame using the pyspark function spark.read.csv()
flightsDF = (spark
            .read
            .csv("/databricks-datasets/asa/small/small.csv"))

# COMMAND ----------

display(flightsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Notice that the data frame does not look quite right. This CSV file contains headers as the first row. The definition of the data frame can be modified with options.

# COMMAND ----------

# create a data frame using the pyspark function spark.read.csv()
# add an option to specifiy the first row contains a header
flightsDF = (spark
            .read
            .option("header", True)
            .csv("/databricks-datasets/asa/small/small.csv"))

# COMMAND ----------

display(flightsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Our data frame is looking better, however, a deeper look into the data will show a discrepency with the data types.
# MAGIC 
# MAGIC If data is small, Spark can infer the data types. If it's not small, you may have to manally specify the schema

# COMMAND ----------

# create a data frame using the pyspark function spark.read.csv()
# add an option to specifiy the first row contains a header
flightsDF = (spark
            .read
            .option("header", True)
            .option("inferSchema", True)
            .csv("/databricks-datasets/asa/small/small.csv"))

# COMMAND ----------

display(flightsDF)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC Notice that this time, it look longer to create the data frame. Spark must run one or more jobs to correctly infer the schema. Make sure to not use inferSchema with large data sets

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Lab 1
# MAGIC 
# MAGIC ## Reading Data
# MAGIC 
# MAGIC For this exercise, we will be using a file called **product.csv**  
# MAGIC The data represents new products we are planning to add to our online store.  
# MAGIC We can use %head ... to view the first few lines of the file.
# MAGIC 
# MAGIC The file is found in the directory **/mnt/training-sources/initech/productsCsv/product.csv**

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Exercise 1
# MAGIC ## Examine and collect file properties
# MAGIC 
# MAGIC 
# MAGIC Using the %fs magic, examine the file to collect specific properties that will be used to tell Spark how to correctly read the data.

# COMMAND ----------

# DBTITLE 1,Execute cell
# MAGIC %fs head /mnt/training-sources/initech/productsCsv/product.csv

# COMMAND ----------

# DBTITLE 1,Answer the following
#Answer the following questions about the file

# 1.) What is the delimiter?
#
#
# 2.) Does the file have headers?
#
#
# 3.) Are there quoted strings in the file?
#
#

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC In the following cell, modify the code such that a data frame named *productDF* is created that contains:
# MAGIC 
# MAGIC - correct column names
# MAGIC - correct data types
# MAGIC - correct data
# MAGIC 
# MAGIC Answering the questions above will help you create the data frame
# MAGIC 
# MAGIC Help: There are many options available when reading a data frame. Find more [information about the options here](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=read%20csv#pyspark.sql.streaming.DataStreamReader) *Ctrl+Click to open in a new tab*

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Exercise 2
# MAGIC ## Read products into data frame
# MAGIC 
# MAGIC The data for Initech's product database has been extract to a delimited file format and placed in an Azure Storage account. That account has been mounted to the Azure Databricks workspace.  T
# MAGIC 
# MAGIC The path for the products file is stored in a variable named **inPath**

# COMMAND ----------

# DBTITLE 1,Complete the following code
# A reference to our csv file
inPath = "/mnt/training-sources/initech/productsCsv/"

productDF = (
    spark.read  # The DataFrameReader
    # remove this line and replace with correct options
    # hint: this data is QUOTEd and ESCAPEd with double quotation marks
    .csv(inPath)  # Creates a DataFrame from CSV after reading in the file
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Don't forget to take a look at your data frame to make sure it's been created succesfully

# COMMAND ----------

display(productDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 2 - Profile data with SQL
# MAGIC 
# MAGIC When working on a new analytics project, or just when looking a new data, it's a helpful and often necessary exercise to examine and profile the data. SQL is a great language for this task. 
# MAGIC 
# MAGIC Dataframes can be examined using SQL by creating a temporary view with a function named `createOrReplaceTempView()`
# MAGIC 
# MAGIC `createOrReplaceTempView` accepts a string as its only argumeent that specifies the name of the temporary view. That view will be scope to only this execution of the notebook.  Whwen you browse away from the notebook, you can no longer access the temporary view, unless you come back and access it from the notebook it was created in.
# MAGIC 
# MAGIC Note:  
# MAGIC *There is an adjacent function named `createOrReplaceGlobalView()` that will create a view that is accessible from any notebook in the workspace.*

# COMMAND ----------

#this function creates a temporary view that can enables SQL access to the data
sampleDF.createOrReplaceTempView("dbx_example")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC With the temporary view created, you can use a %sql *magic* cell to execute data profiling queries. Remember that each cell can only be written in one language.

# COMMAND ----------

# MAGIC %sql SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM example

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Spark SQL supports many standard SQL functions. If you have existing queries used for reporting or analytics, they will probably work in Spark with only minor modifications

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT 
# MAGIC   COUNT(1) AS FlightCount
# MAGIC  ,SUM(Distance) AS TotalDistance
# MAGIC  ,Year * 10000 + Month * 100 + DayofMonth AS DateKey
# MAGIC  ,UniqueCarrier
# MAGIC FROM
# MAGIC   example
# MAGIC GROUP BY
# MAGIC   Year * 10000 + Month * 100 + DayofMonth
# MAGIC  ,UniqueCarrier
# MAGIC HAVING FlightCount > 100
# MAGIC ORDER BY 
# MAGIC   DateKey

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Lab 2
# MAGIC 
# MAGIC ## Profiling Data
# MAGIC 
# MAGIC For this exercise, we will continue using the data frame created above. 
# MAGIC In the following examples and exercises you will explore and profile the products found in the data file

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Exercise 3
# MAGIC ## Create SparkSQL objects for analysis

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC Create a temporary view named **userXX_products**. Use the data frame created above
# MAGIC 
# MAGIC *NOTE: Because we are working in a shared environment, each table or view created in the workshop should be created with a unique name.*

# COMMAND ----------

# DBTITLE 1,Complete the following code
# complete the following code to create the products temporary view

tmpViewName = "userXX_products" #replace "XX" in this variable name with your user number
productDF # Edit this line to create the temporary view

# COMMAND ----------

# MAGIC %md
# MAGIC Validate the view has been created succesfully using the following query

# COMMAND ----------

# DBTITLE 1,Edit and execute the cell
# MAGIC %sql SELECT * FROM userXX_products -- replace "XX" with your user number

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Exercise 4
# MAGIC ## Data Profiling

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Profile the data using SQL. Use the temporary view created in Exercise 3
# MAGIC 
# MAGIC Write 3 SQL queries in the following cells. The queries should help provide answers the following questions
# MAGIC 
# MAGIC 1. How many products are in the data file?
# MAGIC 2. What is the most expensive product in the data frame? The least expensive?
# MAGIC 3. How many Microsoft laptops are in the data and what is their average price?
# MAGIC 
# MAGIC *Note: Remember to use the correct user database*

# COMMAND ----------

# DBTITLE 1,Write query to answer the following
# MAGIC %sql
# MAGIC 
# MAGIC -- How many products are found in the data file?

# COMMAND ----------

# DBTITLE 1,Write query to answer the following
# MAGIC %sql
# MAGIC 
# MAGIC -- How much is the least expensive product? How much is the most expensive? What is the average product price?

# COMMAND ----------

# DBTITLE 1,Write query to answer the following
# MAGIC %sql
# MAGIC 
# MAGIC -- How many Microsoft models are in the data and what is their average price?

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Lab 3
# MAGIC 
# MAGIC ## Writing Data
# MAGIC 
# MAGIC For this exercise, we will continue using the data frame created above. 
# MAGIC In the following examples and exercises, you will write the data in the data frame to a new Spark table which can be used in later queries.
# MAGIC 
# MAGIC There are many options to consider when writing data. For data that will be used in future analysis, it's helpful to write the data directly to a Spark table.  Spark will store the data in Parquet format by default, which is optimized for distributed processing, and a good choice most of the time.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Example 3
# MAGIC 
# MAGIC Write data created in previous data frames to a Spark table.  Tables are stored in databases. A best practice is to store tables used for shared purposes in the same database.
# MAGIC 
# MAGIC Databases can be created using a SQL statement

# COMMAND ----------

# DBTITLE 1,CREATE TABLE statement - %sql magic
# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS airlines;

# COMMAND ----------

# DBTITLE 1,CREATE TABLE statement - python
#tables can also be created in a python cell

spark.sql("CREATE DATABASE IF NOT EXISTS airlines")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC To see what databases are stored in the Azure Databricks workspace, run the following command in a %sql magic cell

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SHOW DATABASES;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC With the database created for the output table, the following statement can be run to write the data to a table.
# MAGIC 
# MAGIC The **saveAsTable()** function is used to write a dataframe to a table. By default the **saveAsTable()** function writes to a folder found at */user/hive/warehouse/[dbanme]/[tablename]>.*  You can override this default behavior by supplying an optional **"path"**
# MAGIC 
# MAGIC Data written using **saveAsTable()** is stored in Parquet format unless otherwise specified
# MAGIC 
# MAGIC *NOTE: **mode("overwrite")** will remove the table before writing. **mode("append")** will add new data to an existing table*

# COMMAND ----------

#the following code will write the data in Parquet format to the path specified
(
  sampleDF.write
  .mode("overwrite") 
  .option("path", "/data/airlines/flights")
  .saveAsTable("airlines.flights")
)

# COMMAND ----------

# MAGIC %sql DESCRIBE EXTENDED airlines.flights

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Exercise 5
# MAGIC ## Create database objects

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC In order to save tables and enable SQL queries, you'll need to create a new database with your user number in the format userXX where XX is replaced with your user number.
# MAGIC 
# MAGIC *Hint: You can use a %sql magic cell or a python cell to accomplish this*
# MAGIC 
# MAGIC Your database should be named **userXX** where **XX** is your user number: example **user01**

# COMMAND ----------

# DBTITLE 1,Complete the following code
# MAGIC %sql
# MAGIC --Hint: Use IF NOT EXISTS to prevent errors when creating a database that may already exist

# COMMAND ----------

# DBTITLE 1,Execute cell
# MAGIC %sql
# MAGIC --Run this cell to verify that your database has been created.
# MAGIC --Notice that you can see databases created by all users in the workspace
# MAGIC SHOW DATABASES;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Exercise 6
# MAGIC ## Create *products* table

# COMMAND ----------

# MAGIC %md
# MAGIC Using the **productDF** dataframe created in the cells above, write the data to a Spark table named **products** stored in your own user database. Note that the format is **database_name**.**table_name**.  The data should be overwritten so that the table always contains the latest version of the data in the data frame.
# MAGIC 
# MAGIC Variables have been provided for the location and table Name. Be sure to update the user name to reflect your user.

# COMMAND ----------

# DBTITLE 1,Complete the following code
# edit the following code to write data to your table
locationName = "/data/userXX/products"
tableName = "products"

(
  productDF.write
  .option()
  .mode()
  .saveAsTable()
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC When the writing is complete, the table is ready to use. Run the following SQL cell to ensure that your table is working correctly. Remember to edit the database name to match your user number

# COMMAND ----------

# DBTITLE 1,Edit and run the query
# MAGIC %sql
# MAGIC 
# MAGIC USE userXX; -- edit this line to contain your database name
# MAGIC 
# MAGIC SELECT * FROM products;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Review
# MAGIC 
# MAGIC In this notebook we've covered the following topics:
# MAGIC 
# MAGIC + Reading raw data from CSV files
# MAGIC + Profiling and querying data at rest
# MAGIC + Writing data into Spark tables for later use
# MAGIC 
# MAGIC This notebook only scratches the surface for working with Spark. Follow the links below for more information.
# MAGIC + <a href="https://docs.databricks.com/user-guide/importing-data.html" target="_blank">Importing Data</a>
# MAGIC + <a href="https://docs.databricks.com/spark/latest/dataframes-datasets/introduction-to-dataframes-python.html" target="_blank">Intro to Dataframes</a>
# MAGIC + <a href="https://docs.databricks.com/user-guide/tables.html" target="_blank">Writing Data in Databricks</a>
# MAGIC + <a href="https://docs.microsoft.com/en-us/learn/modules/intro-to-azure-databricks/" target="_blank">Intro to Azure Databricks</a>
# MAGIC + <a href="https://spark.apache.org/docs/latest/api/python/index.html" target="_blank">Pyspark API Documentation</a>

# COMMAND ----------

# MAGIC %md
# MAGIC <div style="text-align:left; line-height: 0;">
# MAGIC   <img src="https://media.licdn.com/dms/image/C4D0BAQGTxEimg0Z8yQ/company-logo_200_200/0?e=2159024400&v=beta&t=QbnJyKfK_pGIUaYC7JdZ2p6wXAfQ-pylS6Y6UjLpyu0" alt="Blue-Granite">
# MAGIC   <p>
# MAGIC   Want to learn more about Azure Databricks and explore how it can be used within your organization? <a href="https://www.blue-granite.com/azure-databricks-poc" target="_blank">Contact us today to learn more!</a>
# MAGIC   <p>
# MAGIC   Come check out our blog on <a href="https://www.blue-granite.com/blog/topic/azure-databricks" target="_blank">Azure Databricks.</a>
# MAGIC </div>