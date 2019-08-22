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
# MAGIC ## Part 2 - Batch Processing
# MAGIC 
# MAGIC ## Lab Overview
# MAGIC In this lab, you will build a common pattern used in the Batch Layer of the lambda architecture. Initech has provided a large data set containing historical orders they would like loaded to the Data Lake and made available for reporting and analytics. The orders data is currently in raw format and will need to be encriched before it can be used effecively by analysts and business users.
# MAGIC 
# MAGIC ## Technical Accomplishments:
# MAGIC - Read and enrich historcal (batch) data using the following Spark functions
# MAGIC   - `.join()`
# MAGIC   - `.select()`
# MAGIC   - `.drop()`
# MAGIC   - `.withColumn()`
# MAGIC - Write data to a Delta table
# MAGIC - Review Delta Table Metadata

# COMMAND ----------

# MAGIC %md
# MAGIC # Lambda Architecture with Databricks Delta Lake
# MAGIC The Lambda architecture is a modern approach to combining data-in-motion (streaming data) with traditional batch processing practices to enable fast response and real-time analytics, while maintaining a single version of truth provided by more traditional ETL approaches.
# MAGIC 
# MAGIC ## Lambda Architecture Overview
# MAGIC 
# MAGIC The Lamdba Architecture consists of three main *layers*
# MAGIC 
# MAGIC <img src="https://www.blue-granite.com/hubfs/LambdaArchitecturev2.png", alt="Lambda Architecture Overview", width="60%" />
# MAGIC 
# MAGIC   ### Batch Layer
# MAGIC 
# MAGIC   Traditional ETL approaches are the benefit of the batch layer. Typically these are run daily and incorporate many different operational systems into a data mart - often based on a star-schema.  Data in this layer has been enriched and is used for Dashboarding, Reporting, and other Analytics
# MAGIC 
# MAGIC   ### Speed Layer
# MAGIC 
# MAGIC   This layer connects to data in motion and with the use of modern technology platforms enables real-time structured analytics and basic dashboarding and reporting. 
# MAGIC 
# MAGIC   ### Serving Layer
# MAGIC 
# MAGIC   The serving layer acts as a single pane of glass for business users and analysts to query data without requiring the knowledge of streaming architectures or batch processing. 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Example 1 
# MAGIC ## Enriching Data - Adding two datasets together
# MAGIC 
# MAGIC Using the flight data from the previous Data Engineering exercise we will now build a modern data pipeline that includes batch processing for historical data, and stream processing for data happening in real time.
# MAGIC 
# MAGIC When working with batch data, we start with a Data Frame, just as in the previous example

# COMMAND ----------

# DBTITLE 1,Load flights data
#create a data frame using the pyspark function spark.read.csv()
flightsDF= (spark
            .read
            .option("header", True)
            .option("inferSchema", True)
            .csv("/databricks-datasets/asa/small/small.csv"))

display(flightsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Spark includes a large number of functions that can be used to enrich and process data. These functions are grouped into two categories: **Transformations** and **Actions**
# MAGIC 
# MAGIC Because Spark is a lazily-evaluated architeture, no Spark jobs are submitted until an **Action** is performed. Review the table below to see examples of common **transformations** and **actions**  This list is by no means comprehensive.
# MAGIC 
# MAGIC ![transformations and actions](https://training.databricks.com/databricks_guide/gentle_introduction/trans_and_actions.png)
# MAGIC 
# MAGIC ETL and ELT processes often have to join to additional datasets to enrich raw data.  Note that the **join** function is a **transformation** -- no Spark jobs are submitted to complete the join

# COMMAND ----------

# DBTITLE 1,Create second data frame for join
#import the sql functions from the pyspark library which contain many of 
#the common ETL/ELT transformations that we perform
from pyspark.sql.functions import *

#create a new data frame containing a list of Airline codes. 
airlineCodesDF = spark.read.table("airlines.airlinecodes")

# COMMAND ----------

# DBTITLE 1,Join data frames to create enriched data set
#enrich the original data frame with the full name of each airline
enrichedDF = flightsDF.join(airlineCodesDF, flightsDF.UniqueCarrier == airlineCodesDF.AirlineCode)

#show the results
display(enrichedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Lab 1
# MAGIC ## Data Enrichment 1: Joins
# MAGIC 
# MAGIC Initech has a large number of historical orders available in raw format it its Data Lake.  This data is stored as parquet files and contains the following columns:
# MAGIC 
# MAGIC + **SalesOrderID** - integer value containing a key to the Sales Order stored in the transactional source system
# MAGIC + **ProductID** - integer value representing the purchase product
# MAGIC + **rowguid** - unique identifier for each row
# MAGIC + **OrderQty** - integer value specifiy the number of products ordered on this line
# MAGIC + **UnitPriceDiscount** - decimal value containing the discount amount, if any
# MAGIC + **ModifiedDate** - the date the transaction was entered or modified
# MAGIC 
# MAGIC In the first exercise of this lab, you will be joining the historical order information to the Proudct table created in the previous notebook. This will add additional product attributes to the data frame to allow for additional analysis

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Exercise 1
# MAGIC ## Create histOrders data frame
# MAGIC 
# MAGIC Using your knowledge of creating data frame from file-based data, create a data frame called **histOrders** that has the following properties:
# MAGIC 
# MAGIC + stored as **Parquet**
# MAGIC + located in the folder **/mnt/training-sources/initech/orders**

# COMMAND ----------

# DBTITLE 1,Complete and execute the following code
# edit the following code to create the histOrders dataframe

histOrders = (
  spark.read
  .format()
  .load()
)

# COMMAND ----------

# DBTITLE 1,Run this cell to load validation tests
# MAGIC %run "./ValidationTests/04 - Data Engineering 2 Validation"

# COMMAND ----------

# DBTITLE 1,Run this cell to test your work
print(testLab1Ex1(histOrders))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Exercise 2
# MAGIC ## Create *products* data frame
# MAGIC 
# MAGIC As we learning in the example above, you'll need a minimum of two data frame to join them together. Create a second data frame name **products** that is sourced from the products table created in the previous notebook.
# MAGIC 
# MAGIC *NOTE: Be sure to use the correct database name based on your user number*

# COMMAND ----------

# DBTITLE 1,Edit and run the following code
#edit this code to read the data in the products table
products = (
  spark.read
)

# COMMAND ----------

# DBTITLE 1,Run this cell to load validation tests
# MAGIC %run "./ValidationTests/04 - Data Engineering 2 Validation"

# COMMAND ----------

# DBTITLE 1,Run this cell to test your work
print(testLab1Ex2(products))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Exercise 3
# MAGIC ## Complete enrichment of histOrders
# MAGIC 
# MAGIC With both data frames created they can now be joined together. Use the following cells to examine both data frames and determine the appropriate keys to join on.  When the keys have been determined, edit the code to complete the join and create **enrichedOrders**

# COMMAND ----------

# DBTITLE 1,Execute the following code 
display(histOrders)

# COMMAND ----------

# DBTITLE 1,Execute the following code
display(products)

# COMMAND ----------

# DBTITLE 1,Complete and execute the  cell
#complete the code below to join histOrders and products
enrichedOrders = 

# COMMAND ----------

# DBTITLE 1,Run this cell to load validation tests
# MAGIC %run "./ValidationTests/04 - Data Engineering 2 Validation"

# COMMAND ----------

# DBTITLE 1,Run this cell to test your work
print(testLab1Ex3(enrichedOrders))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Lab 2
# MAGIC ## Data Enrichment 2: Data set sculpting
# MAGIC 
# MAGIC After joining two tables together in pyspark, the resulting data frame will contain all columns from both tables. Very rarely are all columns from both tables required for analysis. Spark includes a couple of helpful functions that will allow you to craft the shape of the dataset exactly as needed.
# MAGIC 
# MAGIC + **select()** - A list of arguments are passed via the call to `select()`. The output is a new data frame containing only columns whose names match the input arguments. `select()` returns the columns in the same order as passed in, effectively allowing for column reordering in the dataset.
# MAGIC 
# MAGIC + **drop()** - Like it's adjacent function, `drop()` accepts a list of input arguments and matches them to the column names present in the data frame. The output `drop()` returns is a data frame that only contains columns whose names do not match any on the input list. `drop()` returns the output data frame in the same order as the original data frame.
# MAGIC 
# MAGIC In addition to passing in a hard-coded list of column names, python lists and list algebra can be used to dynmically **select()** or **drop()** columns in a data frame.

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC # Example 2
# MAGIC ## Sclupting flight data
# MAGIC 
# MAGIC In the following cells, you will learn how to use `select()` and `drop()` to sculpt the perfect data set for analysis.

# COMMAND ----------

# DBTITLE 1,Select columns to keep
#to use the select function, simply provide a list of column names that should be selected
selectDF = enrichedDF.select("Year", "Month", "DayofMonth", "AirlineName", "Alliance", "FlightNum", "TailNum", "ActualElapsedTime", "arrDelay", "depDelay" )

display(selectDF)

# COMMAND ----------

# DBTITLE 1,Drop columns to remove
#similarily, the drop() function accepts a list of column names that should be dropped from the input data frame
dropDF = enrichedDF.drop("DayOfWeek", "DepTime", "CRSDepTime", "ArrTime", "CRSArrTime", "UniqueCarrier")

display(dropDF)

# COMMAND ----------

# DBTITLE 1,Bonus example: Column selection iterator
# each data frame has a ".columns" property
#.columns is a python list. It can be used as input to the select function and iterated in a for loop

#example: Select only columns that start with 'A'
allAColumnsDF = enrichedDF.select([c for c in enrichedDF.columns if c.startswith("A")])

print(allAColumnsDF.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##Lab 2 - Project Order columns
# MAGIC 
# MAGIC In this lab, you will continue the process of enriching the Orders data frame by projecting specific columns from the **enrichedOrders** data frame created in **Lab 1**

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #Exercise 4
# MAGIC ## Data Set Scultping
# MAGIC 
# MAGIC Upon completing this lab, you will create a new dataframe named **projectedOrders** that will contain the following columns, in order:
# MAGIC 
# MAGIC + rowguid
# MAGIC + ModifiedDate
# MAGIC + brand
# MAGIC + category
# MAGIC + model
# MAGIC + price
# MAGIC + OrderQty
# MAGIC + UnitPriceDiscount
# MAGIC 
# MAGIC You may choose either the `select()` or `drop()` approach as you wish.

# COMMAND ----------

# DBTITLE 1,Complete the following cell
#edit the code below to create the the data frame with the specified columns
projectedOrders = 

# COMMAND ----------

# DBTITLE 1,Run this cell to load validation tests
# MAGIC %run "./ValidationTests/04 - Data Engineering 2 Validation"

# COMMAND ----------

# DBTITLE 1,Run this cell to test your work
print(testLab2Ex1(projectedOrders))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Lab 3
# MAGIC ## Data Enrichment 3: Derived Columns
# MAGIC 
# MAGIC One of the most common and arguably, most important activities in ETL/ELT pipelines is deriving new columns. Usually this involves performing calculations on already existing columns most likey from the result of projection and joins to collect the right data set.
# MAGIC 
# MAGIC Spark and its corresponding *python, scala, SQL, and R* ecosystems have a pretty large number of different ways to derive new columns. In this workshop we will use a function named **withColumn()**. 
# MAGIC 
# MAGIC **withColumn()** accepts two parameters: 
# MAGIC 
# MAGIC 1. A *string* value specifying the new column name
# MAGIC 2. A *expression* made up of a number of functions used to derive the new column

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Example 3
# MAGIC ## Calculate Total Delay
# MAGIC 
# MAGIC Let's take a look at an example using the airline data

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <img src="/files/img/FlightDataSetReview.jpg" alt="Screenshot of airline data table">

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Notice there are two delay columns:
# MAGIC 
# MAGIC + `arrDelay` the number of minutes late (or early) on arrival
# MAGIC + `depDelay` the number of minutes late (or early) departing
# MAGIC 
# MAGIC Follow the example below to learn how to make a new column named **totDelay** with the formula of `Arrival Delay + Departure Delay`
# MAGIC 
# MAGIC ----
# MAGIC 
# MAGIC *Note: There are several different syntaxes for referencing a column in pyspark. A good habit to get in is to use the `col()` function to reference a data frame column  and `lit()` function to supply a literal value. Both of these functions are included in the **pyspark.sql.functions** library*

# COMMAND ----------

from pyspark.sql.functions import *

derivedDF = selectDF.withColumn("totDelay", col("ArrDelay") + col("DepDelay"))

display(derivedDF) #Note: Derived columns are added to the end of the data frame

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Lab 3
# MAGIC ## Derive Line Item Amount
# MAGIC 
# MAGIC 
# MAGIC With your raw orders data set enriched with product information, and pruned to contain only analytical columns it's now time to build in real value by deriving new columns. As you saw in the examples above, deriving column(s) is an easy task that can add a lot of value and usability to your analysis.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Exercise 5
# MAGIC ## Derived Column
# MAGIC 
# MAGIC After joining the historical orders data set to the products data set, you now have all of the information required to calculate a column names `TotalLineItemAmt`. The fields required from the **projectedOrders** data frame are:
# MAGIC 
# MAGIC + OrderQty
# MAGIC + UnitPriceDiscount
# MAGIC + price
# MAGIC 
# MAGIC To calculate the total cost of the line item, use the following business logic:
# MAGIC 
# MAGIC `OrderQty * ((1 - UnitPriceDiscount) * ItemPrice)`

# COMMAND ----------

# DBTITLE 1,Complete and run the cell
#Edit the code below to create the finalOrders
finalOrders = projectedOrders.

# COMMAND ----------

# DBTITLE 1,Run this cell to load validation tests
# MAGIC %run "./ValidationTests/04 - Data Engineering 2 Validation"

# COMMAND ----------

# DBTITLE 1,Run this cell to test your work
print(testLab3Ex1(finalOrders))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Lab 4 
# MAGIC ## Data Enrichment 4: Preserving results
# MAGIC 
# MAGIC When building a modern data platform, writing the data in a format that is efficent for storage, updates, and queries is incredibly important. To help you choose the right one, Databricks supports a new table format called Delta. Delta tables are stored in Parquet format, but include additional metatdata to enables upserts (updates + insert) and transactional deletes. 
# MAGIC 
# MAGIC It also includes automated optimzation support to ensure the data files in the back end are correctly sized.
# MAGIC 
# MAGIC Writing a data frame as Delta format is easy, and requires only minor changes from our initial exaqmple of writing. At a minimum the only change from the example in the previous notebook is to change the **format** of the table. See below for an example
# MAGIC 
# MAGIC If you already have a Delta table created for that location, you don't need to create the table again, simply appending data to the folder will update the table.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Example 4
# MAGIC ## Data Lake to Delta Lake
# MAGIC 
# MAGIC Review the code below to create a Delta enabled table

# COMMAND ----------

#in this example, we'll write the derivedDF as a Delta table and create the Spark table to enable SQL and ODBC access to the data

spark.sql("DROP TABLE IF EXISTS airlines.flights_delta")

dbutils.fs.rm("/data/airlines/flights_delta", True)

(
derivedDF
    .write
    .format("delta")
    .mode("overwrite") #use overwrite to replace existing data or append to add new rows
    .option("path", "/data/airlines/flights_delta")
    .saveAsTable("airlines.flights_delta")
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC **Delta** tables contain metadata that normal Parquet tables do not. One piece of metadata is a transaction history log. You can access this log with the following command

# COMMAND ----------

# MAGIC %sql DESCRIBE HISTORY airlines.flights_delta;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Run the `OPTIMIZE` function to ensure the storage layer is configured for peak operation at query time. `ZORDER` can be used to index data for specific columns that are used often in queries, often a key or date value.

# COMMAND ----------

# MAGIC %sql OPTIMIZE airlines.flights_delta ZORDER BY (TailNum)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Lab 4 
# MAGIC ## Create Resuable Results with Databricks Delta
# MAGIC 
# MAGIC In the previous labs, you've enriched the historical orders and they are now ready for saving in their final location.  By saving enriched results in a new Delta table, you are enabling future projects and analysts easier access to the process you've already built. Chosing Delta as the format is a smart move as it add three key features we look for with a data management platform:  
# MAGIC <br />  
# MAGIC + **Audit and Security** - Delta supports a fully logged transaction model and stores history for a minimum of 30 days. Each transactional change to the application is logged and most can be rolled back if needed. Because the Delta format has been developed to work in parallel with exsiting technologies, Databricks RBAC Table Access control model applies, just as with standard Databricks tables.
# MAGIC <p />  
# MAGIC + **Performance** - Delta stores data in highly compressed columnar format called Parquet.  Parquet is the standard for high-performing Spark applications. Delta combines Parquet's aggressive compression and high performance with automated functions to `OPTIMIZE` the file storage layer and also introduces `ZORDER` which acts as an index for table to more accurately partition prune and even file prune when determined by the query optimizer.
# MAGIC <p />  
# MAGIC + **Managability** - Because Delta includes a fully supported transaction log, Databricks tables stored in this format now have full `ACID` transaction support. Inserts, Updates, Deletes, and yes, even upserts with `MERGE` are now available for use and fully supported in Databricks applications.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Exercise 6
# MAGIC ## Save as Delta
# MAGIC 
# MAGIC Edit the cell below to save the **finalOrders** data frame as a Delta table.  Be sure to use the correct database, and edit the location to match your database name.  Variables have been provided for the location and table names. Use those in your code to write the new table. Be sure to select the correct mode that will delete any existing table and write the new data.

# COMMAND ----------

# DBTITLE 1,Complete and execute the cell
locationName = "/data/userXX/orders" #edit the userXX database to match your user number
tableName = "userxx.orders" #replace xx with your user number

(
  finalOrders #add the correct parameters to adjust the location and table name
    .write
    .format()
    .mode()
    .option()
    .saveAsTable
)

# COMMAND ----------

# DBTITLE 1,Complete and run the following cell
# MAGIC %sql 
# MAGIC 
# MAGIC --replace xx in the database name with your user number. ZORDER BY rowguid.
# MAGIC OPTIMIZE userxx.orders ZORDER BY () 

# COMMAND ----------

# DBTITLE 1,Run this cell to load validation tests
# MAGIC %run "./ValidationTests/04 - Data Engineering 2 Validation"

# COMMAND ----------

# DBTITLE 1,Run this cell to test your work
print(testLab4Ex1(tableName))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) Basic Lambda Architeture Pipeline with Delta Lake and Databricks
# MAGIC 
# MAGIC <img src="https://kpistoropen.blob.core.windows.net/collateral/delta/Delta.png", alt="Lambda Architecture Pipeline", width="60%" />

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Review
# MAGIC 
# MAGIC In this notebook we've covered the following topics:
# MAGIC 
# MAGIC + Reading raw data from source files
# MAGIC + Enriching data sets by combining them together with `.join()`
# MAGIC + Sculpting data using `select()` and `drop()`
# MAGIC + Creating actionable insights by deriving columns using `withColumn()`
# MAGIC + Enabling self-service analytics by presenting curated data using Delta tables in a Data Lake
# MAGIC 
# MAGIC This notebook only scratches the surface for working with Spark. Follow the links below for more information.
# MAGIC + <a href="https://docs.databricks.com/delta/index.html" target="_blank">Delta Lake Guide</a>
# MAGIC + <a href="https://docs.microsoft.com/en-us/learn/modules/perform-basic-data-transformation-in-azure-databricks/" target="_blank">Transformations in Databricks</a>
# MAGIC + <a href="https://docs.microsoft.com/en-us/learn/paths/data-engineering-with-databricks/" target="_blank">Data Engineering with Azure Databricks</a>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <div style=".img{float:right;}">
# MAGIC   <svg class="img">
# MAGIC     <img src="https://media.licdn.com/dms/image/C4D0BAQGTxEimg0Z8yQ/company-logo_200_200/0?e=2159024400&v=beta&t=QbnJyKfK_pGIUaYC7JdZ2p6wXAfQ-pylS6Y6UjLpyu0" alt="Blue-Granite">
# MAGIC   </svg>
# MAGIC   <p>
# MAGIC   Want to learn more about Azure Databricks and explore how it can be used within your organization? <a href="https://www.blue-granite.com/azure-databricks-poc" target="_blank">Contact us today to learn more!</a>
# MAGIC   <br>
# MAGIC   Come check out our blog on <a href="https://www.blue-granite.com/blog/topic/azure-databricks" target="_blank">Azure Databricks.</a>
# MAGIC   </p>
# MAGIC </div>