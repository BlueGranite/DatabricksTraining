# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://cdn2.hubspot.net/hubfs/257922/BGLogoHeader.png" alt="Blue-Granite">

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2019 BlueGranite, Inc. All rights reserved.<br/>

# COMMAND ----------

# MAGIC %md
# MAGIC # Building a Data Lake
# MAGIC ## Part 3 - Stream Processing
# MAGIC 
# MAGIC ### Lab Overview
# MAGIC In the previous lab, you explored how to ingest, enrich, transform, and present a historical orders dataset that was already created.  In this lab, you'll be working with live orders as they happen on Initech's online shopping portal. Initech architects believe that reacting faster to customer trends, suggestive selling using advanced processes, and better internal response by Operations will all be enabled by ingesting data faster than yesterday.
# MAGIC 
# MAGIC Initech has configued a data streaming framework that sends details of each transaction as it happens to an Azure Event Hub. Event Hubs are a message queuing services offered by Microsoft in the Azure cloud computing platform. These Event HUbs maintain an ordered queue of messages that any number of different consumers can connect to.  For our explroation, Initech has also configued Event Hubs Capture, which is a service that allows events to be permenetally stored in Azure Blob Storage. We will be connecting to this permanent storage layer to consume events in real-time.
# MAGIC 
# MAGIC ## Technical Accomplishments
# MAGIC * Building a Structure Streamining Data Frame using `readStream()`
# MAGIC * Decode and reconstruct the logged transaction using `from_json()`
# MAGIC * Encrich data in real-time
# MAGIC * Write directly to a Delta table using `writeStream()`

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Lambda Architecture Review  
# MAGIC 
# MAGIC This lab will focuss on the **Speed Layer** of the Lambda Architecture. 
# MAGIC 
# MAGIC <img src="https://www.blue-granite.com/hubfs/LambdaArchitecturev2.png", alt="Lambda Architecture Overview", width="60%" />
# MAGIC 
# MAGIC In the speed layer, we typically connect to a stream of data and consume events as they happen in real time. Modern platforms, like Azure Databricks, allow for analytic functions on the data happening in real time.
# MAGIC 
# MAGIC ### Challenges
# MAGIC * Larger Data
# MAGIC * Faster data and decisions - seconds, minutes, hours not days or weeks after it is created
# MAGIC * Streaming Pipelines can be hard
# MAGIC * Realtime Dashboards and alerts - for the holiday season, promotional campaigns, track falling or rising trends
# MAGIC 
# MAGIC ### Azure Databricks Solutions
# MAGIC * Easy streaming pipelines almost the same as batch - SQL, Python, Scala, Java & R
# MAGIC * Make this data available on Storage and Delta enabled tables to end users in minutes not days or weeks. 
# MAGIC 
# MAGIC ### Why Initech Needs Streaming
# MAGIC * Sales up or down (rolling 24 hours, 1 hour), to identify trends that are good or bad
# MAGIC * Holidays and promotions - how are they performing in real time

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Lab 1 
# MAGIC ## Create Streaming Data Frame
# MAGIC 
# MAGIC In this example, we will demonstrate how to create a streaming data frame that is connected to a Azure Blob Storage container that is the endpoint of an Azure Event Hub streaming pipeline. Each event that is streamed into Event Hubs is written to an Azure Blob conatiner. *Editor's Note: This is done in the workshop to reduce the costs and complexity of mainitaining an Azure Event Hub that would consumed by a group of users concurrently*

# COMMAND ----------

# MAGIC %md
# MAGIC # Example 1
# MAGIC ## What is Structured Streaming?
# MAGIC 
# MAGIC <div style="width: 100%">
# MAGIC   <div style="margin: auto; width: 800px">
# MAGIC     <img src="http://spark.apache.org/docs/latest/img/structured-streaming-stream-as-a-table.png"/>
# MAGIC   </div>
# MAGIC </div>
# MAGIC 
# MAGIC Data is appended to the Input Table every _trigger interval_. For instance, if the trigger interval is 1 second, then new data is appended to the Input Table every seconds.
# MAGIC 
# MAGIC ### Structure Streaming and Azure Event Hubs
# MAGIC When working with Structure Streaming Data Frames, there are a few considerations:
# MAGIC 
# MAGIC 1. You must supply a schema in the data frame defintion
# MAGIC 2. If you are reading from Event Hubs, the message that contains the data will be stored in a *binary* field named **Body**
# MAGIC 3. Your cluster will stay on as long as the stream in being read. Be sure to cancel any running cells to allow your cluster to auto-terminate
# MAGIC 
# MAGIC The following cell will show how to infer the schema of the stream, and save it for later use

# COMMAND ----------

# DBTITLE 1,Read a sample of the stream and save the schema
#Event Hub events are captured in real-time to a blob storage account. The folder structure shown
#below points to the location where the events are captured.
flightsInputPath = "dbfs:/mnt/training-sources/bgdbxworkshop/flights/*/*/*/*/*/*/*.avro"

#the schema property of the data frame can be used to set schema in another data frame definition
messageSchema = spark.read.format("avro").load(flightsInputPath).schema
print(messageSchema)

# COMMAND ----------

# DBTITLE 1,Create connection to captured stream events
#streaming DataFrame reader for data on Azure Storage
inputStream = (
    spark.readStream
    .schema(messageSchema) \
    .option("maxFilesPerTrigger", 1)
    .format("avro")
    .load(flightsInputPath)
               )

display(inputStream)

# COMMAND ----------

# DBTITLE 1,Convert Body to a String value
#covert the Body to a string value
messageStream = inputStream.select(inputStream.Body.cast("string"))
display(messageStream)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Introducing from_json()
# MAGIC 
# MAGIC `from_json()` is a function found in the pyspark.sql.functions package. It's used to convert a string of valid json to a predefined schema that must be supplied to the function.

# COMMAND ----------

# DBTITLE 1,Extract attributes from json
from pyspark.sql.types import *
from pyspark.sql.functions import *

#manually create the flightsSchema to be using in the from_json() function
flightsSchema = StructType([
  StructField("Year", IntegerType(), False),
  StructField("Month", IntegerType(), False),
  StructField("DayOfMonth", IntegerType(), False),
  StructField("UniqueCarrier", StringType(), False),
  StructField("FlightNum", IntegerType(), False),
  StructField("TailNum", StringType(), False),
  StructField("ActualElapsedTime", IntegerType(), False),
  StructField("arrDelay", IntegerType(), False),
  StructField("depDelay", IntegerType(), False)
])

#use the schema above to parse the json string into a structure.
#use a nested select pattern to then select all of the individual attributes of the structure
flightsStreamDF = (
    messageStream
    .select(from_json(col("Body"), flightsSchema).alias("flights"))
    .select("flights.*")
)

#view the real-time data
display(flightsStreamDF)

#register the data frame as a temporary view to enable SQL analysis
flightsStreamDF.createOrReplaceTempView("realtimeflights")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Lab 1
# MAGIC 
# MAGIC ## Real-time Initech Orders
# MAGIC For this exercise, you'll connect to a stream of orders that are being placed on Initech's online shopping portal. The orders contain the same information as the historical order data you worked with in the previous section.
# MAGIC 
# MAGIC The following attributes are available in the data stream:
# MAGIC 
# MAGIC Attribute Name | Data Type
# MAGIC ---|---
# MAGIC **SalesOrderID** | IntegerType()
# MAGIC **ProductID** | IntegerType()
# MAGIC **rowguid** | StringType()
# MAGIC **OrderQty** | IntegerType()
# MAGIC **UnitPriceDiscount** | DecimalType()
# MAGIC **ModifiedDate** | StringType()
# MAGIC 
# MAGIC Read and edit the code below to create the **RealTimeOrderStream** - a structured streaming data frame containg the enriched order data similarly built with the Batch Layer pipeline in the previous section.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Exercise 1 
# MAGIC ### Configure the Spark Structure Streaming connection
# MAGIC 
# MAGIC Initech streams orders real-time to an Azure Event Hub. Because they have less stringest latency requirements, Initech captures Event Hub events in an Azure Storage contaier, where analytics tools like Azure Databricks can connect and consume in realtime.
# MAGIC 
# MAGIC Event Hubs Captures stores each event in **avro** file format.  The path where the streaming events are stored have been saved in a variable named **ordersInputPath**

# COMMAND ----------

# DBTITLE 1,Complete and run the cell
#this is the path to the real-time orders feed.
#files are stored in Avro format and are found in a deep recursive directory structure

#spark allows wildcards in a path, which will instruct Spark to traverse all paths at that level
ordersInputPath = "dbfs:/mnt/training-sources/bgdbxworkshop/initechorders/*/*/*/*/*/*/*.avro"


#the schema property of the data frame can be used to set schema in another data frame definition
#enter the correct **format** and **location** to read the data into a batch data frame and extract the schema
messageSchema = spark.read.format().load().schema

#review the schema
print(messageSchema)

# COMMAND ----------

# MAGIC %md
# MAGIC Now that you have the schema created for the message review and complete the following code to configure the connection to the real-time stream

# COMMAND ----------

# DBTITLE 1,Complete and run the cell
#edit the code below to configure the connection to the real-time orders
#hint: Remember the files are stored in Avro format
ordersInputStream = (
    spark.readStream
    .schema() \
    .option("maxFilesPerTrigger", 1)
    .format()
    .load()
)

#display the structured streaming data frame. It will update every few seconds
display(ordersInputStream)

# COMMAND ----------

# DBTITLE 1,Run this cell to load validation tests
# MAGIC %run "./ValidationTests/05 - Data Engineering 3 Validation"

# COMMAND ----------

# DBTITLE 1,Run this cell to test your work
print(testLab1Ex1(ordersInputStream))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Exercise 2
# MAGIC ### Decode the message
# MAGIC 
# MAGIC With the connection to the stream complete, you now can decode the message and read the data
# MAGIC 
# MAGIC Review and edit the code below to decode the messages. Don't forget to review the data frame in Exercise 1 to understand the message schema

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

#build the command to select the body of the message
#and cast it to a string

#Helpful functions:
# select()
# cast()

ordersMessageStream = ordersInputStream.
display(messageStream)

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

#build the command to select the body of the message
#and cast it to a string

#Helpful functions:
# select()
# cast()

ordersMessageStream = ordersInputStream.select(ordersInputStream.Body.cast("string"))
display(ordersMessageStream)

# COMMAND ----------

# DBTITLE 1,Run this cell to load validation tests
# MAGIC %run "./ValidationTests/05 - Data Engineering 3 Validation"

# COMMAND ----------

# DBTITLE 1,Run this cell to test your work
print(testLab1Ex2(messageStream))

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Exercise 3
# MAGIC ### Create structured data
# MAGIC 
# MAGIC In this exercise, you'll need to convert the message string decoded above and apply a known structure to it.
# MAGIC 
# MAGIC To review, the message contains the following attributes:
# MAGIC 
# MAGIC Attribute Name | Data Type
# MAGIC ---|---
# MAGIC **SalesOrderID** | IntegerType()
# MAGIC **ProductID** | IntegerType()
# MAGIC **rowguid** | StringType()
# MAGIC **OrderQty** | IntegerType()
# MAGIC **UnitPriceDiscount** | DecimalType()
# MAGIC **ModifiedDate** | StringType()
# MAGIC 
# MAGIC Review and complete the code below to parse the message and begin creating valuable data

# COMMAND ----------

#create the message schema
#fill in the missing values

from pyspark.sql.types import *

orderMessageSchema = StructType([
    StructField("SalesOrderID", __________),
    StructField("ProductID", IntegerType()),
    StructField("rowguid", ____________),
    StructField("OrderQty", IntegerType()),
    StructField(__________, FloatType())),
    StructField("ModifiedDate", TimestampType())
])

#now construct the final data frame that will parse the message
#use from_json to convert the string to a json structure
#then select all of the attributes from that structure
RealTimeOrdersStream = (
    ordersMessageStream
      .select(from_json(col(), ).alias("orders"))
      .select("orders.*")
)

#prepare the stream configuration and present as a Temporary View
#this will enable data profiling with SQL

#replace userXX with your user number
RealTimeOrdersStream.createOrReplaceTempView("userXX_RealTimeOrders")

# COMMAND ----------

# DBTITLE 1,Run this cell to load validation tests
# MAGIC %run "./ValidationTests/05 - Data Engineering 3 Validation"

# COMMAND ----------

# DBTITLE 1,Run this cell to test your work
print(testLab1Ex3(RealTimeOrdersStream))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Exercise 3 Review
# MAGIC ### Profile real-time data
# MAGIC 
# MAGIC Now that the stream is configured, use a SQL cell to query the stream and examine the results in real-time.altzone

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --Write a query to retun the stream messages
# MAGIC SELECT * FROM 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Example 2
# MAGIC ## Enriching Real-Time Data
# MAGIC 
# MAGIC In the first lab you learned how to connect to a data stream and consume events in real-time. You also learned that Spark's structured streaming data frames are highly flexible objects that can be interacted with using any of the DataFrame API functions, or SparkSQL.
# MAGIC 
# MAGIC In the previous section, we consumed a large amount of historical orders after enriching the raw order data. With Spark structured streaming, the approaches and methods used to enrich batch data are the same.
# MAGIC 
# MAGIC In this example, you'll learn how to enrich data in real-time.  Remember in the previous section we enriched the data by:
# MAGIC 
# MAGIC 1. Joining to the **AirlineCodes** and adding in the order's product attributes
# MAGIC 2. Selecting only the columns we desired to save for analysis
# MAGIC 3. Calculating the **totDelay** metric for analtyics
# MAGIC 
# MAGIC The cells below show how to enrich data in real-time using structured streatming data frames

# COMMAND ----------

# DBTITLE 1,Add Airline Attributes
#create a data frame containing the additional airline attributes to add to the flight stream
airlineCodesDF = spark.read.table("airlines.airlinecodes")

#combine the real-time flight infomration with the additional airline attributes
joinedRealTimeFlights = flightsStreamDF.join(airlineCodesDF, flightsStreamDF.UniqueCarrier == airlineCodesDF.AirlineCode)

#review the results
display(joinedRealTimeFlights)

# COMMAND ----------

# DBTITLE 1,Project columns for analysis
#once the two data frames are commbined select() or drop() can be used to 
#project the data frame to only the columns needed for analysis
projectedRealTimeFlights = joinedRealTimeFlights.select("Year", "Month", "DayofMonth", "AirlineName", "Alliance", "FlightNum", "TailNum", "ActualElapsedTime", "arrDelay", "depDelay")

# display(projectedRealTimeFlights)

# COMMAND ----------

# DBTITLE 1,Calculate totDelay
#structured streaming data frames are based on the DataFrame API 
#therefore .withColumns() works just as expected
RealTimeFlights = projectedRealTimeFlights.withColumn("totDelay", col("arrDelay") + col("depDelay"))

#review the results
display(RealTimeFlights)

#replace the temp view created in the previous example
RealTimeFlights.createOrReplaceTempView("realtimeflights")

# COMMAND ----------

# DBTITLE 1,Number of flights by Carrier
# MAGIC %sql
# MAGIC 
# MAGIC SELECT
# MAGIC   Alliance,
# MAGIC   AirlineName,
# MAGIC   COUNT(1) AS NumFlights 
# MAGIC FROM realtimeflights 
# MAGIC GROUP BY Alliance, AirlineName
# MAGIC ORDER BY Alliance, NumFlights

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab 2
# MAGIC ## Enrich Raw Orders in Real-Time
# MAGIC 
# MAGIC In Lab 1, you configured the connection to the stream of real-time orders from the Initech online shopping portal, and have prepared the data for future enrichment. Similar to the previous section on Batch Processing, you would be completing the following enrichments in this exercise:
# MAGIC 
# MAGIC Complete the cells below to create the RealTimeOrders data frame that will be **enriched** and ready for real-time analysis

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Exercise 4
# MAGIC ## Enrich in Real-Time
# MAGIC 
# MAGIC Using your newly learned skills in data enrichment, apply the same ETL process you applied in the previous section
# MAGIC 
# MAGIC 1. Add product attributes to the raw order real-time stream
# MAGIC 2. Ensure the data frame only includes columns that have meaningful value for analysis
# MAGIC   + rowguid
# MAGIC   + ModifiedDate
# MAGIC   + brand
# MAGIC   + category
# MAGIC   + model
# MAGIC   + price
# MAGIC   + OrderQty
# MAGIC   + UnitPriceDiscount  
# MAGIC 3. Calculate the **LineItemTotalAmount** metric using the `OrderQty * ((1 - UnitPriceDiscount) * ItemPrice)` business logic

# COMMAND ----------

# DBTITLE 1,Add Product Attributes
#Complete the code below tocreate a data frame containing product attributes. 
#hint: product attributes are available in the userXX.products table 
products = spark.read

#now combine the raw orders with the product attributes
joinedRealTimeOrders = RealTimeOrderStream.



# COMMAND ----------

# DBTITLE 1,Project columns for analysis
#Next, you'll need to modify the data frame to ensure that it contains only the
#following fields, in this order

projectedRealTimeOrders = joinedRealTimeOrders

# COMMAND ----------

# DBTITLE 1,Calculate totalLineItemAmount
# finally  calcuate the [totalLineItemAmount] metric
# the formula to calucate [totalLineItemAmount] is 
# ItemQty * ((1 - UnitPriceDiscount) * UnitPrice)


RealTimeOrders = projectedRealTimeOrders.

#review your work
display(RealTimeOrders)

#replace the temporary view created in Lab 1
RealTimeOrders.createOrReplaceTempView("userXX_RealTimeOrders") #be sure to replace userXX with your user number

# COMMAND ----------

# DBTITLE 1,Run this cell to load validation tests
# MAGIC %run "./ValidationTests/05 - Data Engineering 3 Validation"

# COMMAND ----------

# DBTITLE 1,Run this cell to test your work
print(testLab2Ex1(RealTimeOrders))

# COMMAND ----------

# DBTITLE 1,Exercise 4: Explore results
# MAGIC %sql
# MAGIC 
# MAGIC --Use the following SQL cell to explore and profile the real-time orders
# MAGIC SELECT
# MAGIC   
# MAGIC FROM
# MAGIC   userXX_RealTimeOrders --be sure to replace userxx with your user number

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Example 3
# MAGIC ## Serving Analytics with Structured Streaming and Delta Lake
# MAGIC 
# MAGIC Remember that in the Lambda Architecture, the Streaming Layer and Batch Layer are combined into a Serving Layer that can be used to query data ingested in real-time as well as historical data processed by batch.
# MAGIC 
# MAGIC For this example, we will show how to merge data in real-time to an existing Delta table. A SQL interface will provide a serving layer to answer user queries for data both at rest and in motion.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --Review the current Delta table for storing flight stats
# MAGIC SELECT * FROM flights_delta LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --Notice the columns of the delta table are the same as the 
# MAGIC --enriched real time data set created in Example 2
# MAGIC SELECT * FROM realtimeflights LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Update Delta Tables using MERGE
# MAGIC 
# MAGIC When loading data into a reporting or data mart, it's very common to use an **upsert** pattern. That is, insert new rows, and update existing rows. When using Delta tables in Azure Databricks, full ACID transaction support is avialable, including a **MERGE** statement to efficiently perform upserts into exsiting tables.
# MAGIC 
# MAGIC When combined with a structure streaming data set, we are able to stream enriched data in real-time into a persisted table for end users to query anytime they need. Since current transactions are streamed in real time, queries and reports never need to be out of date.
# MAGIC 
# MAGIC When writing data directly to a Delta table in real-time, you'll need to create a helper function and use two new pyspark functions:
# MAGIC 
# MAGIC `writeStream().start()` - the command the instructs Spark to begin writing the stream in micro batches
# MAGIC `forEachBatch()` - this will call a function or expression for each microbatch that is written
# MAGIC 
# MAGIC Here is the **MERGE** statement in action.

# COMMAND ----------

#first we have to set up a helper function that will merge 
#each microbatch into the delta table

def upsertToDelta(microBatchDF, batchId):
  
  #create a temp view based on the microbatch so
  #the MERGE sql command can be executed
  microBatchDF.createOrReplaceTempView("flightUpdates")
  
  sqlMERGECommand = """
  MERGE INTO airlines.flights_delta dest USING flightUpdates src
    ON src.FlightNum = dest.FlightNum 
      AND src.TailNum = dest.TailNum 
      AND src.AirlineName = dest.AirlineName
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
  RealTimeFlights
  .writeStream
  .foreachBatch(upsertToDelta)
  .outputMode("update")
  .start()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --View the history log of the Delta table to see the MERGE commands
# MAGIC DESCRIBE HISTORY airlines.flights_delta

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Optimizing Delta tables.
# MAGIC 
# MAGIC When working with streaming data it is common for distributing computing platform like Spark to write this data in many small files.  While efficient for writing, this is not an optimized format for user queries.  
# MAGIC 
# MAGIC Delta tables have the capability to optimize the table and ensure that the files stored in the backend are optimized for queries.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC OPTIMIZE airlines.flights_delta ZORDER BY (TailNum);

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab 3
# MAGIC ## Streaming Delta Lake
# MAGIC 
# MAGIC With the enriched dataset from Lab 2 orders can now be streamed into the Delta table. Use the MERGE statement to load new orders into the table in real-time. In this Lab, you'll perform the following steps:
# MAGIC 
# MAGIC 1. Create a helper function that loads a microbatch of events from the real-time stream
# MAGIC 2. Upsert into the **orders** table using `MERGE`
# MAGIC 3. Query the data and review the real time updates
# MAGIC 
# MAGIC Complete the cells below to merge the results of the enriched data frame into the Delta table **userXX.orders**

# COMMAND ----------

# DBTITLE 1,Exercise 5: Upsert Orders into Delta table in real-time
#first we have to set up a helper function that will merge 
#each microbatch into the delta table

def upsertToDelta(microBatchDF, batchId):
  
  #create a temp view based on the microbatch so
  #the MERGE sql command can be executed
  microBatchDF.createOrReplaceTempView("orderUpdates")
  
  #edit the merge statement below to update the correct orders
  sqlMERGECommand = """
  MERGE INTO userxx.orders dest USING orderUpdates src
    ON 
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

# DBTITLE 1,Review Results
# MAGIC %sql
# MAGIC 
# MAGIC --Write a SQL statement to review the userXX.orders table updates in real-time. HINT: Look at the latest records first...

# COMMAND ----------

# MAGIC %md
# MAGIC # Review
# MAGIC 
# MAGIC In this lab, you've learned how to use Spark Structured Streaming and enrich data in real time. You've also learned how to stream enriched data into a high-performing Delta table to enable timely and accurate user queries.  The following skills were covered in this section:
# MAGIC 
# MAGIC + Configure and connect to data being generated in real-time using `readStream()`
# MAGIC + Enrich data using DataFrame and SparkSQL APIs in real-time
# MAGIC + Save real-time results in a Delta table using `writeStream()`
# MAGIC 
# MAGIC For more information on Streaming and Delta Optimization, see the following links: