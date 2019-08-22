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
# MAGIC # Notebook Introduction
# MAGIC Each notebook has a default language that was selected when the notebook was created.  It can not be changed.
# MAGIC 
# MAGIC This is a python notebook.
# MAGIC 
# MAGIC Notebooks can be created as
# MAGIC - Python
# MAGIC - Scala
# MAGIC - SQL
# MAGIC - R

# COMMAND ----------

#python code can be executed in a cell
result = 1 + 1
print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Execute the previous cell by clicking the small **Play** button in the upper right corner. *NOTE: You may have to click the cell, then hover your mouse in the upper right corner for the menu to appear*
# MAGIC 
# MAGIC You can also execute a cell using Keyboard Shortcuts
# MAGIC 
# MAGIC - CTRL + Enter = Run Command in Cell
# MAGIC - SHIFT + Enter = Run Command in Cell then Move to Next Cell

# COMMAND ----------

# MAGIC %md
# MAGIC # Cell Magic
# MAGIC 
# MAGIC You can change the language of any inidivial cell regardless of the notebook's default language

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT 42 AS TheAnswer;

# COMMAND ----------

# MAGIC %scala
# MAGIC val x = 1
# MAGIC val y = x + x
# MAGIC y

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls /mnt

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC ps -a

# COMMAND ----------

# MAGIC %md
# MAGIC # Workflow Execution
# MAGIC 
# MAGIC You can call one notebook from another to create a workflow

# COMMAND ----------

# MAGIC %run ../../Admin/PreWorkshopPrep

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Data Profiling
# MAGIC In addition to developing code in multiple languages, Databricks notebooks are also great data profiling tools.

# COMMAND ----------

# MAGIC %sql DESCRIBE airlines.sampleflights

# COMMAND ----------

# MAGIC %sql SELECT actualelapsedtime FROM airlines.sampleflights