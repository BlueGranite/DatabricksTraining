# Databricks notebook source
# DBTITLE 1,Lab 1 Exercise 1
def testLab1Ex1(df):
  columns = df.dtypes
  assert len(columns) == 6, "Expected 6 columns but found " + str(len(columns))
  
  return ("Great work! All validation tests passed!")

# COMMAND ----------

# DBTITLE 1,Lab 1 Exercise 2
def testLab1Ex2(df):
  columns = df.columns
  assert len(columns) == 8, "Expected 8 columns but found " + str(len(columns))
  
  assert "product_id" in columns, "Unable to find a 'product_id' field. Available fields are {0}".format(columns)
  
  return ("Great work! All validation tests passed!")

# COMMAND ----------

# DBTITLE 1,Lab 1 Exercise 3
def testLab1Ex3(df):
  columns = df.columns
  assert len(columns) == 14, "Expected 14 columns but found " + str(len(columns))
  
  assert "price" in columns, "Unable to find a 'price' field. Available fields are {0}".format(columns)
  
  assert "brand" in columns, "Unable to find a 'brand' field. Available fields are {0}".format(columns)
  
  assert "model" in columns, "Unable to find a 'model' column. Available fields are {0}".format(columns)
  
  
  types = df.dtypes
  
  assert ("price", "double") in types, "'price' should be a double."
  
  return ("Great work! All validation tests passed!")

# COMMAND ----------

# DBTITLE 1,Lab 2 Exercise 1
def testLab2Ex1(df):
  coltypes = df.dtypes
  
  assert coltypes[0][0] == "rowguid", "Expected column 0 to be \"rowguid\" but found \"" + coltypes[0][0] + "\"."

  assert coltypes[0][1] == "string",        "Expected column 0 to be of type \"string\" but found \"" + coltypes[0][1] + "\"."
  
  assert coltypes[1][0] == "ModifiedDate", "Expected column 0 to be \"ModifiedDate\" but found \"" + coltypes[1][0] + "\"."

  assert coltypes[1][1] == "timestamp",        "Expected column 0 to be of type \"timestamp\" but found \"" + coltypes[1][1] + "\"."

  assert coltypes[2][0] == "brand",   "Expected column 1 to be \"brand\" but found \"" + coltypes[2][0] + "\"."
  assert coltypes[2][1] == "string",     "Expected column 1 to be of type \"string\" but found \"" + coltypes[2][1] + "\"."

  assert coltypes[3][0] == "category",      "Expected column 2 to be \"category\" but found \"" + coltypes[3][0] + "\"."
  assert coltypes[3][1] == "string",     "Expected column 2 to be of type \"string\" but found \"" + coltypes[3][1] + "\"."

  assert coltypes[4][0] == "model",      "Expected column 3 to be \"model\" but found \"" + coltypes[4][0] + "\"."
  assert coltypes[4][1] == "string",     "Expected column 3 to be of type \"string\" but found \"" + coltypes[4][1] + "\"."

  assert coltypes[5][0] == "price",      "Expected column 4 to be \"price\" but found \"" + coltypes[5][0] + "\"."
  assert coltypes[5][1] == "double",     "Expected column 4 to be of type \"double\" but found \"" + coltypes[5][1] + "\"."

  assert coltypes[6][0] == "OrderQty",  "Expected column 5 to be \"OrderQty\" but found \"" + coltypes[6][0] + "\"."
  assert coltypes[6][1] == "int",     "Expected column 5 to be of type \"integer\" but found \"" + coltypes[6][1] + "\"."

  assert coltypes[7][0] == "UnitPriceDiscount",       "Expected column 6 to be \"UnitPriceDiscount\" but found \"" + coltypes[7][0] + "\"."
  assert coltypes[7][1] == "float",     "Expected column 6 to be of type \"float\" but found \"" + coltypes[7][1] + "\"."

  return ("Great work! All validation tests passed!")

# COMMAND ----------

# DBTITLE 1,Lab 3 Exercise 1
def testLab3Ex1(df):
  columns = df.columns
  assert len(columns) == 9, "Expected 9 columns but found " + str(len(columns))
  
  coltypes = df.dtypes
  
  assert coltypes[8][0] == "TotalLineItemAmt",       "Expected column 7 to be \"TotalLineItemAmt\" but found \"" + types[7][0] + "\"."
  assert coltypes[8][1] == "double",     "Expected column 7 to be of type \"double\" but found \"" + types[7][1] + "\"."
  
  return ("Great work! All validation tests passed!")

# COMMAND ----------

# DBTITLE 1,Lab 4 Exercise 1
def testLab4Ex1(tableName):
  rowcount = spark.sql("select * from {0}".format(tableName)).count()
  
  assert rowcount > 0, "Expected at least one row in {0} but found {1}".format(tableName, str(rowCount))
  
  return ("Great work! All validation tests passed!")