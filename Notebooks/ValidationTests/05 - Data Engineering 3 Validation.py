# Databricks notebook source
# DBTITLE 1,Lab 1 Exercise 1
def testLab1Ex1(df):
  columns = df.columns
  assert len(columns) == 6, "Expected 6 columns but found " + str(len(columns))
  
  coltypes = df.dtypes
  assert coltypes[5][0] == "Body", "Expected column 5 to be \"Body\" but found \"" + colTypes[5][0] + "\"."
  assert coltypes[5][1] == "binary", "Expected column 5 to be of type \"binary\" but found \"" + colTypes[5][1] + "\"."
  
  return ("Great work! All validation tests passed!")

# COMMAND ----------

# DBTITLE 1,Lab 1 Exercise 2
def testLab1Ex2(df):
  columns = df.columns
  assert len(columns) == 1, "Expected 1 columns but found " + str(len(columns))
  
  coltypes = df.dtypes
  assert coltypes[0][0] == "Body", "Expected column 0 to be \"Body\" but found \"" + colTypes[5][0] + "\"."
  assert coltypes[0][1] == "string", "Expected column 0 to be of type \"string\" but found \"" + colTypes[5][1] + "\"."
  
  return ("Great work! All validation tests passed!")

# COMMAND ----------

# DBTITLE 1,Lab 1 Exercise 3
def testLab1Ex3(df):
  coltypes = df.dtypes
  
  assert coltypes[0][0] == "SalesOrderID", "Expected column 0 to be \"SalesOrderID\" but found \"" + coltypes[0][0] + "\"."

  assert coltypes[0][1] == "int",        "Expected column 0 to be of type \"int\" but found \"" + coltypes[0][1] + "\"."

  assert coltypes[1][0] == "ProductID",   "Expected column 1 to be \"ProductID\" but found \"" + coltypes[1][0] + "\"."
  assert coltypes[1][1] == "int",     "Expected column 1 to be of type \"int\" but found \"" + coltypes[1][1] + "\"."

  assert coltypes[2][0] == "rowguid",      "Expected column 2 to be \"rowguid\" but found \"" + coltypes[2][0] + "\"."
  assert coltypes[2][1] == "string",     "Expected column 2 to be of type \"string\" but found \"" + coltypes[2][1] + "\"."

  assert coltypes[3][0] == "OrderQty",      "Expected column 3 to be \"OrderQty\" but found \"" + coltypes[3][0] + "\"."
  assert coltypes[3][1] == "int",     "Expected column 3 to be of type \"int\" but found \"" + coltypes[3][1] + "\"."

  assert coltypes[4][0] == "UnitPriceDiscount",      "Expected column 4 to be \"UnitPriceDiscount\" but found \"" + coltypes[4][0] + "\"."
  assert coltypes[4][1] == "float",     "Expected column 4 to be of type \"float\" but found \"" + coltypes[4][1] + "\"."

  assert coltypes[5][0] == "ModifiedDate",  "Expected column 5 to be \"ModifiedDate\" but found \"" + coltypes[5][0] + "\"."
  assert coltypes[5][1] == "string",     "Expected column 5 to be of type \"string\" but found \"" + coltypes[5][1] + "\"."

  return ("Great work! All validation tests passed!")

# COMMAND ----------

# DBTITLE 1,Lab 2 Exercise 1
def testLab2Ex1(df):
  coltypes = df.dtypes
  
  assert coltypes[0][0] == "rowguid", "Expected column 0 to be \"rowguid\" but found \"" + coltypes[0][0] + "\"."

  assert coltypes[0][1] == "string",        "Expected column 0 to be of type \"string\" but found \"" + coltypes[0][1] + "\"."
  
  assert coltypes[1][0] == "ModifiedDate", "Expected column 1 to be \"ModifiedDate\" but found \"" + coltypes[1][0] + "\"."

  assert coltypes[1][1] == "string",        "Expected column 1 to be of type \"string\" but found \"" + coltypes[1][1] + "\"."

  assert coltypes[2][0] == "brand",   "Expected column 2 to be \"brand\" but found \"" + coltypes[2][0] + "\"."
  assert coltypes[2][1] == "string",     "Expected column 2 to be of type \"string\" but found \"" + coltypes[2][1] + "\"."

  assert coltypes[3][0] == "category",      "Expected column 3 to be \"category\" but found \"" + coltypes[3][0] + "\"."
  assert coltypes[3][1] == "string",     "Expected column 3 to be of type \"string\" but found \"" + coltypes[3][1] + "\"."

  assert coltypes[4][0] == "model",      "Expected column 4 to be \"model\" but found \"" + coltypes[4][0] + "\"."
  assert coltypes[4][1] == "string",     "Expected column 4 to be of type \"string\" but found \"" + coltypes[4][1] + "\"."

  assert coltypes[5][0] == "price",      "Expected column 5 to be \"price\" but found \"" + coltypes[5][0] + "\"."
  assert coltypes[5][1] == "double",     "Expected column 5 to be of type \"double\" but found \"" + coltypes[5][1] + "\"."

  assert coltypes[6][0] == "OrderQty",  "Expected column 6 to be \"OrderQty\" but found \"" + coltypes[6][0] + "\"."
  assert coltypes[6][1] == "int",     "Expected column 6 to be of type \"integer\" but found \"" + coltypes[6][1] + "\"."

  assert coltypes[7][0] == "UnitPriceDiscount",       "Expected column 7 to be \"UnitPriceDiscount\" but found \"" + coltypes[7][0] + "\"."
  assert coltypes[7][1] == "float",     "Expected column 7 to be of type \"float\" but found \"" + coltypes[7][1] + "\"."
  
  assert coltypes[8][0] == "TotalLineItemAmt",       "Expected column 8 to be \"TotalLineItemAmt\" but found \"" + coltypes[7][0] + "\"."
  assert coltypes[8][1] == "double",     "Expected column 8 to be of type \"double\" but found \"" + coltypes[7][1] + "\"."

  return ("Great work! All validation tests passed!")