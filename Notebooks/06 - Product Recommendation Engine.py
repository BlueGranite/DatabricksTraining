# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://cdn2.hubspot.net/hubfs/438089/docs/training/dblearning-banner.png" alt="Databricks Learning" width="555" height="64">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Modfications and MLFlow examples added 2019 by Josh Fennessy, BlueGranite https://www.blue-granite.com

# COMMAND ----------

# MAGIC %md 
# MAGIC # ![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Providing Product Recommendations
# MAGIC 
# MAGIC One of the most common uses of big data is to predict what users want.  This allows Google to show you relevant ads, Amazon to recommend relevant products, and Netflix to recommend movies that you might like.  This lab will demonstrate how we can use Apache Spark to recommend products to a user.  
# MAGIC 
# MAGIC We will start with some basic techniques, and then use the SparkML library's Alternating Least Squares method to make more sophisticated predictions. Here are the SparkML [Python docs](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html) and the [Scala docs](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.package).
# MAGIC 
# MAGIC For this lesson, we will use around 900,000 historical product ratings from our company Initech.
# MAGIC 
# MAGIC In this lab:
# MAGIC * *Part 0*: Data Selection and Engineering
# MAGIC * *Part 1*: MLFlow and SparkML Development
# MAGIC * *Part 2*: Scalable hosting with AzureML

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Challenges
# MAGIC * Data Scientists aren't easily able to collaborate on the complex model development required for an accurate product reocmmendation engine.
# MAGIC * Model tuning is a challenging process. Keeping track of tuning parameters and metric evaluation can be a nightmare during a development push.
# MAGIC * This model needs to be called hundreds of thousands of times per day by a public facing website. 
# MAGIC 
# MAGIC ### Azure Databricks Solutions
# MAGIC * Databricks notebooks support built integration with Github, Bitbuckets, or Azure DevOps source control providers. Pair-programming and integrated comments can also be used within teams.
# MAGIC * MLFlow Tracking allows individual training executions to be logged and analyzed at a later time.
# MAGIC * MLFLow Models can be easily deployed to Azure Kubernetes Services and Azure Machine Learning Services to support scalable operational analtyics.
# MAGIC 
# MAGIC ### Why Initech uses Azure Databricks for ML
# MAGIC * Millions of users and 100,000s of prodcuts, product reccomendations need more than a single machine
# MAGIC * Easy APIs for newer data science team
# MAGIC * Batch processed product recommendations are supported as well as hosted AI models to be called during the customer's browsing session.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### ![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Part 0: Data Selection and Engineering
# MAGIC 
# MAGIC This notebook uses historical product rating data provided by customers and owners of the products being rated. This data is located in a Delta Lake in a table named `productratings`. Additional product and SKU based attributes can be found in a Delta Table named `product`
# MAGIC 
# MAGIC The code in the following cell will:
# MAGIC 
# MAGIC * Create a data frame containing all product ratings 
# MAGIC * Create a data frame containing product attributes
# MAGIC * Combine with additional offline ratings
# MAGIC * Split into training, validation, and test data frames for model development

# COMMAND ----------

#create product_ratings data frame
product_ratings_df = (
   spark
    .read
    .format("delta")
    .load("/delta/initech/productratings")
)

#create products data frame
products_df = (
    spark
      .read
      .format("delta")
      .load("/delta/initech/products")
)

#Offline data is manually entered
offline_user_id = 0
offline_rated_products = [
     (1, offline_user_id, 5.0), # Replace with your ratings.
     (2, offline_user_id, 4.5),
     (3, offline_user_id, 3.0),
     (4, offline_user_id, 5.0),
     (6, offline_user_id, 1.0),
     (7, offline_user_id, 2.0),
     (9, offline_user_id, 4.0),
     (9, offline_user_id, 3.5),
     (9, offline_user_id, 2.0),
     ]

#create a data frame from the manually entered data
offline_ratings_df = spark.createDataFrame(offline_rated_products, ['product_id','user_id','rating'])

#combine offline data with data to be trained.
all_product_ratings_df = product_ratings_df.union(offline_ratings_df)

#split data frame using following ratios: 60% for training, 20% of our data for validation, and leave 20% for testing 
seed = 1800009193 # TODO: Generate random seed
(training_df, validation_df, test_df) = all_product_ratings_df.randomSplit([.6, .2, .2], seed=seed)

#cache the dataframes in the cluster memory
training_df.cache().count()
validation_df.cache().count()
test_df.cache().count()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### ![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Part 1: MLFlow and SparkML Development
# MAGIC 
# MAGIC In the previous notebook, we explored the Spark ML implemention of the Alternating Least Squares (ALS) algortihm used to model product recommendations.  In this notebook, the same algorithim will be used, but we will add in use of the MLFlow framework to help support our team devleopment SDLC.
# MAGIC 
# MAGIC During the development process, a model may be developed, tested, tweaked, reworked, and retrained hundreds of times. It's difficult for a team of developers to efficiently keep track of execution paramters and outcomes. MLFlow Tracking provides a development framework that includes logging and auditing built in. It includes an interactive UI for comparing model executions. Finally it provides a version history of the development process and allows for point in time recovery of specific model behaviors.

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Model Training with MLFLow: 
# MAGIC Use logging and tracking to maintain history of model parameters and outcomes.
# MAGIC 
# MAGIC To get started with [MLFlow](https://mlflow.org/), the library first needs to be installed via the Databricks Portal. For more information on installing libraries see [this guide](https://docs.azuredatabricks.net/user-guide/libraries.html)
# MAGIC 
# MAGIC There are two MLFlow packages you can install from [PyPi](https://pypi.org/project/mlflow/)
# MAGIC 
# MAGIC Package Name | Description
# MAGIC ---|---
# MAGIC mlflow|The base mlflow package that includes tracking, models, and projects
# MAGIC mlflow[extras]|The base mlflow package plus additional common data science tools e.g. scikit-learn
# MAGIC 
# MAGIC Make sure you cluster has one of the libraries listed above installed before continuing.
# MAGIC 
# MAGIC After installing the library, an MLFlow Experiement will need to be created in your Azure Databricks Workspace. [Read more](https://docs.azuredatabricks.net/applications/mlflow/tracking.html#experiments) about creating MLFLow Experiments

# COMMAND ----------

import mlflow
import mlflow.spark
from pyspark.ml.recommendation import ALS 
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline

mlflow_exp = "/Shared/Half-Day Unified Analytics Workshop (DBX)/InitechProductRecommendations"
# mlflow_exp = "<full path to your MLFLow experiment>" #example: "/Users/databrickusers/experiments/product_exp"

mlflow.set_experiment(mlflow_exp)

#params for model tuning
maxIter = 5
regParam = 0.1
setRank = 2

#this will start a MLFlow tracking run. 
with mlflow.start_run():
  # Init ALS Learner
  als = ALS()

  # Set parameters for model calculation
  (als.setPredictionCol("prediction")
     .setUserCol("user_id")
     .setItemCol("product_id")
     .setRatingCol("rating")
     .setMaxIter(maxIter)
     .setSeed(seed)
     .setRegParam(regParam)
     .setRank(setRank)
     .setColdStartStrategy("drop")
  )
  
  #log the run parameters with MFlow Tracking
  mlflow.log_param("maxIterations", maxIter)
  mlflow.log_param("regParam", regParam)
  mlflow.log_param("setRank", setRank)
  
  #create a pipleline model to enable model logging in MLFlow
  #fit the model
  pipeline = Pipeline(stages=[als])
  model = pipeline.fit(training_df)
  
  #test the trained model aginst the test data set (20% of original data size)
  predictions = model.transform(test_df)
        
  #calculate rmse; a metric used to evaulate model effectiveness
  evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                  predictionCol="prediction")

  rmse = evaluator.evaluate(predictions)
  
  #log the calculated metric with MLFlow tracking
  mlflow.log_metric("rmse", rmse)

  #log the model object with MLFlow tracking
  mlflow.spark.log_model(model, "model")
  

# COMMAND ----------

# MAGIC %md
# MAGIC Now that the model training is complete, browse over to the MLFlow Experiement in the Azure Databricks Workspace file browser. The MLFlow portal provides a historical view of each *run* that was executed in the Azure Databricks notebook.
# MAGIC 
# MAGIC Clicking on a run will show the details for that run. You can even see the model object below and browse through the various files that the model is comprised of.
# MAGIC 
# MAGIC Next we will deploy this model so it can be called in real-time from the public website.

# COMMAND ----------

# MAGIC %md 
# MAGIC ### ![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Part 2: Scalable hosting with AzureML
# MAGIC 
# MAGIC Azure Machine Learning Services is a platform-as-a-service resource available in the Azure Cloud. It provides a frameowrk for development, testing, validation, and deployment of machine learning models in all standard languages and machine learning frameworks.
# MAGIC 
# MAGIC For the scope of this demo, we are going to be focusing on the Deployment features of Azure ML. Models created in many tools can be exported using the Azure ML SDK and bundled into a transportable Docker image.  This Docker image can then be hosted in a location of your choice. Azure ML also provides two easy to use hosting options:
# MAGIC 
# MAGIC * Azure Container Instance is a simple single container instance that is useful for development and testing if the image generation process
# MAGIC * Azure Kubernetes Services is a hosted flexible cloud-scale platform for quickly loading additional resouce to meet increasing demand.
# MAGIC 
# MAGIC The code in this section will:
# MAGIC 
# MAGIC * Create an Azure Machine Learning Workspace (if needed)
# MAGIC * Build a Docker image from a model that was logged in MFlow tracking
# MAGIC * Create an Azure Kubernetes Service (AKS) compute target
# MAGIC * Deploy the Docker image to AKS and expose as a web service
# MAGIC * Test calling the hosted web service

# COMMAND ----------

# MAGIC %md
# MAGIC First Azure Databricks needs to connect to an existing Azure Machine Learning Services (AMLS) workspace. If an AMLS workspace doesn't exist, the following code will create it. 
# MAGIC 
# MAGIC *Note: If the requested workspace already exists, it will not be recreated. The workspace object will contain a reference to the existing workspace*

# COMMAND ----------

#setup connection to the Azure ML workspace
import azureml
from azureml.core import Workspace

workspace_name = "<amls workspace_name>"
workspace_location="<amls workspace_location>"
resource_group = "<amls resource_group_name>"
subscription_id = "<azure subscription_id>"

workspace = Workspace.create(name = workspace_name,
                             location = workspace_location,
                             resource_group = resource_group,
                             subscription_id = subscription_id,
                             exist_ok=True)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Next we need to use `run_id` field of the MLFlow Tracking portal to identify the version of the model we with to deploy.
# MAGIC 
# MAGIC This model version will be packaged into a Docker container with the following code example.
# MAGIC 
# MAGIC *Note: You can modify the **conda.yaml** file in the model directory to indclude additional dependencies and software packages that might be needed to execute your model in a container

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###Create a new Docker Image based on the MLFlow model
# MAGIC 
# MAGIC *Note: images can take time to create. Plan on 10 minutes to create the image from this model*

# COMMAND ----------

import mlflow.azureml


run_id = "<MLFlow Tracking run_id>"

#build the URI to download the model details
model_uri = "runs:/" + run_id + "/model"

#enter a desired model description
model_description = "<model_description>"
model_image, azure_model = mlflow.azureml.build_image(model_uri=model_uri, 
                                                      workspace=workspace,
                                                      model_name="model",
                                                      image_name="model",
                                                      description=model_description,
                                                      synchronous=False)

#wait for the model to be created
model_image.wait_for_creation(show_output=True)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Option 2: Connected to a previously created image

# COMMAND ----------

import mlflow.azureml
from azureml.core import Image
from azureml.core.image import ContainerImage

#enter the image name from the Azure ML workspace
image_name = "<image_name>"

#retrieve the image configuration
model_image = Image(workspace, name=image_name)

# COMMAND ----------

# #for DEV deployments, Azure Container Instances

# from azureml.core.webservice import AciWebservice, Webservice

# dev_webservice_name = "skurecs-dev"
# dev_webservice_deployment_config = AciWebservice.deploy_configuration()
# dev_webservice = Webservice.deploy_from_image(name=dev_webservice_name, image=model_image, deployment_config=dev_webservice_deployment_config, workspace=workspace)

# dev_webservice.wait_for_deployment()

# COMMAND ----------

#call the service via a webservice to score the results
import requests
import json

def query_endpoint_example(scoring_uri, inputs, service_key=None):
  headers = {
    "Content-Type": "application/json",
  }
  if service_key is not None:
    headers["Authorization"] = "Bearer {service_key}".format(service_key=service_key)
    
  print("Sending batch prediction request with inputs: {}".format(inputs))
  response = requests.post(scoring_uri, data=json.dumps(inputs), headers=headers)
  preds = json.loads(response.text)
  print("Received response: {}".format(preds))
  return preds

ratings_pd = my_ratings_df.toPandas()

print(ratings_pd)

sample = ratings_pd.iloc[[0]]
                                                 
query_input = sample.to_json(orient='split')
query_input = eval(query_input)
# query_input.pop('index', None)
print(query_input)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC To deploy an image, we need to create a new or connect to an existing Azure Kubernetes Service compute target.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a New AKS Cluster
# MAGIC 
# MAGIC *Note: It will take a few minutes to create a new AKS cluster*

# COMMAND ----------

from azureml.core.compute import AksCompute, ComputeTarget

# Use the default configuration (you can also provide parameters to customize this)
prov_config = AksCompute.provisioning_configuration()

aks_cluster_name = "<aks_name>"

# Create the cluster
aks_target = ComputeTarget.create(workspace = workspace, 
                                  name = aks_cluster_name, 
                                  provisioning_configuration = prov_config)

# Wait for the create process to complete
aks_target.wait_for_completion(show_output = True)
print(aks_target.provisioning_state)
print(aks_target.provisioning_errors)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connect to an existing AKS Cluster

# COMMAND ----------

from azureml.core.compute import AksCompute, ComputeTarget

# Get the resource group from https://portal.azure.com -> Find your resource group
resource_group = "<resource-group>"

# Give the cluster a local name
aks_cluster_name = "<aks_name>"

# Attatch the cluster to your workgroup
attach_config = AksCompute.attach_configuration(resource_group=resource_group, cluster_name=aks_cluster_name)
aks_target = ComputeTarget.attach(workspace, name=aks_cluster_name, attach_configuration=attach_config)

# Wait for the operation to complete
aks_target.wait_for_completion(True)
print(aks_target.provisioning_state)
print(aks_target.provisioning_errors)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC With the AKS cluster created and connected to, the image can now be deployed as a web service.
# MAGIC 
# MAGIC The code in the following cell will deploy the model selected from MLFlow in the cells above

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a new web service deployment from a model image

# COMMAND ----------

from azureml.core.webservice import Webservice, AksWebservice

# Set configuration and service name
webservice_name = "<prod_webservice_name>"

#default configuration can be modified for custom requirements
webservice_deployment_config = AksWebservice.deploy_configuration()

# Deploy from image selected above
webservice = Webservice.deploy_from_image(workspace = workspace, 
                                               name = webservice_name,
                                               image = model_image, 
                                               deployment_config = webservice_deployment_config,
                                               deployment_target = aks_target)

#wait for the webservice to be deployed
webservice.wait_for_deployment(show_output = True)

# COMMAND ----------

print(webservice.get_logs())

# COMMAND ----------

# MAGIC %md if you receive an error deploying your image as a webservice, you can use the following code to get information from the image creation logs

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Connect to an existing Web Service in Azure ML

# COMMAND ----------

from azureml.core.webservice import Webservice

webservice_name = "<webservice_name>"

#connect to an existing web service
webservice = Webservice(name=webservice_name, workspace=workspace)

# COMMAND ----------

# MAGIC %md
# MAGIC With the webservice succesfully deployed, a test call to the API can be executed. 
# MAGIC 
# MAGIC Further integration with web and mobile apps is now possible using standard web technolgies. Because the model is hosted in Azure Kubernetes Services, scale is not an issue.  The AKS cluster will automatically react to increased or decreased demand.

# COMMAND ----------

import requests
import json

#this function calls an Azure ML hosted webservice and returns the predicted results
def query_endpoint_example(scoring_uri, inputs, service_key=None):
  headers = {
    "Content-Type": "application/json",
  }
  if service_key is not None:
    headers["Authorization"] = "Bearer {service_key}".format(service_key=service_key)
  
#   print ("Headers: {}") #don't show headers if we are going to save a key or other authentication
  print("Sending batch prediction request with inputs: {}".format(inputs))
  response = requests.post(scoring_uri, data=json.dumps(inputs), headers=headers)
  preds = json.loads(response.text)
  print("Received response: {}".format(preds))
  return preds

#AzureML requires the input data to be in a Pandas data format 
#that has been converted to JSON using the **split** orientation

#grab a record from the test data frame to test calling for a single score
scoring_df = test_df.toPandas().iloc[[18]]
                                                 
query_input = scoring_df.to_json(orient='split')
query_input = eval(query_input)
query_input.pop('index', None)

#collect values needed to call the service
webserviceURI = webservice.scoring_uri
service_auth_key = webservice.get_keys()[0] if len(webservice.get_keys()) > 0 else None #note: there are more secure ways to do this

result = query_endpoint_example(scoring_uri=webserviceURI, inputs=query_input, service_key=service_auth_key)