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

#setup connection to the Azure ML workspace
import azureml
from azureml.core import Workspace

workspace_name = ""
workspace_location=""
resource_group = ""
subscription_id = ""

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
image_name = "model"

#retrieve the image configuration
model_image = Image(workspace, name=image_name)

# COMMAND ----------

# #for DEV deployments, Azure Container Instances

from azureml.core.webservice import AciWebservice, Webservice

dev_webservice_name = "skurecs-dev"
dev_webservice_deployment_config = AciWebservice.deploy_configuration()
dev_webservice = Webservice.deploy_from_image(name=dev_webservice_name, image=model_image, deployment_config=dev_webservice_deployment_config, workspace=workspace)

dev_webservice.wait_for_deployment()

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