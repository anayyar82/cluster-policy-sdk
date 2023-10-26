# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Class to interact with DLT API

# COMMAND ----------

import requests
import time
  
class DltPipeline():
  
  def __init__(self):
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    self.host = ctx.tags().get("browserHostName").get()
    self.token = ctx.apiToken().get()

  def create_pipeline(self, definition: str):
    r = requests.post(
        f'https://{self.host}/api/2.0/pipelines',
        data = definition,
        headers={'Authorization': f'Bearer {self.token}'}
      )

    print(f"Status Code: {r.status_code}, Response: {r.json()}")
    
    return r
  
  def delete_pipeline(self, pipeline_id: int):
    r = requests.delete(
      f'https://{self.host}/api/2.0/pipelines/{pipeline_id}',
      headers={'Authorization': f'Bearer {self.token}'}
    )
    print(f"Status Code: {r.status_code}, Response: {r.json()}")
    
    return r
  
  def list_pipelines(self, filter: str):
    r = requests.get(
        f'https://{self.host}/api/2.0/pipelines',
        data = filter,
        headers={'Authorization': f'Bearer {self.token}'}
      )

    print(f"Status Code: {r.status_code}, Response: {r.json()}")
    
    return r
  
  def get_pipeline_status(self, pipeline_id):
    return requests.get(
      f'https://{self.host}/api/2.0/pipelines/' + pipeline_id,
      headers={'Authorization': f'Bearer {self.token}'}
    )
  
  def wait_until_deleted(self, pipeline_id):
    import time
    timeout = time.time() + 60   # 1 minute
    while True:
      if time.time() > timeout:
        print("delete timeout")
        return
      
      try:
        r = self.get_pipeline_status(pipeline_id)
        
        if r.json()["status"] == "DELETED":
          return
        
        print(f"Waiting for {pipeline_id} to be deleted")
        time.sleep(1)
      except: # pipeline info not available = deleted
        return
      
  def delete_pipelines(self, notebook_path: str):
    filter = json.dumps({
      "filter": f"notebook='{notebook_path}'"
    })
    
    pipelines = self.list_pipelines(filter).json()

    pipeline_ids = []
    if len(pipelines) > 0:
      for status in pipelines["statuses"]:
        pipeline_ids.append(status["pipeline_id"])

    for pipeline_id in pipeline_ids:
      self.delete_pipeline(pipeline_id)
      self.wait_until_deleted(pipeline_id)
      print(f"Deleted pipeline_id: {pipeline_id}")
      
  def get_pipeline_status(self, pipeline_id):
    r = requests.get(
      f'https://{self.host}/api/2.0/pipelines/' + pipeline_id,
      headers={'Authorization': f'Bearer {self.token}'}
    )
    return r
  
  def get_pipeline_events(self, pipeline_id: str):
    return requests.get(
      f'https://{self.host}/api/2.0/pipelines/' + pipeline_id + '/events',
      headers={'Authorization': f'Bearer {self.token}'}
    )  
    
  
  def update_pipeline(self, pipeline_id: str):
    r = requests.post(
      f'https://{self.host}/api/2.0/pipelines/' + pipeline_id + '/updates',
      headers={'Authorization': f'Bearer {self.token}'},
      data = '{ "full_refresh": "true" }'
    )  
    print(f"updating pipeline: {pipeline_id}")
    return r
    
  def check_running_pipeline_status(self, pipeline_id: str, update_id: str):
    curr_state = ''
    while True:
      r_status = self.get_pipeline_status(pipeline_id).json()
      for s in r_status['latest_updates']:
        if s['update_id'] == update_id:
          status = s
          break
      if status['state'] in ('COMPLETED', 'FAILED'):
        print(f"Pipeline: {pipeline_id} finished with state: {status['state']}")
        return None
      elif curr_state != status['state']:
        print(f"Pipeline: {pipeline_id} is running with current state: {status['state']}")        
      curr_state = status['state']
      time.sleep(2)
    


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Setup

# COMMAND ----------

import pyspark.sql.functions as F
import re

app="dlt_demo"

notebook_dir = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().rsplit('/', 1)[0] + "/"
current_user = spark.sql("SELECT current_user()").collect()[0][0]
storage_dir = f"/Users/{current_user}/{app}/storage/"

db = f"""{app}_{re.sub("[^a-zA-Z0-9]", "_", current_user)}_db"""
spark.conf.set("config.db", db)

pipeline_notebook_path = notebook_dir + "01-DLT-Loan-pipeline-SQL"
pipeline_name = f"""{app}_{re.sub("[^a-zA-Z0-9]", "_", current_user)}"""

print(f"""
current_user: {current_user}
storage_dir: {storage_dir}
database: {db}
pipeline_notebook_path: {pipeline_notebook_path}
""")

# COMMAND ----------

spark.sql(f"DROP DATABASE IF EXISTS {db} CASCADE")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create DLT pipeline

# COMMAND ----------

import json
pipeline = DltPipeline()

# "configuration" is an equivalent for spark.conf.set()

definition = json.dumps({
    "name": pipeline_name,
    "allow_duplicate_names": "true",
    "storage": storage_dir,
    "configuration": {
        "pipelines.applyChangesPreviewEnabled": "true"
    },
    "clusters": [
        {
            "label": "default",
            "autoscale": {
                "min_workers": 1,
                "max_workers": 2
            }
        }
    ],
    "libraries": [
        {
            "notebook": {
                "path": pipeline_notebook_path
            }
        }
    ],
    "target": db,
    "continuous": "false",
    "development": "true"
})

pipeline.delete_pipelines(pipeline_notebook_path)
r = pipeline.create_pipeline(definition)

pipeline_id = r.json()["pipeline_id"]
print(f"Created pipeline_id: {pipeline_id}, name: {pipeline_name}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Run DLT pipeline

# COMMAND ----------

try:
  update_id = pipeline.update_pipeline(pipeline_id).json()['update_id']
except KeyError:
  update_id = pipeline.update_pipeline(pipeline_id).json()['message'].split(' ')[3].strip('\'"')

# COMMAND ----------

pipeline.check_running_pipeline_status(pipeline_id, update_id)
