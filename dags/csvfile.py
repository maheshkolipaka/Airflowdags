
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime
import requests
from airflow.models.xcom import XCom

username='airbyte'
password='airbyte'
# getting the parameters from API
def pram_values(ti,**context):
  sourcefilename = context['dag_run'].conf['filename']

  ti.xcom_push(key='filename',value=sourcefilename)
# function to create source in airbyte
#It will read the file in s3 bucket
def createSource(ti):
    sourceFileName = ti.xcom_pull(key='filename',task_ids='prams')
    x= requests.post("http://L201572:8000/api/v1/sources/create",auth=(username,password),json={
      "sourceDefinitionId": "778daa7c-feaf-4db6-96f3-70fd645acc77",
      "connectionConfiguration":{
      "url": f"s3://spotlightus/uploadfiles/{sourceFileName}",
      "format": "csv",
      "provider": {
      "storage": "S3",
      "aws_access_key_id": "AKIA4F52A4XRV6BAEPI4",
      "aws_secret_access_key": "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"
      },
    "dataset_name": sourceFileName,
    "reader_options": "{\"sep\":\",\"}"
  },
        "workspaceId": "8a1291fd-f11c-437d-802a-c5cd1baf9827",
        "name": sourceFileName
        })
    i=x.json()
    sourceID=i.pop("sourceId")
    return sourceID
# function to create destination in airbyte
def createdes():
    y=requests.post(url="http://L201572:8000/api/v1/destinations/create",auth=(username,password),json={
  "workspaceId": "8a1291fd-f11c-437d-802a-c5cd1baf9827",
  "name": "Snowfalke_sql",
  "destinationDefinitionId": "424892c4-daac-4491-b35d-c6688ba547ba",
  "connectionConfiguration": {
    "host": "nfyczpe-yq49101.snowflakecomputing.com",
    "role": "ACCOUNTADMIN",
    "schema": "BRONZE_LAYER",
    "database": "SPOTLIGHT",
    "username": "MaheshKolipaka",
    "warehouse": "COMPUTE_WH",
    "credentials": {
      "password": "M@hesh1234"
    },
    "loading_method": {
      "method": "Internal Staging"
    }
    }
    })
    j=y.json()
    destination=j["destinationId"]
    return destination
# function to create connection between source and destiantion in airbyte
def connection(ti):
  sourceconnid = ti.xcom_pull(task_ids='source')
  desconnid= ti.xcom_pull(task_ids='destination')
  sourceFileName = ti.xcom_pull(key='filename',task_ids='prams')
  z=requests.post(url='http://L201572:8000/api/v1/connections/create',auth=(username,password),json={
   "name": "Ingestion_into_bronze",
    "namespaceDefinition": "destination",
    "namespaceFormat": "${SOURCE_NAMESPACE}",
    "prefix": "",
    "sourceId": sourceconnid,
    "destinationId": desconnid,
      "operationIds": [
    "9d24a761-0703-46d6-8948-90c37ed97748"
      ],
    "syncCatalog": {
      "streams": [
       {
        "stream": {
          "name": sourceFileName,
          "jsonSchema": {
          },
          "supportedSyncModes": [
            "full_refresh"
          ],
          "defaultCursorField": [],
          "sourceDefinedPrimaryKey": [],
          "namespace": sourceFileName
        },
        "config": {
          "syncMode": "full_refresh",
          "cursorField": [],
          "destinationSyncMode": "append",
          "primaryKey": [],
          "aliasName": sourceFileName,
          "selected": True,
          "fieldSelectionEnabled": False
         }
        }
      ]
    },
      "scheduleType": "manual",
      "status": "active",
      "geography": "auto",
      "notifySchemaChanges": True,
     "nonBreakingChangesPreference": "ignore"
    })
  k=z.json()
  connID=k.pop('connectionId')
  return connID
# function to discover the schema for the given source in airbyte
def discover_schema(ti):
  connid=ti.xcom_pull(task_ids='create_conncetion')
  sourceconnid = ti.xcom_pull(task_ids='source')
  a=requests.post(url="http://L201572:8000/api/v1/sources/discover_schema", auth=(username,password),json={
  "sourceId": sourceconnid,
  "connectionId": connid,
  "disable_cache": False
  })
  l=a.json()
  catlogid=l['catalogId']
  k=l.pop('catalog')
  stream=k['streams'][0]['stream']
  ti.xcom_push(key='stream_schema',value=stream)
  return catlogid
# function to update the connection with the schema in airbyte
def update_conn(ti):
  catlogid= ti.xcom_pull(task_ids='discover_schema')
  connid=ti.xcom_pull(task_ids='create_conncetion')
  sourceFileName = ti.xcom_pull(key='filename',task_ids='filecheck')
  stream=ti.xcom_pull(key='stream_schema',task_ids='discover_schema')
  requests.post(url='http://L201572:8000/api//v1/connections/update', auth=(username,password),json={
  "connectionId": connid,
  "namespaceDefinition": "destination",
  "namespaceFormat": "${SOURCE_NAMESPACE}",
  "name": "updated",
  "prefix": "",
  "operationIds": [
    "9d24a761-0703-46d6-8948-90c37ed97748"
  ],
  "syncCatalog": {
    "streams": [
      {
        "stream": stream,
        "config": {
          "syncMode": "full_refresh",
          "cursorField": [
          ],
          "destinationSyncMode": "append",
          "primaryKey": [
          ],
          "aliasName": sourceFileName,
          "selected": True,
          "fieldSelectionEnabled": False
        }
      }
    ]
  },
  "scheduleType": "manual",
  "status": "active",
  "sourceCatalogId": catlogid,
  "geography": "auto",
  "notifySchemaChanges": False,
  "nonBreakingChangesPreference": "ignore",
  "breakingChange": False
  }
  )
# sync the connection to load the data from s3 bucket to snowflake
def syncconn(ti):
  connid=ti.xcom_pull(task_ids='create_conncetion')
  requests.post(url='http://L201572:8000/api/v1/connections/sync',auth=(username,password),json={
  "connectionId": connid
})


default_args = {
'owner': 'airflow',
'start_date': datetime(2022, 12 ,25, 10, 00, 00),
'concurrency': 1,
'retries': 0
}

with DAG('Airbyte',
catchup=False,
default_args=default_args,
#schedule_interval='* * * * *',
schedule_interval=None,
) as dag:

    opr_hello = BashOperator(task_id='Start_operation',bash_command='echo "Staring operation"')

    pram_check = PythonOperator(task_id='prams',python_callable=pram_values,do_xcom_push=True)

    source = PythonOperator(task_id='source',python_callable=createSource,do_xcom_push=True)

    destination= PythonOperator(task_id='destination',python_callable=createdes,do_xcom_push=True)

    create_connection= PythonOperator(task_id='create_conncetion',python_callable=connection,do_xcom_push=True)

    dis_schema= PythonOperator(task_id='discover_schema',python_callable=discover_schema,do_xcom_push=True)

    update_connection= PythonOperator(task_id='update_connect', python_callable=update_conn)

    sync_connection= PythonOperator(task_id='sync_connection',python_callable=syncconn)


opr_hello >> pram_check >> source >> destination >> create_connection >> dis_schema >> update_connection >> sync_connection

