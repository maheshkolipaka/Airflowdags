# importing libiaries
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime
import json, urllib.request
import json, pprint, requests, textwrap
from airflow.models.xcom import XCom
# username and password for airbyte
airbyte_username='airbyte'
airbyte_password='airbyte'

# getting the parameters from the API to start the ETL process
def pram_values(ti,**context):
    host= context['dag_run'].conf['host']
    username=context['dag_run'].conf['dbUsername']
    password= context['dag_run'].conf['dbPassword']
    database= context['dag_run'].conf['dbname']
    schema= context['dag_run'].conf['Schema']
    port=context['dag_run'].conf['port']
    tablename= context['dag_run'].conf['tableName']
    ti.xcom_push(key='hostname', value=host)
    ti.xcom_push(key='username', value=username)
    ti.xcom_push(key='password',value=password)
    ti.xcom_push(key='database', value=database)
    ti.xcom_push(key='schema', value=schema)
    ti.xcom_push(key='port', value=int(port))
    ti.xcom_push(key='tablename', value=tablename)
    
# function  to create the source in airbyte
def create_source(ti):
    host = ti.xcom_pull(key='hostname',task_ids='pram_checks')
    username= ti.xcom_pull(key='username',task_ids='pram_checks')
    password= ti.xcom_pull(key='password',task_ids='pram_checks')
    database= ti.xcom_pull(key='database',task_ids='pram_checks')
    schema= ti.xcom_pull(key='schema',task_ids='pram_checks')    
    port = ti.xcom_pull(key='port',task_ids='pram_checks')
    x= requests.post("http://L201572:8000/api/v1/sources/create",auth=(airbyte_username,airbyte_password),json={
    "sourceDefinitionId": "b5ea17b1-f170-46dc-bc31-cc744ca984c1",
    "workspaceId": "8a1291fd-f11c-437d-802a-c5cd1baf9827",
    "connectionConfiguration": {
        "host": host,
        "port": port,
        "schemas": [
            schema
        ],
        "database": database,
        "password": password,
        "username": username,
        "ssl_method": {
            "ssl_method": "unencrypted"
        },
        "tunnel_method": {
            "tunnel_method": "NO_TUNNEL"
        },
        "replication_method": {
            "method": "STANDARD"
        }
    },
    "name": "test_source"
    })
    i=x.json()
    sourceID=i.pop('sourceId')
    return sourceID
# function  to create destination in airbyte
def createdes(ti):
    y=requests.post(url="http://L201572:8000/api/v1/destinations/create",auth=(airbyte_username,airbyte_password),json={
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
    destination=j.pop('destinationId')
    return destination
# function  to create connection between source and destination in airbyte
def connection(ti):
  sourceconnid = ti.xcom_pull(task_ids='createsource')
  desconnid= ti.xcom_pull(task_ids='createdes')
  tablename= ti.xcom_pull(key='tablename',task_ids='pram_checks') 
  z=requests.post(url='http://L201572:8000/api/v1/connections/create',auth=(airbyte_username,airbyte_password),json={
   "name": "sql_sonwfalke",
    "namespaceDefinition": "destination",
    "namespaceFormat": "${SOURCE_NAMESPACE}",
    "prefix": "",
    "sourceId": sourceconnid,
    "destinationId": desconnid,
      "operationIds": [
    "f8ac1b61-0c79-48f1-a724-d7a68ca4a91c"
      ],
    "syncCatalog": {
      "streams": [
       {
        "stream": {
          "name": tablename,
          "jsonSchema": {
          },
          "supportedSyncModes": [
            "full_refresh"
          ],
          "defaultCursorField": [],
          "sourceDefinedPrimaryKey": [],
          "namespace": tablename
        },
        "config": {
          "syncMode": "full_refresh",
          "cursorField": [],
          "destinationSyncMode": "append",
          "primaryKey": [],
          "aliasName": tablename,
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
# function to discover the schema for the given source tables
def discover_schema(ti):
  connid=ti.xcom_pull(task_ids='create_connection')
  sourceconnid = ti.xcom_pull(task_ids='createsource')
  tablenames= ti.xcom_pull(key='tablename',task_ids='pram_checks')
  a=requests.post(url="http://L201572:8000/api/v1/sources/discover_schema", auth=(airbyte_username,airbyte_password),json={
  "sourceId": sourceconnid,
  "connectionId": connid,
  "disable_cache": False
  })
  data=a.json()
  stream = []
  for i in data['catalog']['streams']:
    for ele in tablenames:
        if i['stream']['name']==ele:
            stream.append(i)
  catlogid=data["catalogId"]
  ti.xcom_push(key='stream_schema',value=stream)
  return catlogid
  
# function to update the connection with the discovered schema
def update_conn(ti):
  catlogid= ti.xcom_pull(task_ids='discover_schema')
  connid=ti.xcom_pull(task_ids='create_connection')
  stream=ti.xcom_pull(key='stream_schema',task_ids='discover_schema')
  requests.post(url='http://L201572:8000/api/v1/connections/update', auth=(airbyte_username,airbyte_password),json={
  "connectionId": connid,
  "namespaceDefinition": "destination",
  "namespaceFormat": "${SOURCE_NAMESPACE}",
  "name": "updated_connection",
  "prefix": "",
  "operationIds": [
    "f8ac1b61-0c79-48f1-a724-d7a68ca4a91c"
  ],
  "syncCatalog": {
    "streams": [
      {
        "stream": stream,
        "config": {
          "syncMode": "full_refresh",
          "cursorField": [
          ],
          "destinationSyncMode": "overwrite",
          "primaryKey": [
          ],
          "aliasName": "department",
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
# function to sync the connection to load the data into snowflake
def syncconn(ti):
  connid=ti.xcom_pull(task_ids='create_connection')
  requests.post(url='http://L201572:8000/api/v1/connections/sync',auth=(airbyte_username,airbyte_password),json={
  "connectionId": connid
})
# arguments for the dag
default_args = {
'owner': 'airflow',
'start_date': datetime(2022, 12 ,25, 10, 00, 00),
'concurrency': 1,
'retries': 0,
'provide_context' : True
}
# Dag starting
with DAG('dynamicdags_test',
catchup=False,
default_args=default_args,
#schedule_interval='* * * * *',
schedule_interval=None,
) as dag:
  # creating the tasks for the ETL process
    opr_hello = BashOperator(task_id='Start_operation',bash_command='echo "Staring operation"')

    pramscheck = PythonOperator(task_id = 'pram_checks', python_callable=pram_values, do_xcom_push=True)

    createsource = PythonOperator(task_id='createsource', python_callable= create_source, do_xcom_push=True)

    createdestination = PythonOperator(task_id='createdes',python_callable= createdes, do_xcom_push= True)

    createconnection= PythonOperator(task_id='create_connection', python_callable=connection, do_xcom_push=True)

    dis_schema= PythonOperator(task_id='discover_schema',python_callable=discover_schema,do_xcom_push=True)

    update_connection= PythonOperator(task_id='update_connect', python_callable=update_conn)

    syncconnection = PythonOperator(task_id='sync_connection', python_callable=syncconn)

# task dependencies
opr_hello>>pramscheck>>createsource>>createdestination>>createconnection>>dis_schema>>update_connection>>syncconnection