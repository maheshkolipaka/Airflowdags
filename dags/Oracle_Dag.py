from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime
import json, urllib.request
import json, pprint, requests, textwrap
from airflow.models.xcom import XCom

airbyte_username='airbyte'
airbyte_password='password'

def pram_values(ti,**context):
    host= context['dag_run'].conf['host']
    username=context['dag_run'].conf['username']
    password= context['dag_run'].conf['password']
    database= context['dag_run'].conf['database']
    schema= context['dag_run'].conf['schema']
    port=context['dag_run'].conf['port']
    tablename= context['dag_run'].conf['tablename']
    
    ti.xcom_push(key='hostname', value=host)
    ti.xcom_push(key='username', value=username)
    ti.xcom_push(key='password',value=password)
    ti.xcom_push(key='database', value=database)
    ti.xcom_push(key='schema', value=schema)
    ti.xcom_push(key='port', value=port)
    ti.xcom_push(key='tablename', value=tablename)
    return host,username,password,database,port,schema,tablename

def create_source(ti):
    host = ti.xcom_pull(key='hostname',task_ids='pram_checks')
    username= ti.xcom_pull(key='username',task_ids='pram_checks')
    password= ti.xcom_pull(key='password',task_ids='pram_checks')
    database= ti.xcom_pull(key='database',task_ids='pram_checks')
    schema= ti.xcom_pull(key='schema',task_ids='pram_checks')    
    port = ti.xcom_pull(key='port',task_ids='pram_checks')
    source= requests.post("http://D101862:8000/api/v1/sources/create",auth=(airbyte_username,airbyte_password),json={

       "sourceDefinitionId": "b39a7370-74c3-45a6-ac3a-380d48520a83",
    "workspaceId": "7e74fed5-a423-48e7-ad68-cae331892ca0",
    "connectionConfiguration": {
        "host": host,
        "port": port,
        "password": password,
        "username": username,
        "database":database.lower(),
        "schema":schema.lower(),
        "encryption": {
            "encryption_method": "unencrypted"
        },
        "tunnel_method": {
            "tunnel_method": "NO_TUNNEL"
        },
        "connection_data": {
            "sid": "orcl",
            "connection_type": "sid"
        }
    },
    "name": "Oracle DB"

    })
    source_js = source.json()
    sourceID = source_js.pop("sourceId")
    return sourceID
    

def createdes(ti):
    destination = requests.post(url="http://D101862:8000/api/v1/destinations/create",auth=(airbyte_username,airbyte_password),json={
 "workspaceId": "7e74fed5-a423-48e7-ad68-cae331892ca0",
  "name": "Snowflake_sql_123",
  "destinationDefinitionId": "424892c4-daac-4491-b35d-c6688ba547ba",
  "connectionConfiguration": {
    "host":"vxhlygl-mk95029.snowflakecomputing.com",
    "role":"ACCOUNTADMIN",
    "user": "snowflake",
    "schema": "PUBLIC",
    "database": "AIRBYE_INGEST",
    "username": "Admin",
    "warehouse": "COMPUTE_WH",
    "credentials": {
            "password": "Admin@123",
            "auth_type": "Username and Password"
      },
        "loading_method": {
            "method": "Standard"
      }
      }
    })
    dest_js = destination.json()
    destination = dest_js.pop("destinationId")
    return destination

def connection(ti):
  sourceconnid = ti.xcom_pull(task_ids='createsource')
  destconnid= ti.xcom_pull(task_ids='createdes')
  tablename= ti.xcom_pull(key='tablename',task_ids='pram_checks') 
  connection = requests.post(url='http://D101862:8000/api/v1/connections/create',auth=(airbyte_username,airbyte_password),json={
   "name": "Oracle-Snowflake",
  "namespaceDefinition": "destination",
  "namespaceFormat": "${SOURCE_NAMESPACE}",
  "prefix": "",
  "sourceId": sourceconnid,
  "destinationId": destconnid,
  "operationIds": [
    "fb88d773-0248-466c-b091-ac9ae1a10b00"
  ],
  "syncCatalog": {
    "streams": [
      {
        "stream": {
          "name": tablename.upper(),
          "jsonSchema": {},
          "supportedSyncModes": [
            "full_refresh"
          ],
          
          "defaultCursorField": [ ],
          "sourceDefinedPrimaryKey": [ ],
          "namespace": "sample"
        },
        "config": {
          "syncMode": "full_refresh",
          "cursorField": [ ],
          "destinationSyncMode": "append",
          "primaryKey": [],
          "aliasName": tablename.lower(),
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
  con_js = connection.json()
  
  connID = con_js.pop('connectionId')
  return connID

def discover_schema(ti):

  connid=ti.xcom_pull(task_ids='create_connection')
  sourceconnid = ti.xcom_pull(task_ids='createsource')
  tablename= ti.xcom_pull(key='tablename',task_ids='pram_checks')
  a=requests.post(url="http://D101862:8000/api/v1/sources/discover_schema", auth=(airbyte_username,airbyte_password),json={
  "sourceId": sourceconnid,
  "connectionId": connid,
  "disable_cache": False
  })
  l=a.json()
  catlogid = l.pop('catalogId')
  k = l.pop('catalog')
  for i in range(100):
    stream = k['streams'][i]['stream']['name']
    if stream == tablename.upper():
      break
  pos = i 
  stream = k['streams'][pos]['stream']
  ti.xcom_push(key='stream_schema',value=stream)
  return catlogid

def update_conn(ti):
  catlogid= ti.xcom_pull(task_ids='discover_schema')
  connid=ti.xcom_pull(task_ids='create_connection')
  stream=ti.xcom_pull(key='stream_schema',task_ids='discover_schema')
  requests.post(url='http://D101862:8000/api/v1/connections/update', auth=(airbyte_username,airbyte_password),json={
  "connectionId": connid,
  "namespaceDefinition": "destination",
  "namespaceFormat": "${SOURCE_NAMESPACE}",
  "name": "Oracle-Snowflake",
  "prefix": "",
  "operationIds": [
    "fb88d773-0248-466c-b091-ac9ae1a10b00"
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
          "aliasName": "alias",
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

def syncconn(ti):
  connid=ti.xcom_pull(task_ids='create_connection')
  requests.post(url='http://D101862:8000/api/v1/connections/sync',auth=(airbyte_username,airbyte_password),json={
  "connectionId": connid
})

default_args = {
'owner': 'airflow',
'start_date': datetime(2022, 12 ,25, 10, 00, 00),
'concurrency': 1,
'retries': 0,
'provide_context' : True
}

with DAG('Oracle_Dag_Run',
catchup=False,
default_args=default_args,
#schedule_interval='* * * * *',
schedule_interval=None,
) as dag:

    opr_hello = BashOperator(task_id='Start_operation',bash_command='echo "Staring operation"')

    pramscheck = PythonOperator(task_id = 'pram_checks', python_callable=pram_values, do_xcom_push=True)

    createsource = PythonOperator(task_id='createsource', python_callable= create_source, do_xcom_push=True)

    createdestination = PythonOperator(task_id='createdes',python_callable= createdes, do_xcom_push= True)

    createconnection= PythonOperator(task_id='create_connection', python_callable=connection, do_xcom_push=True)

    dis_schema= PythonOperator(task_id='discover_schema',python_callable=discover_schema,do_xcom_push=True)

    update_connection= PythonOperator(task_id='update_connect', python_callable=update_conn)

    syncconnection = PythonOperator(task_id='sync_connection', python_callable=syncconn)


opr_hello>>pramscheck>>createsource>>createdestination>>createconnection>>dis_schema>>update_connection>>syncconnection