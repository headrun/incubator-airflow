from datetime import datetime, timedelta
from airflow import DAG
from airflow.hooks.sqlite_hook import SqliteHook
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowException
from airflow.operators.sensors import ExternalTaskSensor 
from airflow.operators.bash_operator import BashOperator 
from airflow.operators.email_operator import  EmailOperator
from airflow.operators import PythonOperator 
import airflow.models as af_models 
import airflow.operators as af_op

import json
import os, sys
from dateutil import parser
from urllib.request import urlopen

from ConfigParser import SafeConfigParser
_cfg = SafeConfigParser()
_cfg.read('airflow.cfg')

integration_host = _cfg.get('integration_server', 'host')
integration_port = _cfg.get('integration_server', 'port')

def Task_Store(ingestion_id, exchange):
    def Task1(**self):
        url = 'http://%s:%s/ingestions/getnstore_%s_files/?ingid=%s' \
            % (integration_host, integration_port, exchange, ingestion_id)
        urlopen(url).read()
    return Task1

def Task_Validate(ingestion_id, exchange):
    def Task1(**self):
        url = 'http://%s:%s/validations/data_validation/?ingid=%s' \
            % (integration_host, integration_port, ingestion_id)
        urlopen(url).read()
    return Task1

def Task(**self):
    print "Ingestion have completed Successflly!"

default_args = {
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'dagrun_timeout': timedelta(minutes=1),
    'email' : ['praneeth@headrun.com'],
    'email_on_failure': True,
    'email_on_retry': True
}

def get_dynim_dag_list(**self):
    clients_file = open('/root/integration_engine/dags/clients.json', 'r')
    data =json.load(clients_file)
    data = json.loads(data) 
    listofdags = []
    for clientid, client in data.iteritems():
        default_args['owner'] = client['owner']
        startdate = parser.parse(client['start_date'])
        dag = af_models.DAG(dag_id=str(client['name']).replace(' ', '_').replace('&', '_'), start_date = startdate, schedule_interval=client['schedule_time'], default_args=default_args)
        ingestions = client['ingestions']
        for ingid, ingestion in ingestions.iteritems():
            taskid = 'task' + clientid + '_' + ingid
            exchange = ingestion['exchange']
            task_store = Task_Store(ingid, exchange)
            task_validation = Task_Validate(ingid, exchange)

            #tasks
            task1 = af_op.PythonOperator(task_id = ingestion['name'].replace(' ', '_'), provide_context=True, \
                 python_callable=Task, dag=dag)

            #subtasks
            st1 = af_op.PythonOperator(task_id = ingestion['name'].replace(' ', '_') + '_store', provide_context=True, \
                    python_callable=task_store, dag=dag)
            st2 = af_op.PythonOperator(task_id = ingestion['name'].replace(' ', '_') + '_validate', provide_context=True, \
                    python_callable=task_validation, dag=dag)

            st2.set_upstream(st1)
            task1.set_upstream(st2)

        listofdags.append(dag)
    return listofdags
