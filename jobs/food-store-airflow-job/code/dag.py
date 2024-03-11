import os
import sys
import pendulum
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.models.param import Param
from airflow.decorators import task
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from _1arqfrefmvkguyzjuxlq0g_.tasks import airflow_pipeline
PROPHECY_RELEASE_TAG = "__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__"

with DAG(
    dag_id = "1arQFrEfmVkGuyzjuXLQ0g_", 
    schedule_interval = "0/5 * * * *", 
    default_args = {"owner" : "Prophecy", "ignore_first_depends_on_past" : True, "do_xcom_push" : True, "pool" : "OJMmpjio"}, 
    start_date = pendulum.today('UTC'), 
    end_date = pendulum.datetime(2024, 4, 1, tz = "UTC"), 
    catchup = True
) as dag:
    airflow_pipeline_op = airflow_pipeline()
