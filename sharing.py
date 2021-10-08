from airflow.models import DAG
from datetime import datetime

from airflow.sensors.filesystem import FileSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.http.sensors.http import HttpSensor

default_args = {
    'start_date': datetime(2020,1,1)
}

with DAG('sharing_example', schedule_interval='*/5 * * * *', default_args=default_args, catchup=False) as dag:
    start_task = DummyOperator(task_id='start')
    file_sensor = FileSensor(task_id='file_sensor_task', poke_interval=30, filepath='/tmp/sharing/')
    move_to_archive = BashOperator(task_id='move_to_archive', bash_command='echo "mv something"')
    is_api_available = HttpSensor(task_id='is_reference_api_available', http_conn_id='reference_api', endpoint='api/', method='GET')
    clean_and_processing_with_spark = BashOperator(task_id='clean_and_process_with_spark', bash_command='echo "spark-submit clean_and_enrich_job"')
    train_model1 = BashOperator(task_id='train_model_1', bash_command='echo "spark-submit train_with_spark_mlib_job --model xyz')
    train_model2 = BashOperator(task_id='train_model_2', bash_command='echo "spark-submit train_with_spark_mlib_job --model abc')
    train_model3 = BashOperator(task_id='train_model_3', bash_command='echo "spark-submit train_with_spark_mlib_job --model mno')
    select_best_accuracy = DummyOperator(task_id='select_best_accuracy')
    end_task = DummyOperator(task_id='end')

start_task >> file_sensor >> move_to_archive
move_to_archive >> is_api_available >> clean_and_processing_with_spark
move_to_archive >> clean_and_processing_with_spark
clean_and_processing_with_spark >> train_model1 >> select_best_accuracy
clean_and_processing_with_spark >> train_model2 >> select_best_accuracy
clean_and_processing_with_spark >> train_model3 >> select_best_accuracy
select_best_accuracy >> end_task