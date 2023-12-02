"""
dag_apod_nikita.py
Automation ETL pipeline
for the apod API

Created by Nikita Roldugins 8/2/2023.
"""


import pendulum
import subprocess
import credentials_nikita
from datetime import datetime, time
from configparser import ConfigParser
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.sensors.time_sensor import TimeSensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


api_key = credentials_nikita.api_key
timezone = pendulum.timezone("Europe/Amsterdam")

config = ConfigParser()
config.read("/home/airflow_usr/scripts/nikita/config.ini")
landing_path = config["test"]["landing_apod"]
base_path = config["test"]["base_apod"]
my_name = config["test"]["name"]

current_datetime = datetime.now()
year = current_datetime.year
month = current_datetime.month
day = current_datetime.day


dag = DAG(
    dag_id="dag_apod_nikita",
    start_date=datetime(2023, 8, 2, tzinfo=timezone),
    schedule_interval="0 12 * * *",
    catchup=False
)


def is_empty(**kwargs):
    """
    Check if data exists for the given layer
    :param kwargs: storage of arguments
    """
    layer = kwargs["layer"]
    if layer == "landing":
        path = f"{landing_path}/ingest_{year}-{month}-{day}/*.json"
    elif layer == "base":
        path = f"{base_path}/year={year}/month={month}/day={day}/*"
    else:
        raise Exception("Key \"layer\" is not given")
    command = f"hdfs dfs -test -e {path}"
    code = subprocess.call(command, shell=True)
    if code != 0:
        raise Exception(f"Missing data in {layer} layer. Code: {code}")


with dag:
    producer = BashOperator(
        task_id="producer",
        bash_command="""python3 /home/airflow_usr/scripts/nikita/producer_nikita.py test apod {0}""".format(api_key),
    )

    wait_5_min = TimeSensor(
        task_id="wait_5_min",
        target_time=time(hour=12, minute=5),
        mode="poke",
        poke_interval=60,
        timeout=500
    )

    consumer = BashOperator(
        task_id="consumer",
        bash_command="""spark-submit /home/airflow_usr/scripts/nikita/consumer_nikita.py test apod""",
    )

    is_empty_landing = PythonOperator(
        task_id="is_empty_landing",
        python_callable=is_empty,
        provide_context=True,
        op_kwargs={"layer": "landing"}
    )

    base_nikita = BashOperator(
        task_id="base_nikita",
        bash_command="""spark-submit /home/airflow_usr/scripts/nikita/base_nikita.py test apod"""
    )

    is_empty_base = PythonOperator(
        task_id="is_empty_base",
        python_callable=is_empty,
        provide_context=True,
        op_kwargs={"layer": "base"}
    )

    apod = BashOperator(
        task_id="apod",
        bash_command="""spark-submit /home/airflow_usr/scripts/nikita/analytical_nikita.py test apod"""
    )

    v_apod_dashboard_las_5_days = BashOperator(
        task_id="v_apod_dashboard_las_5_days",
        bash_command="""spark-submit /home/airflow_usr/scripts/nikita/analytical_nikita.py test v_apod_dashboard_las_5_days"""
    )

    producer >> wait_5_min >> consumer >> \
    is_empty_landing >> base_nikita >> is_empty_base >> \
    [apod, v_apod_dashboard_las_5_days]
