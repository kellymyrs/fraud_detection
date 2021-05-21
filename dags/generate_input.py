from datetime import datetime, timedelta, date
import json
from faker import Faker
import random
import os.path
from random import randint
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import uuid

'''
Function that generates the input data creatiing json files with unique identifier
Dag running in random intervals
'''
def gen_input():
    fake = Faker('en_US')
    id = uuid.uuid1()
    request = {'request_id': id.int, 'request_timestamp': datetime.now().timestamp(), 'longtitude': random.uniform(-180.0, 180.0), 'latitude': random.uniform(-90.0, 90.0), 'document_photo_brightness_percent': randint(0, 100), 'is_photo_in_a_photo_selfie': randint(0, 100)}
    path = os.path.join('./request', str(datetime.now().year), str(datetime.now().month), str(datetime.now().hour) + '/')
    if not os.path.exists(path):
        os.makedirs(path)
    filename = path + "request" + str(id.int) + ".json"
    with open(filename, "w") as outfile:
        json.dump(request, outfile)
    print(request)


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2021, 5, 16),
}
random.seed(int(date.today().strftime('%Y%m%d')))
randomCronString="* {} * * *".format(random.randint(0,24))
dag = DAG('generate_input', default_args=default_args, schedule_interval=randomCronString)

gen_input = PythonOperator(
    task_id="generate_input",
    python_callable=gen_input,
    provide_context=False,
    dag=dag
)

gen_input
