from datetime import datetime, timedelta, date
import json
from faker import Faker
import random
import os.path
from random import randint
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def dec():
    fake = Faker('en_US')
    request_path = os.path.join('./request', str(datetime.now().year), str(datetime.now().month), str(datetime.now().hour) + '/')
    for filename in os.listdir(request_path):
        if "read" not in filename and os.path.isfile(filename):
            with open(os.path.join(request_path, filename)) as f:
                data = json.load(f)
            final_dec = {'request_id': data.request_id, 'timestamp': data.request_timestamp, 'is_fraud_request': random.randint(0, 1)}
            path = os.path.join('./decision', str(datetime.now().year), str(datetime.now().month), str(datetime.now().hour) + '/')
            if not os.path.exists(path):
                os.makedirs(path)
            filename = path + "request.json"
            with open(filename, "w") as outfile:
                json.dump(final_dec, outfile)
            os.rename(os.path.join(request_path, filename), os.path.join(request_path, filename + "read"))
            outfile.close()
            f.close()
            print(final_dec)


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2021, 5, 16),
}
random.seed(int(date.today().strftime('%Y%m%d')))
randomCronString="* {} * * *".format(random.randint(0,24))
dag = DAG('decision', default_args=default_args, schedule_interval=randomCronString)

dec = PythonOperator(
    task_id="decision",
    python_callable=dec,
    provide_context=False,
    dag=dag
)

dec
