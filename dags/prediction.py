from datetime import datetime, timedelta, date
import json
from faker import Faker
import random
import os.path
from random import randint
from airflow import DAG
import pandas as pd
import sqlite3
from airflow.operators.python_operator import PythonOperator

'''
Function that is simulating the prediction
First converts the data to numerical
Then calculates probability and used round function to get the final decision 
Dag running in random intervals
'''
def pred():
    fake = Faker('en_US')
    request_path = os.path.join('./request', 'year=' + str(datetime.now().year), 'month=' + str(datetime.now().month), 'hour=' + str(datetime.now().hour) + '/')
    i = 1
    for filename in os.listdir(request_path):
        with open(os.path.join(request_path, filename)) as f:
            data = json.load(f)
        dataframe_pred = pd.DataFrame.from_dict(data)
        pred_numeric = pd.to_numeric(dataframe_pred)
        with pd.option_context('display.max_rows', None, 'display.max_columns',
                               None):  # more options can be specified also
            print(pred_numeric)
        probability = random.uniform(0, 1)
        prediction = {'request_id': data.request_id, 'insert_timestamp': data.request_timestamp, 'agent_id': data.agent_id, 'probability_of_fraud': probability, 'final_decision_by_agent': round(probability)}
        print(prediction)
        con = sqlite3.connect('prediction.db')
        cur = con.cursor()
        # Create table
        cur.execute('''CREATE TABLE data
                       (request_id text, insert_timestamp text, agent_id text, probability_of_fraud real, final_decision_by_agent real)''')
        cur.execute("INSERT INTO data VALUES (" + prediction.request_id + "," + prediction.insert_timestamp + "," + prediction.agent_id + "," + prediction.probability_of_fraud + "," + prediction.final_decision_by_agent + ")" )
        con.commit()
        con.close()
        i += 1


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2021, 5, 16),
}
random.seed(int(date.today().strftime('%Y%m%d')))
randomCronString="* {} * * *".format(random.randint(0,24))
dag = DAG('prediction', default_args=default_args, schedule_interval=randomCronString)

pred = PythonOperator(
    task_id="prediction",
    python_callable=pred,
    provide_context=False,
    dag=dag
)

pred
