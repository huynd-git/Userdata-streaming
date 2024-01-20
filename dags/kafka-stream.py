from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import json
from kafka import KafkaProducer
import time
import logging
import uuid

default_args = {
    'owner': 'huynd',
    'start_date': datetime(2024, 1, 13, 21, 30)
}

def get_data():
    """
    Collect user information from randomuser.me api
    Output: user's information in json format 
    """
    info = requests.get('https://randomuser.me/api/')
    info = info.json()
    info = info['results'][0]

    return info

def format_data(info):
    """
    Format information collected from get_data() function
    Input: user information in json format
    Output: Python dictionary contains user's information
    """
    data = {}
    data['id'] = str(uuid.uuid4())
    data['first_name'] = info['name']['first']
    data['last_name'] = info['name']['last']
    data['gender'] = info['gender']

    loc = info['location']
    data['address'] = "{0}, {1}, {2}, {3}, {4}".format(loc['street']['number'], loc['street']['name'],
                        loc['city'], loc['state'], loc['country'])
    data['postcode'] = loc['postcode']

    data['email'] = info['email']
    data['username'] = info['login']['username']

    dob = datetime.strptime(info['dob']['date'], "%Y-%m-%dT%H:%M:%S.%fZ")
    data['dob'] = str(datetime.date(dob))

    reg_date = datetime.strptime(info['registered']['date'], "%Y-%m-%dT%H:%M:%S.%fZ")
    data['registered_date'] = str(datetime.date(reg_date))
    data['phone'] = info['phone']
    data['picture'] = info['picture']['thumbnail']

    return data

def stream_data():
    """
    Stream data to kafka broker
    """
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms= 5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60: break
    
        try:
            user_info = get_data()
            user_info = format_data(user_info)

            producer.send('user_created', json.dumps(user_info).encode('utf-8'))
        except Exception as e:
            logging.error('An error occurs: {}'.format(e))
            continue


with DAG('user_info_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )