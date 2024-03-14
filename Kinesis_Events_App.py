import pandas as pd
import json
import random 
import time
import hashlib
import os
from datetime import timedelta
import boto3

from utils_events import get_second_event,get_coords,create_masive_users

stream_name='{YOUR_DATA_STREAM}'
region='us-east-1'
KinesisClient = boto3.client('kinesis',region_name=region)


cities = ['Bogotá','Medellín','Cali','Bucaramanga','Barranquilla']
payment_online = ['Credit_card','PSE','Nequi','Daviplata']
os = ['ANDROID','IOS','WEB']
initial_event = 'LAUNCH_APP'
second_event = ['HOME','EXIT_APP','HOME']
third_event = ['GO_TO_CATEGORY','EXIT_APP','GO_TO_CATEGORY','GO_TO_CATEGORY']
event_category = ['LIQUORS','PHARMACY','TECHNOLOGY','ELECTRO_DOMESTIC','BABY','SUPERMARKET']
final_user_event = ['PURCHASE','EXIT_APP','PURCHASE','EXIT_APP','PURCHASE']
bog_coords = (4.6126,-74.0705)
buc_coords = (7.1186,-73.1161)
cali_coords = (3.4400,-76.5197)
bar_coords = (10.9639,-74.7964)
mede_coords = (6.2447,-75.5748)


user_list =  create_masive_users(1000)
x = 0
data_purchase = []

while(x >= 0):
    
    date = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")
    
    event = get_second_event(initial_event,second_event,third_event,os,cities)
    
    
    user_id = random.choices(user_list)[0]
    event_user_1 = event[0]
    event_user_2 = event[1]
    event_user_3 = event[2]
    event_user_4 = event[3]
    event_user_os = event[4]
    event_user_city = event[5]
    event_user_order = event[6]
    event_user_status = event[7]
    event_user_payment = event[8]
    
    purchase = {'USER_ID': user_id, 
            'INITIAL_EVENT' : event_user_1,
            'EVENT_2':event_user_2,
            'EVENT_3':event_user_3,
            'EVENT_OUT' : event_user_4,
            'OS_USER':event_user_os,
            'CITY' : event_user_city,
            'LATITUD' : get_coords(event_user_city)[0],
            'LONGITUD' : get_coords(event_user_city)[1],
            'ORDER_TYPE' : event_user_order,
            'STATUS':event_user_status,
            'PAYMENT_METHOD':event_user_payment,
            'CREATED_AT': date}
    
    record_value =  json.dumps(purchase).encode('utf-8')
    print(record_value)
    records=KinesisClient.put_record(StreamName=stream_name, Data=record_value, PartitionKey='USER_ID') #json.dumps(purchase)
    print("Total data ingested:"+str(x) +",ReqID:"+ records['ResponseMetadata']['RequestId'] + ",HTTPStatusCode:"+ str(records['ResponseMetadata']['HTTPStatusCode']))

    x += 1
    
    time.sleep(random.choice([1,1.5]))