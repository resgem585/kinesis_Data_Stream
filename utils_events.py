import pandas as pd 
import json
import random 
import time
import hashlib
import os


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


def get_second_event(initial_event,second_event,third_event,os,cities):
    
    event_2 = random.choice(second_event)
    
    if event_2 == 'HOME':
        
        event_3 = random.choice(third_event)
        
        if event_3 == 'GO_TO_CATEGORY':
            
            last_event = random.choice(event_category)
            final_event =  random.choice(final_user_event)
            
            if final_event == 'PURCHASE' : 

                payment = random.choice(payment_online)
                os_source = random.choice(os)
                city = random.choice(cities)
                status = 'COMPLETED'
                order_type = 'PURCHASE'
            
            elif final_event == 'EXIT_APP' : 
                
                payment = 'Null'
                os_source = random.choice(os)
                city = random.choice(cities)
                status = 'UNCONVERTED'
                order_type = 'USER_VISIT'
        
        elif event_3 == 'EXIT_APP':
            
            payment = 'Null'
            os_source = random.choice(os)
            city = random.choice(cities)
            status = 'UNCONVERTED'
            order_type = 'USER_VISIT'
            last_event = 'HOME'              
    
    else :
        
        
        payment = 'Null'
        os_source = random.choice(os)
        city = random.choice(cities)
        status = 'UNCONVERTED'
        order_type = 'USER_VISIT'
        last_event = 'LAUNCH_APP'
        event_3 = 'Null'
        
    
    return initial_event,event_2,event_3,last_event,os_source,city,order_type,status,payment


def get_coords(city):
    
    if city == 'Bogotá':
        coords = bog_coords
    elif city == 'Bucaramanga':
        coords = buc_coords
    elif city == 'Cali':
        coords = cali_coords
    elif city == 'Barranquilla':
        coords = bar_coords
    elif city == 'Medellín':
        coords = mede_coords
        
    return coords 
    
def create_masive_users(n_users):
    
    users_bank = []
    
    for i in range(n_users):
        
        date = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%%S")
        users_bank.append(str(hashlib.sha256(f"{i} {date}".encode('utf-8')).hexdigest())[:10])
        
    return users_bank