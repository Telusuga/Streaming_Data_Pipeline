!pip install google-cloud-pubsub
!pip install names

from google.cloud import pubsub_v1
import names as n
import random

publisher=pubsub_v1.PublisherClient()
topic_name='projects/dataflow-test-414808/topics/final'

try:
    for i in range(5):
    id=random.randint(1,10)
    cities_in_andhra_pradesh = [
    "Visakhapatnam",
    "Vijayawada",
    "Tirupati",
    "Rajamahendravaram",
    "Nellore",
    "Guntur",
    "Kakinada",
    "Kurnool",
    ]
    name=n.get_full_name()
    d={"id":id,"Name":name,"City":random.choice(cities_in_andhra_pradesh)}
    rec=str(d).encode('utf-8')
    record=publisher.publish(topic_name,rec, origin="python-sample", username="gcp")
    record.result()
    print(f'The records that are pushed to the {topic_name} are {d}')
    
    
except e as Exception:
    print('Failed due to the following error-->',e)
