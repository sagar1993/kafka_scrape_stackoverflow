import json
from time import sleep
from kafka import KafkaConsumer

parsed_topic_name = 'raw_recipes'

while True:
    consumer = KafkaConsumer(parsed_topic_name, bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)
    # auto_offset_reset='earliest', 
    for msg in consumer:
        print(msg)
    if consumer is not None:
        consumer.close()
    sleep(10)
