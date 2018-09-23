import time
import urllib2
import re
import re
from bs4 import BeautifulSoup
import json
import datetime
from dateutil.parser import parse
import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer


def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = str(key)
        value_bytes = str(value)
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer
    
def remove_spaces(text):
    return re.sub('\s+',' ',text)


def start_kafka_producer ():
    kafka_producer = connect_kafka_producer()
    stackoverflow_url = 'https://stackoverflow.com/questions/tagged/java?page=%s&sort=newest&pagesize=50'
    prev = None
    d = datetime.timedelta(seconds=1)
    while True:
        
        prev_time = None
        break_loop = False
        
        for i in range(1,11):
            if break_loop:
                break

            url = stackoverflow_url % i

            page = urllib2.urlopen(url)
            soup = BeautifulSoup(page)

            questions = soup.find_all('div', class_="question-summary")

            for question in questions:
                if not question:
                    continue
                obj = {}
                vote_count = remove_spaces(question.find(class_='vote-count-post ').find('strong').text)
                views = remove_spaces(question.find(class_='views ').text)

                summary = question.find(class_='summary')
                ques_a_tag = summary.find('a', class_='question-hyperlink')

                ques, ques_url = ques_a_tag.text, ques_a_tag.get("href")

                ques_desc = remove_spaces(summary.find(class_='excerpt').text)

                user = question.find(class_='user-details').find('a')
                user_name = ""
                user_url = ""

                time_class = question.find(class_='user-action-time')
                time_str = time_class.find(class_='relativetime')['title']

                prev_time = max(prev_time, parse(time_str)) if prev_time else parse(time_str)
                if prev and prev > parse(time_str):
                    break_loop = True
                    break

                if user:
                    user_name = user.text
                    user_url = user.get('href')

                obj['vote_count'] = vote_count
                obj['views'] = views
                obj['ques'] = ques
                obj['ques_url'] = 'https://stackoverflow.com' + ques_url
                obj['ques_desc'] = ques_desc
                obj['user_name'] = user_name
                obj['user_url'] = 'https://stackoverflow.com' + user_url
                publish_message(kafka_producer, 'raw_recipes', 'raw', obj)
            prev = prev_time + d
        time.sleep(300)

start_kafka_producer()
