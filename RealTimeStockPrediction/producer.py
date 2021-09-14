'''
/**********************************************************************************
@Author: Amar Pawar
@Date: 2021-09-13
@Last Modified by: Amar Pawar
@Last Modified time: 2021-09-013
@Title : Producer code to fetch data from alphavantage API using kafka
/**********************************************************************************
'''
from time import sleep
from kafka import KafkaProducer
from alpha_vantage.timeseries import TimeSeries
import json
import sys
import os
from dotenv import load_dotenv
load_dotenv('.env')
from logging_handler import logger

def dataGrabber():
    try:
        ticker = 'IBM'
        keys = os.getenv("KEY")
        time = TimeSeries(key=keys, output_format='json')
        data, metadata = time.get_intraday(symbol=ticker, interval='5min', outputsize='full')
        return data
    except Exception as e:
        logger.info(e)
        sys.exit(1)


def messagePublisher(producerKey, key, data_key):
    keyBytes = bytes(key, encoding='utf-8')
    producerKey.send("stock", json.dumps(data[key]).encode('utf-8'), keyBytes)
    logger.info("Message Published!")


def kafkaProducerConnect():
    try:
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
        return producer
    except Exception as e:
        logger.info(e)


if __name__ == "__main__":
    data = dataGrabber()
    if len(data) > 0:
        kafkaProducer = kafkaProducerConnect()
        for key in sorted(data):
            messagePublisher(kafkaProducer, key, data[key])
            sleep(3)