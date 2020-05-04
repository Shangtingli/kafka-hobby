# Designate Kafka Cluster and Topics to send event
# Designate a ticker, fetch the ticker information every second

import argparse 
from kafka import KafkaProducer
import time
import requests
import logging
import json

APIKEY = "F7CQNT0ORHAIWYUW"
SLEEP_TIME = 20
logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('data-producer')

logger.setLevel(logging.DEBUG)

def getEndpoint(symbol):
    endpoint = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=" + symbol  +"&interval=1min&apikey=" + APIKEY
    return endpoint


def fetch_price(producer, symbol,topic_name):
    """
        Helper function to get stock data and send to Kafka
        @param producer ==> Instance of Kafka producer
        @param symbol ==> The symbol of the stock 
        @return None
    """
    try:
        logger.debug('Start to fetch stock prices for ' + symbol)
        r = requests.get(getEndpoint(symbol))
        logger.debug("Get Stock Info %s ", symbol)
        data = r.json()
        producer.send(topic_name, value=data)
        print(producer.config)
        logger.debug("Sent Stock Price for %s", symbol)

    except KafkaTimeoutError as timeout_error:
        logger.warn("Fail to send the stock information, caused by %s", symbol, topic_name)

    except Exception:
        logger.warn("Fail to get stock price for %s", symbol)
    


if __name__ == "__main__":
    symbol = "AAPL"

    topic_name = "stock-analyzer"
    bootstrap_servers=['192.168.99.104:9092']

    producer = KafkaProducer(
        bootstrap_servers = bootstrap_servers,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    for i in range(1000):
        fetch_price(producer,symbol,topic_name)
        time.sleep(SLEEP_TIME)


