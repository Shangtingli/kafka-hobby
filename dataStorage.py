import logging
from kafka import KafkaConsumer
import atexit,json
from cassandra.cluster import Cluster

logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('dataStorage')
logger.setLevel(logging.DEBUG)

def persist(stock_data, session, table_name):
    stock_data = json.loads(stock_data)
    data_series = stock_data['Time Series (1min)']
    meta_data = stock_data['Meta Data']
    time_zone = meta_data['6. Time Zone']
    stock_symbol = meta_data['2. Symbol']

    latest_time = list(data_series.keys())[0]
    latest_data = data_series[latest_time]
    openPrice = float(latest_data['1. open'])
    highPrice = float(latest_data['2. high'])
    lowPrice = float(latest_data['3. low'])
    closePrice = float(latest_data['4. close'])
    volume = float(latest_data['5. volume'])

    logger.debug("Start to persist data in cassandra for stock %s", stock_symbol)
    statement = "INSERT INTO %s (stock_symbol, trade_time, trade_price) VALUES ('%s','%s',%f)" % (table_name,stock_symbol, latest_time, openPrice)
    session.execute(statement)
    logger.debug("Persisted data into cassandra cluster")


def shutdown(session, consumer):
    consumer.close()
    session.shutdown()

if __name__ == "__main__":
    topic_name = "stock-analyzer"
    kafka_broker = "192.168.99.104:9092"
    cassandra_broker = ["192.168.99.104"]
    keyspace= "stock"
    table_name = "stock"

    logger.debug("Start Init")
    consumer = KafkaConsumer(topic_name,bootstrap_servers=kafka_broker)
    logger.debug("Kafka Client init success")
    cassandra_cluster = Cluster(contact_points = cassandra_broker)
    session = cassandra_cluster.connect(keyspace)
    logger.debug("Cassandra Cluster, Session Init Successful")

    atexit.register(shutdown,session, consumer)
    for msg in consumer:
        persist(msg.value,session,table_name)

    



    