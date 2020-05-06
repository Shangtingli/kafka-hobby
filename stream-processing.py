
from kafka import KafkaProducer
import atexit,logging
import time, json
from kafka.errors import KafkaError
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


"""
.sh : spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.4.5.jar stream-processing.py

"""
logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('stream-processing')
logger.setLevel(logging.DEBUG)

topic = "stock-analyzer"
new_topic = "average-stock-price"
kafka_broker = "192.168.99.104:9092"


def getLatestPrice(record):
    stock_data = json.loads(record[1])
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

    return openPrice


def process(timeobj,rdd):
    num_of_records = rdd.count()
    if num_of_records == 0:
        return
    
    price_sum = rdd.map(lambda record: getLatestPrice(record)).reduce(lambda x,y: x + y)
    average = num_of_records / price_sum

    data = json.dumps({
        "timestamp": time.time(),
        "average": average
    })

    print(data, new_topic)
    logger.debug("Sending Data to new topic")
    kafka_producer.send(new_topic,value = data)
    logger.debug("Finished sending Data to new topic")


    


def shutdown_hook(producer):
    try:
        logger.info('flush pending messages to kafka')
        producer.flush(10)
        logger.info("finish flushing pending messages")
    except KafkaError as kafka_error:
        logger.warn('Failed to flush pending messages to kafka')
    finally:
        try:
            producer.close(10)
        except Exception as e:
            logger.warn("Failed to close the kafka connection")


if __name__ == "__main__":
    sc = SparkContext("local[2]","StockAveragePrice")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc,5)
    directKafkaStream = KafkaUtils.createDirectStream(ssc,[topic],{'metadata.broker.list':kafka_broker})

    directKafkaStream.foreachRDD(process)

    kafka_producer = KafkaProducer(
        bootstrap_servers = kafka_broker,
        value_serializer=lambda x: x.encode('utf-8')
    )
    atexit.register(shutdown_hook,kafka_producer)

    ssc.start()

    ssc.awaitTermination()

