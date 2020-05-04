from kafka import KafkaConsumer
# from pymongo import MongoClient
from json import loads


if __name__ == "__main__":
    # consumer = KafkaConsumer(
    #     'stock-analyzer',
    #     bootstrap_servers=['192.168.99.104:9092'],
    #     auto_offset_reset='earliest',
    #     enable_auto_commit=True,
    #     group_id='my-group',
    #     value_deserializer=lambda x: loads(x.decode('utf-8')))

    consumer = KafkaConsumer(
        'stock-analyzer',
        bootstrap_servers=['192.168.99.104:9092'])
    
    for message in consumer:
        try:
            message = message.value
            print(message)
            print()
        except:
            print("Kafka consumer cannot process this message")