import os
from confluent_kafka import Consumer
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

def create_mongodb_client():
    mongo_uri = os.environ["mongo.uri"]
    client = MongoClient(mongo_uri)
    db = client[os.environ["mongo.database"]]
    return db[os.environ["mongo.collection"]]

def consume_source_and_insert_mongodb(consumer_config, source_topic, destination_collection):
    # Create Consumer instance
    consumer = Consumer(consumer_config)

    # Subscribe to topic
    consumer.subscribe([source_topic])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                print("Waiting...")
            elif msg.error():
                print("ERROR: {}".format(msg.error()))
            else:
                key = msg.key().decode('utf-8') if msg.key() else None
                value = msg.value().decode('utf-8') if msg.value() else None
                destination_collection.insert_one({
                    'key': key,
                    'value': value,
                    'topic': source_topic
                })
                consumer.commit(message=msg)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

def main():
    consumer_config = {
        # User-specific properties that you must set
        'bootstrap.servers': os.environ["kafka.destination.bootstrap.servers"],
        'security.protocol': os.environ["kafka.destination.security.protocol"],
        'sasl.mechanism':   os.environ["kafka.destination.sasl.mechanism"],
        'sasl.username':    os.environ["kafka.destination.sasl.username"],
        'sasl.password':    os.environ["kafka.destination.sasl.password"],
        'enable.auto.commit': False,
        # Fixed properties
        'group.id': 'get-data-and-insert-mongodb',
        'auto.offset.reset': 'earliest'
    }

    source_topic = os.environ["destination_topic"]
    destination_collection = create_mongodb_client()
    with ThreadPoolExecutor(max_workers=3) as executor:
        for _ in range(3):
            executor.submit(consume_source_and_insert_mongodb,
                consumer_config, source_topic, destination_collection)
    print("Started consuming messages from Kafka and inserting into MongoDB.")

if __name__ == "__main__":
    main()