import os
from confluent_kafka import Consumer, Producer
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

load_dotenv()


def consume_source_and_produce_destination(consumer_config, producer_config, source_topic, destination_topic):
    # Create Consumer instance
    consumer = Consumer(consumer_config)
    producer = Producer(producer_config)

    # Subscribe to topic
    consumer.subscribe([source_topic])

    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            consumer.commit(message=msg)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                print("Waiting...")
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                key = msg.key().decode('utf-8') if msg.key() else None
                value = msg.value().decode('utf-8') if msg.value() else None
                producer.produce(topic=destination_topic, value=value, key=key, on_delivery=delivery_callback)
                producer.poll(0)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        consumer.close()

def main():
    consumer_config = {
        # User-specific properties that you must set
        'bootstrap.servers': os.environ["kafka.source.bootstrap.servers"],
        'security.protocol': os.environ["kafka.source.security.protocol"],
        'sasl.mechanism':   os.environ["kafka.source.sasl.mechanism"],
        'sasl.username':    os.environ["kafka.source.sasl.username"],
        'sasl.password':    os.environ["kafka.source.sasl.password"],
        'enable.auto.commit': False,
        # Fixed properties
        'group.id': 'get-data-from-kafka-source',
        'auto.offset.reset': 'earliest'
    }

    producer_config = {
        # User-specific properties that you must set
        'bootstrap.servers': os.environ["kafka.destination.bootstrap.servers"],
        'security.protocol': os.environ["kafka.destination.security.protocol"],
        'sasl.mechanism':   os.environ["kafka.destination.sasl.mechanism"],
        'sasl.username':    os.environ["kafka.destination.sasl.username"],
        'sasl.password':    os.environ["kafka.destination.sasl.password"],
        'acks': 'all',
        # Fixed properties
        'client.id': 'produce-data-from-kafka-source-to-destination'
    }
    source_topic = os.environ["source_topic"]
    destionation_topic = os.environ["destination_topic"]
    with ThreadPoolExecutor(max_workers=3) as executor:
        for _ in range(3):
            executor.submit(consume_source_and_produce_destination,
                consumer_config, producer_config, source_topic, destionation_topic)

if __name__ == '__main__':
    main()
