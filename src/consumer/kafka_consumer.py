"""
This process creates the two kafka topics to be used.
The input-topic with ten partitions and the output-topic with one partition.
Also preloads the kafka cluster with test data (if flag is set to true).
"""
import os
import json
import logging
from confluent_kafka import Consumer, Producer

# defining logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# reading the environement variables defined on the docker compose
KAFKA_CLUSTER = os.environ.get('KAFKA_CLUSTER_CONNECT', 'localhost:9092')
logging.info(
    (f'>Env variables: KAFKA_CLUSTER_CONNECT={KAFKA_CLUSTER}'))

BROKER_CONFIG = {
    'bootstrap.servers': KAFKA_CLUSTER,
    'group.id': "ConsumerGroup1",
    'auto.offset.reset': 'smallest'
}
POLLING_TIMEOUT = 2  # seconds
MIN_COMMIT_COUNT = 2


def consume_data(topic: list) -> list:
    """Consume the data from the input-topic and write it to the output-topic.
    The data is going to be sorted in an ascending order.
    Args:
        - list, list of topic names of the kafka cluster to subscribe.
    Returns: None.
    """
    consumer = Consumer(BROKER_CONFIG)
    running = True
    all_messages = []
    try:
        logging.info(f'Starting consuming messages from {topic}')
        consumer.subscribe(topic)
        msg_count = 0
        total = 0
        while running:
            msg = consumer.poll(timeout=POLLING_TIMEOUT)
            print(msg)
            if not msg:
                logger.info('No message continue.')
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(
                        f'Partition readed from topic: {msg.topic()}, '
                        f'partition: {msg.partition()}, offset: {msg.offset()}'
                        )
                elif msg.error():
                    logger.error(f'Error while polling. {msg.error()}')
                    raise KafkaException(msg.error())
            else:
                message_value = int(msg.value().decode('utf-8'))
                print(type(message_value))
                logging.info(f'Processing message: {message_value}')
                total += message_value
                # add to a list to order the values and loaded to the output topic
                all_messages.append(message_value)
                # load_data_output_topic(msg)
                msg_count += 1
                if msg_count % MIN_COMMIT_COUNT == 2:
                    consumer.commit(async=False)
    except KeyboardInterrupt as ki:
        logger.warning('>>> Process stopped by the user. Thanks for consuming data. :)')
        running = False
        all_messages.sort()
        load_data_output_topic('output_topic', all_messages)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


def acked(err, msg) -> None:
    """
    """
    if err:
        logger.error(f'Failed to deliver message: {str(msg)}, {str(err)}')
    else:
        logging.info(f'Message produced: {str(msg)}')


def load_data_output_topic(topic: str, all_messages: list) -> None:
    """Loads the sample data to the input kafka topic.
    This will load data across 10 different partitions.
    Args:
        - str, the topic name where the data is going to be loaded.
        - list, the sample data to be loaded by the producer across
          all the partitions of the specified topic.
    Returns: None
    """
    producer = Producer(BROKER_CONFIG)

    # iterate through partitions
    for data in all_messages:
        try:
            producer.produce(topic, value=str(number), partition=0, callback=acked)
        except Exception as e:
            logger.error(
                f'Producer failed to produce a message to the topic. {e}')
            raise Exception(
                f'Failed to produce a message from Kakfia. {e}')
        producer.poll(0)

    # ensure all the delivery queue has been loaded
    producer.flush()
    logging.info('Data successfully produced and loaded to the specify topic.')


def main() -> None:
    """Orchestrates all the process execution.
    From configuring the cluster topics to load the sample input data.
    """
    # start to consume the data
    topics = ['input_topic']
    consume_data(topics)


if __name__ == '__main__':
    main()
