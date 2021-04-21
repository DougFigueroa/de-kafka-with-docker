"""
This process creates the two kafka topics to be used.
The input-topic with ten partitions and the output-topic with one partition.
Also preloads the kafka cluster with test data (if flag is set to true).
"""
import os
import json
import logging
from sqlalchemy import create_engine
from confluent_kafka import Consumer, Producer

# defining logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Kafka configs
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
MIN_COMMIT_COUNT = 2  # quantity of message before commit offset to kafka
POLLING_LIMIT = 60

# DB configs
DB_USER = 'admin'
DB_PASS = 'Admin123@'  # this should be read from vault or some secrets manager
DB = 'kafkadb'
HOST = 'localhost'
PORT = '5432'
DB_URL = f'postgresql://{DB_USER}:{DB_PASS}@{HOST}:{PORT}/{DB}'


def consume_data(topic: list):
    """Consume the data from the input-topic and write it to the output-topic.
    The data is going to be sorted in an ascending order.
    Args:
        - topic: list, topic names of the kafka cluster to subscribe.
    Returns: None.
    """
    consumer = Consumer(BROKER_CONFIG)
    running = True
    status = 'RUNNING'
    try:
        logging.info(f'Starting consuming messages from {topic}')
        consumer.subscribe(topic)
        msg_count = 0
        waiting_counter = 0
        while running:
            # stop consuming process after the defined number of pollings
            if waiting_counter >= POLLING_LIMIT:
                status = 'FINISHED'
                break
            msg = consumer.poll(timeout=POLLING_TIMEOUT)
            if not msg:
                logger.debug('There is not new messages. Continue...')
                waiting_counter += 1
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(
                        f'Partition readed from topic: {msg.topic()}, '
                        f'partition: {msg.partition()}, offset: {msg.offset()}'
                        )
                    status = 'FINISHED'
                elif msg.error():
                    logger.error(f'Error while polling. {msg.error()}')
                    status = 'FAILED'
                    raise KafkaException(msg.error())
            else:
                message_value = int(msg.value().decode('utf-8'))
                load_message_to_db(message_value)
                msg_count += 1
                if msg_count % MIN_COMMIT_COUNT == 0:
                    consumer.commit(async=False)
    except KeyboardInterrupt as ki:
        logger.warning(
            '>>> Process stopped by the user. Thanks for consuming data. :)')
    finally:
        logging.info('Closing consumer connection to kafka...')
        # Close down consumer to commit final offsets.
        consumer.close()
        load_consumer_status_to_db(status)


def load_message_to_db(message_value: str) -> None:
    """Loads the consumed messages values to a postgres table to not exceed memory.
        Args:
            - message_value: str, the message value consumed from input_topic.
        Returns: None.
    """
    db = create_engine(DB_URL)
    connection = db.connect()
    try:
        connection.execute(
            f'INSERT INTO messages (number) VALUES (\'{message_value}\');')
        logging.debug(
            f'Value: {message_value} successfully loaded to table messages')
    except Exception as e:
        logging.error(f'Failed to insert value: {message_value}. {e}')
    finally:
        connection.close()
        db.dispose()


def load_consumer_status_to_db(status: str) -> None:
    """Loads the consumer status after consuming messages to postgres table.
        Args:
            - status: str, status of the consumer after polling: 
                      FINISHED|FAILED|RUNNING.
        Returns: None.
    """
    db = create_engine(DB_URL)
    connection = db.connect()

    try:
        connection.execute(
            f'INSERT INTO consumers_status (status) VALUES (\'{status}\');')
        logging.info(
            f'Consumer status: {status}. Successfully loaded to table consumers_status')
    except Exception as e:
        logging.error(f'Failed to insert value: {message_value}. {e}')
    finally:
        connection.close()
        db.dispose()


def main() -> None:
    """Orchestrates all the process execution.
    From consuming the data from the input_topic to loaded on postgres.
    """
    # start to consume the data
    topics = ['input_topic']
    consume_data(topics)


if __name__ == '__main__':
    main()
