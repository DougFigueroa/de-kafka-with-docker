"""
This process creates the two kafka topics to be used.
The input-topic with ten partitions and the output-topic with one partition.
Also preloads the kafka cluster with test data (if flag is set to true).
"""
import os
import json
import logging
from sqlalchemy import create_engine, select
from confluent_kafka import Consumer, Producer

# defining logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# kafka configs
# reading the environement variables defined on the docker compose
KAFKA_CLUSTER = os.environ.get('KAFKA_CLUSTER_CONNECT', 'localhost:9092')
# to check how many FINISHED MESSAGE SHOULD READ FROM DB
# depending the parallelism of the consumers
CONSUMERS_EXPECTED = int(os.environ.get('KAFKA_CONSUMERS_EXPECTED', '1'))
logging.info(
    (f'>Env variables: KAFKA_CLUSTER_CONNECT={KAFKA_CLUSTER}'))
TOPIC = 'output_topic'
BROKER_CONFIG = {
    'bootstrap.servers': KAFKA_CLUSTER
}
POLLING_TIMEOUT = 2  # seconds

# DB configs
# this should be read from vault or some secrets manager**
DB_USER = 'admin'
DB_PASS = 'Admin123@'
DB = 'kafkadb'
HOST = 'localhost'
PORT = '5432'
DB_URL = f'postgresql://{DB_USER}:{DB_PASS}@{HOST}:{PORT}/{DB}'
# how many records retrieve from database to not avoid memory issues
BATCH_SIZE = 10


def daemon_read(table_name: str, column_name: str) -> None:
    """Reads consumers status from postgres table to know when to start
    producing the ordered list of integers.
    Args:
        - table_name: str, postgres table name where status of consumers are store.
        - column_name: str, the column name with the status value.
    Returns: None.
    """
    db = create_engine(DB_URL)
    connection = db.connect()
    query = f'SELECT COUNT(1) count FROM {table_name} WHERE {column_name}=\'FINISHED\';'
    logging.info('Reading the consumers status table...')
    try:
        while True:
            count = int(connection.execute(query).first()[0])
            if count % CONSUMERS_EXPECTED == 0:
                logger.info(
                    'All consumers readed, proceding to load to output_topic...')
                read_data_from_database('messages', 'number')
                break
            if not count:
                break
    except Exception as e:
        logging.error(f'Failed read the status of consumers. {e}')
    except KeyboardInterrupt as k:
        logging.warning(f'Process interrupted by user. {k}')
    finally:
        connection.close()
        db.dispose()


def read_data_from_database(table_name: str, column_name: str) -> None:
    """Reads messages data store on postgres and sorts it in ascending order.
    Args:
        - table_name: str, postgres table name where data is store.
        - column_name: str, the column name where the value of the topic is.
    Returns: None.
    """
    db = create_engine(DB_URL)
    connection = db.connect()
    query = f'SELECT {column_name} FROM {table_name} ORDER BY {column_name};'
    # run the query but set the cursor at server level
    proxy = connection.execution_options(stream_results=True).execute(query)
    logging.info('Reading the messages from Postgres messages table.')

    try:
        while True:
            batch_messages = proxy.fetchmany(BATCH_SIZE)  # 1,000 at a time
            if not batch_messages:
                break
            load_data_output_topic('output_topic', batch_messages)
            # for message in batch_messages:
            #   load_data_output_topic('output_topic', message['number'])
        logging.info('All messsages has been processed')
    except Exception as e:
        logging.error(f'Failed read message from db an loaded to topic. {e}')
    finally:
        connection.close()
        db.dispose()
    return


def acked(err: str, msg: object) -> None:
    """Callback function to notify if a message has been produced or not.
    Args:
        - err: str, the error message.
        - msg: object, the message produced.
    """
    if err:
        logger.error(f'Failed to deliver message: {str(msg)}, {str(err)}')
    else:
        logging.debug(f'Message produced: {str(msg)}')


def load_data_output_topic(topic: str, all_messages: list) -> None:
    """Loads messages to the output_topic in kafka.
    The message will be in a single partition ordered increasingly.
    Args:
        - topic: str, the topic name where the data is going to be loaded.
        - all_messages: list, the data to be loaded to the output topic.
    Returns: None
    """
    producer = Producer(BROKER_CONFIG)

    # iterate through messages
    for data in all_messages:
        try:
            producer.produce(topic, value=str(data[0]), partition=0, callback=acked)
        except Exception as e:
            logger.error(
                f'Producer failed to produce a message to the topic. {e}')
            raise Exception(
                f'Failed to produce a message from Kakfa. {e}')
        producer.poll(0)

    # ensure all the delivery queue has been loaded
    producer.flush()
    logging.info('Data successfully produced and loaded to the output topic.')
    return


def main() -> None:
    """Orchestrates all the process execution.
    From read the data in ascending way on postgres to loaded on the
    output topic. There is a daemon that is going to be running until
    all the configured consumers end writing the data to postgres.
    """
    # run process untill all the consumers have
    daemon_read('consumers_status', 'status')


if __name__ == '__main__':
    main()
