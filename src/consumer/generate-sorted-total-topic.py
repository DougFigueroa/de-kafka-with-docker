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
# KAFKA_CLUSTER = os.environ['KAFKA_CLUSTER_CONNECT']
KAFKA_CLUSTER = 'localhost:9092'
BROKER_CONFIG = {'bootstrap.servers': 'localhost:9092',
            'group.id': 1, 'session.timeout.ms': 6000}


def read_json_file(file_route: str) -> dict:
    """
    Read the json configuration file to set topics and partitions.
    Args:
        - str, the route(with name) of the configuration file.
    Returns:
        - dict, with the configurations defined on the json file.
    """
    with open(file_route, 'r') as f:
        config = json.load(f)
        logging.info('JSON file readed.')
    return config



def list_topics_and_config(admin: object) -> None:
    """Check the topics that exists at a specifid.
    And displays other configs of the Kafka Cluster.
    Args:
        - object, the admin client kafka object.
    Returns: None.
    """
    list_response = admin.list_topics(timeout=20)
    # get all the broker info
    logging.info('>Brokers details:')
    for counter, broker in enumerate(list_response.brokers.items(), start=1):
        logging.info(f'{counter}-Broker info: {broker}')
    logging.info('>Topics details:')
    # get all the topic names
    for counter, topic_data in enumerate(list_response.topics.items(), start=1):
        logging.info(f'{counter}-Topic info: {topic_data}')


def consume_data() -> None:
    """Consume the data from the input-topic and write it to the output-topic.
    The data is going to be sorted in an ascending order.
    """
    consumer = Consumer(BROKER_CONFIG)

    messages = consumer.consume(10)
    print('Consumed')
    for m in messages:
        print(m)
    consumer.close()


def load_data_output_topic(topic: str, sample_data: list) -> None:
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
    for data in sample_data:
        for number in data['values']:
            try:
                producer.produce(topic, str(number), None, data['partition'])
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
    configuration_file = 'topic_config.json'
    config = read_json_file(configuration_file)
    # start to consume the data
    consume_data()
    flag = True
    if flag:
        in_topic_name = config['in_topic_conf']['topic_name']
     


if __name__ == '__main__':
    main()
