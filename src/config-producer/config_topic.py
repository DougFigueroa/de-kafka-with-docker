"""
This process creates the two kafka topics to be used.
The input-topic with ten partitions and the output-topic with one partition.
Also preloads the kafka cluster with test data (if flag is set to true).
"""
import os
import time
import json
import logging
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer

# defining logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# reading the environement variables defined on the docker compose
KAFKA_CLUSTER = os.environ.get('KAFKA_CLUSTER_CONNECT', 'localhost:9092')
LOAD_DATA = os.environ.get('LOAD_SAMPLE_DATA', False)
logging.info(
    (f'>Env variables: KAFKA_CLUSTER_CONNECT={KAFKA_CLUSTER} '
     f'LOAD_SAMPLE_DATA={LOAD_DATA}'))
BROKER_CONFIG = {'bootstrap.servers': KAFKA_CLUSTER}


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


def create_topics(admin: object, config: dict) -> None:
    """Create the kafka topics based on the configuration file.
    Args:
        - object, the admin client kafka object.
        - dict, json configuration of the process.
    Returns: None.
    """
    # read the topic configuration and create the NewTopic objects
    topics = []
    for k, v in config.items():
        topics.append(NewTopic(
            v['topic_name'],
            num_partitions=v['partitions_quantity'],
            replication_factor=1
            )
        )

    logging.info(f'Starting the creation of the topics: {topics}...')
    creation_response = admin.create_topics(topics)
    # the response has futures (which runs asynchronously) so we validate them
    # to see if they succeeded or not
    for topic, f in creation_response.items():
        try:
            f.result()
            logging.info(f'Creation of the topic {topic} completed.')
        except Exception as e:
            logger.error(f'Error creating the kafka topic: {topic}. {e}')
            raise Exception(f'Error creating the kafka topic: {topic}. {e}')


def list_topics_and_config(admin: object) -> None:
    """Check the topics that exists at a specifid.
    And displays other configs of the Kafka Cluster.
    Args:
        - object, the admin client kafka object.
    Returns: None.
    """
    list_response = admin.list_topics(timeout=5)
    # get all the broker info
    logging.info('>Broker details:')
    for counter, broker in enumerate(list_response.brokers.items(), start=1):
        logging.info(f'{counter}-Broker info: {broker}')
    logging.info('>Topics details:')
    # get all the topic names
    for counter, topic_data in enumerate(list_response.topics.items(), start=1):
        logging.info(f'{counter}-Topic info: {topic_data}')


def load_sample_data(topic: str, sample_data: list) -> None:
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
    data_file = 'dummie_data.json'
    time.sleep(5)
    actual_path = os.path.dirname(__file__)
    configuration_path = os.path.join(actual_path, configuration_file)
    data_path = os.path.join(actual_path, data_file)
    config = read_json_file(configuration_path)
    # defining the admin client needed to create topics
    admin = AdminClient(BROKER_CONFIG)
    create_topics(admin, config)
    # this step its only for validation purposes
    list_topics_and_config(admin)
    # start the load of the sample data to the input topic
    if LOAD_DATA:
        in_topic_name = config['in_topic_conf']['topic_name']
        sample_data = read_json_file(data_path)
        load_sample_data(in_topic_name, sample_data)


if __name__ == '__main__':
    main()
