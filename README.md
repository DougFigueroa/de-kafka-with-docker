# Kafka-assesment
Data Engineering project.

Creates a two node Kafka cluster, with an input and output topic.
And loads the data between the topics using consumers and producers while taking all the precautions needed to ensure the quality of the data and the performance.

Also there is a process to initialize the Kafka cluster by creating the topics and loading sample data.

Uses python and confluent libraries to make it work.


## Step to launch the project
**[IMPORTANT]**Before launch the project, make sure you have installed Docker on your machine.

- Clone the project on some folder on your local machine:
  ```bash
  git clone https://github.com/DougFigueroa/realde-kafka-assesment.git
  ```
- Open a Powershell/bash console and go to the project's folder: 
  ```application-deploy/```

- You have 2 options to run the project:
  - To create a simple consumer to read all the partitions and load the data. First modify the **ENV** variable of the producer to tell that the process should expect only one consumer. This is on the docker-compose file: `docker-compose-complete-app.yaml`. Like this: ```KAFKA_CONSUMERS_EXPECTED=1```

  And then run the command show below: 
  ```bash 
    docker-compose -f docker-compose-complete-app.yaml up
  ```
  - To create a parallel consumer to read all the partitions at the same time (10 consumers) and load the data. Run the command show below: 
  ```bash 
    docker-compose -f docker-compose-complete-app.yaml up --scale consumer=10
  ```

After you finished using the cluster, you can "turn it off" using the command:
```bash 
    docker-compose -f docker-compose-complete-app.yaml down
  ```

*Note1: the images used for Kafka and Zookeeper are from Confluent. Postgres image and the custom images of the consumers and producer are stored on DockerHub on my personal repo: komodoensis29*

*Note2: you can add the flag -d to the docker-compose command to run the container on detached mode*

## Validate if topics were populate as expected
Run the command ```docker ps``` to list all the containers running. And enter to one of the two kafka nodes: **kafka_broker1** or **kafka_broker2**, to do this, run the command: ```docker exec -it <containerid> /bin/bash```.

Once inside the container, you can check the topics using the console consumer provided by kafka:
For input topic:
```bash
kafka-console-consumer --topic input_topic --bootstrap-server 127.0.0.1:19092 --from-beginning
```
For output-topic:
```bash
kafka-console-consumer --topic output_topic --bootstrap-server 127.0.0.1:19092 --from-beginning
```

*Note3: if you can't run the command kafka-console-consumer, check the ip address of the container*