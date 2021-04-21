FROM python:3.6-slim

RUN mkdir -p /home/consumer

COPY . /home/consumer/

RUN pip install -r /home/consumer/requirements.txt

CMD ["python", "/home/consumer/kafka_consumer.py"]