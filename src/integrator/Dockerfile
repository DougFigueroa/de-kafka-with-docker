FROM python:3.6-slim

RUN mkdir -p /home/producer

COPY . /home/producer/

RUN pip install -r /home/producer/requirements.txt

CMD ["python", "/home/producer/kafka_integrator_producer.py"]