FROM python:3.8-slim

RUN mkdir -p /home/app

COPY . /home/app/
RUN pip install -r /home/app/requirements.txt

CMD ["python", "/home/app/config_topic.py"]