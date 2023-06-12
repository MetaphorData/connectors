FROM python:3.8-slim

COPY . /src

RUN pip install '/src[all]'

RUN rm -rf /src

RUN apt-get clean
RUN apt-get update
RUN apt-get install -y git

CMD ["sh", "-c", "metaphor ${CONNECTOR} ${CONFIG_FILE}"]
