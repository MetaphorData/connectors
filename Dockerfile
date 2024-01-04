
FROM python:3.8-slim

RUN apt-get clean
RUN apt-get update
RUN apt-get install -y git build-essential libsasl2

COPY . /src

RUN pip install '/src[all]'

RUN rm -rf /src

CMD ["sh", "-c", "metaphor ${CONNECTOR} ${CONFIG_FILE}"]
