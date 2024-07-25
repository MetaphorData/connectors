
FROM python:3.8-slim

RUN apt-get clean
RUN apt-get update
RUN apt-get install -y git build-essential libsasl2-dev

COPY ./requirements.txt /src/requirements.txt

RUN pip install -r /src/requirements.txt --no-deps

RUN rm -rf /src

COPY . /src

RUN pip install '/src[all]'

RUN rm -rf /src

CMD ["sh", "-c", "metaphor ${CONNECTOR} ${CONFIG_FILE}"]
