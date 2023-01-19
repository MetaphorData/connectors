FROM python:3.8-slim

COPY . /src

RUN apt-get install default-libmysqlclient-dev
RUN pip install '/src[all]'

RUN rm -rf /src

CMD ["sh", "-c", "metaphor ${CONNECTOR} ${CONFIG_FILE}"]
