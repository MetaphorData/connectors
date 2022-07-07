FROM python:3.8-slim

ADD . /src 

RUN pip install '/src[all]'
RUN pip install 'lumigo_opentelemetry'

CMD ["sh", "-c", "python -m ${PY_MODULE} ${CONFIG_FILE}"]
