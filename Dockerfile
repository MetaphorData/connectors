FROM python:3.8-slim

COPY . /src

RUN pip install '/src[all]'

RUN rm -rf /src

CMD ["sh", "-c", "python -m ${PY_MODULE} ${CONFIG_FILE}"]
