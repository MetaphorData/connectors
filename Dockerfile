FROM python:3.8-slim

RUN pip install metaphor-connectors[all]

CMD ["sh", "-c", "python -m ${PY_MODULE} ${CONFIG_FILE}"]
