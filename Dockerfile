FROM python:3.8-slim AS base

# Builder - Dependencies
FROM base AS builder

RUN pip install poetry

COPY pyproject.toml poetry.lock src/
WORKDIR /src

RUN poetry export --all-extras --format=requirements.txt --output=requirements.txt

# Runtime
FROM base AS runtime

RUN apt-get clean
RUN apt-get update
RUN apt-get install -y git build-essential libsasl2-dev

COPY --from=builder /src/requirements.txt /dep/requirements.txt
RUN pip install -r /dep/requirements.txt --no-deps

COPY . src/
RUN pip install '/src[all]'
RUN rm -rf /src

CMD ["sh", "-c", "metaphor ${CONNECTOR} ${CONFIG_FILE}"]
