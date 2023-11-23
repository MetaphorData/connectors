# Kafka Connector

This connector extracts technical metadata from Kafka using [Confluent's Python Client](https://github.com/confluentinc/confluent-kafka-python).

## Setup

To run a Kafka cluster locally, follow the instructions below:
1. Install [kcat](https://github.com/edenhill/kcat) to interact with the kafka broker.
2. Start a kafka cluster locally via docker-compose:
```shell
docker-compose --file metaphor/kafka/docker-compose.yml up -d
```
3. Create a topic with `kcat`:
```shell
kcat -b localhost:9092 -t {TOPIC} -P
<ctrl-d>
```
4. Create a schema for the topic:
```shell
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data {SCHEMA_STR} http://localhost:8081/subjects/{TOPIC}/versions
```

## Config File

Create a YAML config file based on the following template.

### Required Configurations

You must specify at least one bootstrap server, i.e. a pair of host and port pointing to the Kafka broker instance. You must also specify a URL for the schema registry.

```yaml
servers:
  - host: <host>
    port: <port>
schema_registry_url: <schema_registry_url>
output:
  file:
    directory: <output_directory>
```

See [Output Config](../common/docs/output.md) for more information on `output`.

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `kafka` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```shell
metaphor kafka <config_file>
```

Manually verify the output after the run finishes.