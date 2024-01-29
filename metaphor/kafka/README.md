# Kafka Connector

This connector extracts technical metadata from Kafka using [Confluent's Python Client](https://github.com/confluentinc/confluent-kafka-python).

## Setup

If [ACL](https://docs.confluent.io/platform/current/security/rbac/authorization-acl-with-mds.html) is enabled, the credentials used by the crawler must be allowed to perform [Describe operation](https://docs.confluent.io/platform/current/kafka/authorization.html#topic-resource-type-operations) on the topics of interest.

## Config File

Create a YAML config file based on the following template.

### Required Configurations

You must specify at least one bootstrap server, i.e. a pair of host and port pointing to a Kafka broker instance. You must also specify a URL for the schema registry.

```yaml
bootstrap_servers:
  - host: <host>
    port: <port>
schema_registry_url: <schema_registry_url>
```

To use HTTP basic authentication for the schema registry, specify the credentials in `schema_regitry_url` using the format `https://<username>:<password>@host:port`.

### Optional Configurations

#### Output Destination

See [Output Config](../common/docs/output.md) for more information.

#### SASL Authentication

You can optionally authenticate against the brokers by adding the following SASL configurations:

```yaml
sasl_config:
  # SASL mechanism, e.g. GSSAPI, PLAIN, SCRAM-SHA-256, etc.
  mechanism: <mechanism>
  
  # SASL username & password for PLAIN, SCRAM-* mechanisms
  username: <username>
  password: <password>
```

Some mechanisms (e.g., `kerberos` & `oauthbearer`) require additional configs that can be specified using `extra_admin_client_config`:

```yaml
extra_admin_client_config:
  sasl.kerberos.service.name: "kafka"
  sasl.kerberos.principal: "kafkaclient"
```

See [https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md) for a complete list.

#### Filtering

You can filter the topics you want to include in the ingested result:

```yaml
filter:
  includes: <set of patterns to include>
  excludes: <set of patterns to exclude>
```

By default the following topics are excluded:

- `_schema`
- `__consumer_offsets`

#### Topic to Schema Subject Mapping

Kafka messages can have key and value schemas defined in the schema registry. There are three strategies to map topics to schema subjects from the schema registry:

| Subject Name Strategy                  | Key Schema Subject | Value Schema Subject |
| :------------------------------------- | :----------------- | :------------------- |
| `TOPIC_RECORD_NAME_STRATEGY` (Default) | `<topic>-key`      | `<topic>-value`      |
| `RECORD_NAME_STRATEGY`                 | `*-key`            | `*-value`            |
| `TOPIC_RECORD_NAME_STRATEGY`           | `<topic>-*-key`    | `<topic>-*-value`    |

where `<topic>` is the topic name, and `*` matches either all strings or a set of values specified in the config.

##### Example: TOPIC_RECORD_NAME_STRATEGY

The following is the default config, which assumes all messages for a topic `topic` have `topic-key` key schema and `topic-value` value schema.

```yaml
default_subject_name_strategy: TOPIC_RECORD_NAME_STRATEGY
```

##### Example: RECORD_NAME_STRATEGY

The following config specificities that topic `topic` to have two types of key-value schemas, `(type1-key, type1-value)` and `(type2-key, type2-value)`:

```yaml
default_subject_name_strategy: RECORD_NAME_STRATEGY
topic_naming_strategies:
  topic:
    records:
      - type1
      - type2
```

##### Example: TOPIC_RECORD_NAME_STRATEGY

This is similar to `RECORD_NAME_STRATEGY`, except the schema subjects are prefixed with the topic name. For example, the following specifies that the topic `topic` to have two types of key-value schemas, `(topic-type1-key, topic-type1-value)` and `(topic-type2-key, topic-type2-value)`

```yaml
default_subject_name_strategy: TOPIC_RECORD_NAME_STRATEGY
topic_naming_strategies:
  topic:
    records:
      - type1
      - type2
```

Instead of explicitly enumerating the type values, you can specify an empty list to match all possible values, i.e. `(topic-*-key, topic-*-value)`:

```yaml
default_subject_name_strategy: TOPIC_RECORD_NAME_STRATEGY
topic_naming_strategies:
  tpoic:
    records: []
```

##### Example: Overriding Strategy for Specific Topics

It is possible to override the subject name strategy for specific topics, e.g.

```yaml
default_subject_name_strategy: RECORD_NAME_STRATEGY
topic_naming_strategies:
  topic1:
    records:
      - type1
      - type2
  topic2:
    override_subject_name_strategy: TOPIC_NAME_STRATEGY
```

The results in the following schemas

- `topic1`: `(type1-key, type1-value)`, `(type2-key, type2-value)`
- `topic2`: `(topic2-key, topic2-value)`

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `kafka` extra.

Run the following command to test the connector locally:

```shell
metaphor kafka <config_file>
```

Manually verify the output after the run finishes.
