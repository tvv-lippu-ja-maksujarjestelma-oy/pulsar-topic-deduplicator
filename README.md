# pulsar-topic-deduplicator

Receive messages from an Apache Pulsar topic and send the first of each unique message into another topic, in order.

This operation is similar to compacting a topic but instead of keeping the last occurrence, keep the first occurrence.
It is used to remove duplicated messages that were created by e.g. multiple replicas of [mqtt-pulsar-forwarder](https://github.com/tvv-lippu-ja-maksujarjestelma-oy/mqtt-pulsar-forwarder).

## Development

1. Create a suitable `.env` file for configuration.
   Check below for the configuration reference.
1. Create any necessary secrets that the `.env` file points to.
1. Install dependencies:

   ```sh
   npm install
   ```

1. Run linters and tests and build:

   ```sh
   npm run check-and-build
   ```

1. Load the environment variables:

   ```sh
   set -a
   source .env
   set +a
   ```

1. Run the application:

   ```sh
   npm start
   ```

## Docker

You can use the Docker image `tvvlmj/pulsar-topic-deduplicator:edge`.
Check out [the available tags](https://hub.docker.com/r/tvvlmj/pulsar-topic-deduplicator/tags).

## Configuration

| Environment variable               | Required? | Default value | Description                                                                                                                                                                                                                                            |
| ---------------------------------- | --------- | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `CACHE_WINDOW_IN_SECONDS`          | ❌ No     | `172800`      | How old messages to read from `PULSAR_PRODUCER_TOPIC` to warm up the cache when the service starts, maybe after serious downtime.                                                                                                                      |
| `DEDUPLICATION_IGNORED_PROPERTIES` | ❌ No     |               | A stringified JSON array of strings that names the Pulsar message properties that will be ignored in the deduplication check. E.g. the property `mqttIsDuplicate` from mqtt-pulsar-forwarder should not matter for deduplication.                      |
| `DEDUPLICATION_WINDOW_IN_SECONDS`  | ❌ No     | `3600`        | How old messages should be compared to new messages. It is enough for the cache to hold old enough messages to cover network delays from the duplicated data sources.                                                                                  |
| `HEALTH_CHECK_PORT`                | ❌ No     | `8080`        | Which port to use to respond to health checks.                                                                                                                                                                                                         |
| `PINO_LOG_LEVEL`                   | ❌ No     | `info`        | The level of logging to use. One of "fatal", "error", "warn", "info", "debug", "trace" or "silent".                                                                                                                                                    |
| `PULSAR_BLOCK_IF_QUEUE_FULL`       | ❌ No     | `true`        | Whether the send operations of the producer should block when the outgoing message queue is full. If false, send operations will immediately fail when the queue is full.                                                                              |
| `PULSAR_CACHE_READER_NAME`         | ✅ Yes    |               | The name of the reader for reading messages from `PULSAR_PRODUCER_TOPIC` to fill the initial cache.                                                                                                                                                    |
| `PULSAR_COMPRESSION_TYPE`          | ❌ No     | `ZSTD`        | The compression type to use in the topic where messages are sent. Must be one of `Zlib`, `LZ4`, `ZSTD` or `SNAPPY`.                                                                                                                                    |
| `PULSAR_CONSUMER_TOPICS_PATTERN`   | ✅ Yes    |               | The topic pattern to consume messages from.                                                                                                                                                                                                            |
| `PULSAR_OAUTH2_AUDIENCE`           | ❌ No     |               | Optional OAuth2 audience. If any of the OAuth2 variables are set, all three (`PULSAR_OAUTH2_ISSUER_URL`, `PULSAR_OAUTH2_KEY_PATH`, `PULSAR_OAUTH2_AUDIENCE`) must be set. If none are set, Pulsar authentication is disabled.                          |
| `PULSAR_OAUTH2_ISSUER_URL`         | ❌ No     |               | Optional OAuth2 issuer URL. If any of the OAuth2 variables are set, all three (`PULSAR_OAUTH2_ISSUER_URL`, `PULSAR_OAUTH2_KEY_PATH`, `PULSAR_OAUTH2_AUDIENCE`) must be set. If none are set, Pulsar authentication is disabled.                        |
| `PULSAR_OAUTH2_KEY_PATH`           | ❌ No     |               | Optional path to the OAuth2 private key JSON file. If any of the OAuth2 variables are set, all three (`PULSAR_OAUTH2_ISSUER_URL`, `PULSAR_OAUTH2_KEY_PATH`, `PULSAR_OAUTH2_AUDIENCE`) must be set. If none are set, Pulsar authentication is disabled. |
| `PULSAR_PRODUCER_TOPIC`            | ✅ Yes    |               | The topic to send messages to.                                                                                                                                                                                                                         |
| `PULSAR_SERVICE_URL`               | ✅ Yes    |               | The service URL.                                                                                                                                                                                                                                       |
| `PULSAR_SUBSCRIPTION`              | ✅ Yes    |               | The name of the subscription for reading messages from `PULSAR_CONSUMER_TOPICS_PATTERN`.                                                                                                                                                               |
| `PULSAR_TLS_VALIDATE_HOSTNAME`     | ❌ No     | `true`        | Whether to validate the hostname on its TLS certificate. This option exists because some Apache Pulsar hosting providers cannot handle Apache Pulsar clients setting this to `true`.                                                                   |

Note on Pulsar OAuth2:

- If any of `PULSAR_OAUTH2_ISSUER_URL`, `PULSAR_OAUTH2_KEY_PATH`, or `PULSAR_OAUTH2_AUDIENCE` is set, then all three must be set.
- If none are set, the Pulsar client connects without OAuth2 authentication.
