# Timeplus Sink Connector

A Kafka Connect plugin for Timeplus.

## Build

To build the project, run `mvn clean install`. If you want to include all dependency to simplify your deployment, run `mvn clean compile assembly:single`

## Configurations

The following configurations are supported

| key                        | value                                              |
| -------------------------- | -------------------------------------------------- |
| name                       | ID for the plugin instance                         |
| connector.class            | com.timeplus.kafkaconnect.TimeplusSinkConnector    |
| timeplus.sink.address      | Address to Timeplus Cloud or self-managed Timeplus |
| timeplus.sink.workspace    | Timeplus workspace ID                              |
| timeplus.sink.apikey       | API Key for the workspace, 60 chars long           |
| timeplus.sink.stream       | destination stream name                            |
| timeplus.sink.createStream | whether to create the stream if not exist          |
| timeplus.sink.dataFormat   | stream data format. Either `raw` or `json`         |

Here is an example configuration:

```properties
name: TimeplusSink
connector.class: com.timeplus.kafkaconnect.TimeplusSinkConnector
tasks.max: 1
topics: my_topic
timeplus.sink.address: https://us.timeplus.cloud
timeplus.sink.workspace: abc123
timeplus.sink.apikey: 60_char_API_Key
timeplus.sink.stream: data_from_kafka
timeplus.sink.createStream: true
timeplus.sink.dataFormat: raw
key.converter: org.apache.kafka.connect.storage.StringConverter
value.converter: org.apache.kafka.connect.storage.StringConverter
```

Please check https://docs.timeplus.com/docs/kafka-connect for more details.

## Integration Test

to run integration test, you need have a timeplus cloud account and set following enironment and then run the integration using mvn:

```sh
export TIMEPLUS_API_KEY=<your api key>
export TIMEPLUS_ADDRESS=https://dev.timeplus.cloud
export TIMEPLUS_WORKSPACE=<your workspace name>

mvn test -Dtest=com.timeplus.kafkaconnect.integration.IntegrationTest
```

## Support

please email gang@timeplus.com if you have any question when using this plugin.
