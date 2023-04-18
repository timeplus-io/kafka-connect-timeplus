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

## Support

please email gang@timeplus.com if you have any question when using this plugin.
