# kafka-connect-timeplus

kafka timeplus sink connector can sync your kafka data into timeplus using kafka connect.

## build

To build the project, run `mvn clean install`, in case you want to include all dependency to simplify your deployment, run `mvn clean compile assembly:single`

## configurations

following configurations are supported

- connector.class: com.timeplus.kafkaconnect.TimeplusSinkConnector
- timeplus.sink.address: timeplus cloud server address
- timeplus.sink.workspace: timeplus workspace name
- timeplus.sink.apikey: timeplus api key
- timeplus.sink.stream: destination stream name
- timeplus.sink.createStream: whether create a new stream
- timeplus.sink.dataFormat: stream data format, support raw and json

here is an example configuration:

```
{
       "connector.class": "com.timeplus.kafkaconnect.TimeplusSinkConnector",
       "tasks.max": 1,
       "topics": "car_live_data",
       "timeplus.sink.address": "https://dev.timeplus.cloud",
       "timeplus.sink.workspace": "tp-demo",
       "timeplus.sink.apikey": "APIKey",
       "timeplus.sink.stream": "car_sink_raw",
       "timeplus.sink.createStream": true,
       "timeplus.sink.dataFormat": "raw", // support raw and json
       "key.converter": "org.apache.kafka.connect.storage.StringConverter",
       "value.converter": "org.apache.kafka.connect.storage.StringConverter"
}
```
