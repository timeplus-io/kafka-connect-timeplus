package com.timeplus.kafkaconnect;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.types.Password;

import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;

/*
Timeplus Sink Config
*/
public class TimeplusSinkConnectorConfig extends AbstractConfig {

    static final String SINK_ADDRESS_CONFIG = "timeplus.sink.address";
    private static final String SINK_ADDRESS_DOC = "The address of Timeplus Cloud";
    private static final String SINK_ADDRESS_DISPLAY = "Timeplus Address";
    private static final String SINK_ADDRESS_DEFAULT = "https://dev.timeplus.cloud";

    static final String SINK_WORKSPACE_CONFIG = "timeplus.sink.workspace";
    private static final String SINK_WORKSPACE_DOC = "The workspace of Timeplus Cloud";
    private static final String SINK_WORKSPACE_DISPLAY = "Timeplus Workspace";
    private static final String SINK_WORKSPACE_DEFAULT = "default";

    static final String SINK_APIKEY_CONFIG = "timeplus.sink.apikey";
    private static final String SINK_APIKEY_DOC = "The apikey of Timeplus Cloud";
    private static final String SINK_APIKEY_DISPLAY = "Timeplus APIKey";
    private static final String SINK_APIKEY_DEFAULT = "";

    static final String SINK_DATA_FORMAT_CONFIG = "timeplus.sink.dataFormat";
    private static final String SINK_DATA_FORMAT_DOC = "The data format, support json or raw";
    private static final String SINK_DATA_FORMAT_DISPLAY = "Data format";
    private static final String SINK_DATA_FORMAT_DEFAULT = "raw";

    static final String SINK_STREAM_CONFIG = "timeplus.sink.stream";
    private static final String SINK_STREAM_DOC = "The stream name";
    private static final String SINK_STREAM_DISPLAY = "Stream Name";

    static final String SINK_CREATE_STREAM_CONFIG = "timeplus.sink.createStream";
    private static final String SINK_CREATE_STREAM_DOC = "whether create a new stream";
    private static final String SINK_CREATE_STREAM_DISPLAY = "Create Stream";
    private static final Boolean SINK_CREATE_STREAM_DEFAULT = false;

    protected TimeplusSinkConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public TimeplusSinkConnectorConfig(Map<String, String> parsedConfig) {
        this(conf(), parsedConfig);
    }

    public static ConfigDef conf() {
        String group = "Timeplus";
        int orderInGroup = 0;
        return new ConfigDef()
                .define(SINK_ADDRESS_CONFIG,
                        Type.STRING,
                        SINK_ADDRESS_DEFAULT,
                        Importance.HIGH,
                        SINK_ADDRESS_DOC,
                        group,
                        ++orderInGroup,
                        ConfigDef.Width.SHORT,
                        SINK_ADDRESS_DISPLAY)
                .define(SINK_WORKSPACE_CONFIG,
                        Type.STRING,
                        SINK_WORKSPACE_DEFAULT,
                        Importance.HIGH,
                        SINK_WORKSPACE_DOC,
                        group,
                        ++orderInGroup,
                        ConfigDef.Width.SHORT,
                        SINK_WORKSPACE_DISPLAY)
                .define(SINK_APIKEY_CONFIG,
                        Type.PASSWORD,
                        SINK_APIKEY_DEFAULT,
                        Importance.HIGH,
                        SINK_APIKEY_DOC,
                        group,
                        ++orderInGroup,
                        ConfigDef.Width.SHORT,
                        SINK_APIKEY_DISPLAY)
                .define(SINK_CREATE_STREAM_CONFIG,
                        Type.BOOLEAN,
                        SINK_CREATE_STREAM_DEFAULT,
                        Importance.HIGH,
                        SINK_CREATE_STREAM_DOC,
                        group,
                        ++orderInGroup,
                        ConfigDef.Width.SHORT,
                        SINK_CREATE_STREAM_DISPLAY)
                .define(SINK_STREAM_CONFIG,
                        Type.STRING,
                        NO_DEFAULT_VALUE,
                        Importance.HIGH,
                        SINK_STREAM_DOC,
                        group,
                        ++orderInGroup,
                        ConfigDef.Width.SHORT,
                        SINK_STREAM_DISPLAY)
                .define(SINK_DATA_FORMAT_CONFIG,
                        Type.STRING,
                        SINK_DATA_FORMAT_DEFAULT,
                        Importance.HIGH,
                        SINK_DATA_FORMAT_DOC,
                        group,
                        ++orderInGroup,
                        ConfigDef.Width.SHORT,
                        SINK_DATA_FORMAT_DISPLAY);
    }

    public String getAddress() {
        return this.getString(SINK_ADDRESS_CONFIG);
    }

    public String getWorkspace() {
        return this.getString(SINK_WORKSPACE_CONFIG);
    }

    public Password getAPIKey() {
        return this.getPassword(SINK_APIKEY_CONFIG);
    }

    public Boolean getCreateStream() {
        return this.getBoolean(SINK_CREATE_STREAM_CONFIG);
    }

    public String getStream() {
        return this.getString(SINK_STREAM_CONFIG);
    }

    public String getDataformat() {
        return this.getString(SINK_DATA_FORMAT_CONFIG);
    }

}
