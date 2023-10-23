package com.timeplus.kafkaconnect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

/*
Timeplus Sink Connector
*/
public class TimeplusSinkConnector extends SinkConnector {
    private TimeplusSinkConnectorConfig config;
    private static Logger logger = Logger.getLogger(TimeplusSinkConnector.class.getName());

    @Override
    public String version() {
        return "1.0.4";
    }

    @Override
    public void start(Map<String, String> map) {
        config = new TimeplusSinkConnectorConfig(map);
        String address = config.getAddress();
        logger.info("the timeplus address is " + address);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return TimeplusSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        Map<String, String> taskProps = new HashMap<>(config.originalsStrings());
        List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; ++i) {
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return TimeplusSinkConnectorConfig.conf();
    }

}
