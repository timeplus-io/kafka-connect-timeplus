package com.timeplus.kafkaconnect;

import java.io.IOException;
import java.time.Duration;

import okhttp3.Call;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.util.Collection;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.json.JSONArray;
import org.json.JSONObject;

import dev.failsafe.RetryPolicy;
import dev.failsafe.okhttp.FailsafeCall;

/*
Timeplus Sink Task
*/
public class TimeplusSinkTask extends SinkTask {
    private static Logger logger = Logger.getLogger(TimeplusSinkTask.class.getName());

    private static final MediaType JSON = MediaType.get("application/json;format=streaming");
    private static final MediaType RAW = MediaType.get("text/plain;format=lines");

    private static final int MAX_RETRY_COUNT = 10;
    private static final int RETRY_DELAY_MILLI = 500;

    private static final String TIMEPLUS_API_VERSION_PREFIX = "/api/v1beta2";

    OkHttpClient client = new OkHttpClient();

    String address;
    String workspace;
    String apiKey;
    String stream;
    String dataFormat;
    MediaType contentType;
    Boolean createStream;

    String ingestUrl;
    String streamUrl;
    String inferUrl;

    String createPayload;
    Boolean streamCreated;

    final private JSONObject RAW_COL = (new JSONObject()).put("name", "raw").put("type", "string");

    @Override
    public String version() {
        return "1.0.5";
    }

    @Override
    public void start(Map<String, String> props) {
        TimeplusSinkConnectorConfig connectorConfig = new TimeplusSinkConnectorConfig(props);
        this.address = connectorConfig.getAddress();
        this.apiKey = connectorConfig.getAPIKey().value();
        this.workspace = connectorConfig.getWorkspace();
        this.stream = connectorConfig.getStream();
        this.dataFormat = connectorConfig.getDataformat();
        this.createStream = connectorConfig.getCreateStream();

        this.streamUrl = address + "/" + workspace + TIMEPLUS_API_VERSION_PREFIX + "/streams/";
        this.ingestUrl = address + "/" + workspace + TIMEPLUS_API_VERSION_PREFIX + "/streams/" + stream + "/ingest";
        this.inferUrl = address + "/" + workspace + TIMEPLUS_API_VERSION_PREFIX + "/source/infer";

        if (this.dataFormat.equals("raw")) {
            this.contentType = RAW;
        } else {
            this.contentType = JSON;
        }

        this.streamCreated = false;
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.size() == 0) {
            return;
        }

        String bodyString;
        StringBuilder sb = new StringBuilder();
        for (SinkRecord record : records) {
            if (!streamCreated && this.createStream) {
                createStream(record.value().toString());
                streamCreated = true; // only create once
            }
            sb.append(record.value()).append("\n");
        }
        bodyString = sb.toString();

        // Define the retry policy
        RetryPolicy<Response> retryPolicy = RetryPolicy.<Response>builder()
                .handleResultIf(response -> response.code() == 429 || response.code() == 500)
                .withDelay(Duration.ofMillis(RETRY_DELAY_MILLI))
                .withMaxRetries(MAX_RETRY_COUNT)
                .build();

        RequestBody body = RequestBody.create(bodyString, contentType);
        Request request = new Request.Builder()
                .url(ingestUrl)
                .addHeader("X-API-KEY", this.apiKey)
                .post(body)
                .build();

        Call call = client.newCall(request);
        FailsafeCall failsafeCall = FailsafeCall.with(retryPolicy).compose(call);

        Response response = null;
        try {
            response = failsafeCall.execute();
            if (!response.isSuccessful()) {
                logger.severe("ingest to timeplus failed " + response.message());
            }
        } catch (IOException e) {
            logger.warning("ingest to post " + e.getMessage());
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

    @Override
    public void stop() {
        logger.info("sink task stopped");
    }

    private String getCreateStreamRawPayload(String stream) {
        JSONObject payload = new JSONObject();
        payload.put("name", stream);

        JSONArray cols = new JSONArray();
        cols.put(RAW_COL);
        payload.put("columns", cols);

        return payload.toString();
    }

    private String getCreateStreamJSONPayload(String stream, String event) {
        JSONArray inferredColums = this.infer(event);
        JSONObject payload = new JSONObject();
        payload.put("name", stream);
        payload.put("columns", inferredColums);
        return payload.toString();
    }

    void createStream(String event) {
        // TODO check if the stream already exist
        if (this.dataFormat.equals("raw")) {
            this.createPayload = getCreateStreamRawPayload(this.stream);
        } else {
            this.createPayload = getCreateStreamJSONPayload(this.stream, event);
        }

        RequestBody body = RequestBody.create(this.createPayload, JSON);
        Request request = new Request.Builder()
                .url(streamUrl)
                .addHeader("X-API-KEY", this.apiKey)
                .post(body)
                .build();

        Response response = null;
        try {
            response = client.newCall(request).execute();
            if (response != null && response.isSuccessful()) {
                logger.info("create stream success " + response.body().string());
            } else {
                logger.warning("create stream failed " + response.body().string());
            }
        } catch (IOException e) {
            logger.warning("create stream failed " + e.getMessage());
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

    private JSONArray infer(String event) {
        JSONObject eventObj = new JSONObject(event);
        JSONObject payload = new JSONObject();
        payload.put("event", eventObj);

        RequestBody body = RequestBody.create(payload.toString(), JSON);
        Request request = new Request.Builder()
                .url(inferUrl)
                .addHeader("X-API-KEY", this.apiKey)
                .post(body)
                .build();

        Response response = null;
        try {
            response = client.newCall(request).execute();
            if (response != null && response.isSuccessful()) {
                String resp = response.body().string(); // can only be called once
                if (resp != null) {
                    JSONObject respObj = new JSONObject(resp);
                    return respObj.getJSONArray("inferred_columns");
                }
            } else {
                logger.warning("infer stream failed " + response.body().string());
            }
        } catch (IOException e) {
            logger.warning("infer stream failed " + e.getMessage());
        } finally {
            if (response != null) {
                response.close();
            }
        }
        return new JSONArray();
    }
}
