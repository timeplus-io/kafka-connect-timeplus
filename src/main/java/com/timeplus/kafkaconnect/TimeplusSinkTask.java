package com.timeplus.kafkaconnect;

import java.io.IOException;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.util.Collection;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.json.JSONArray;
import org.json.JSONObject;

/*
Timeplus Sink Task
*/
public class TimeplusSinkTask extends SinkTask {

    private static final MediaType JSON = MediaType.get("application/json;format=streaming");
    private static final MediaType RAW = MediaType.get("text/plain;format=lines");
    private final OkHttpClient client = new OkHttpClient();

    private String address;
    private String workspace;
    private String apiKey;
    private String stream;
    private String dataFormat;
    private MediaType contentType;
    private Boolean createStream;

    private String ingestUrl;
    private String streamUrl;
    private String inferUrl;

    private String createPayload;
    private Boolean streamCreated;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        TimeplusSinkConnectorConfig connectorConfig = new TimeplusSinkConnectorConfig(props);
        this.address = connectorConfig.getAddress();
        this.apiKey = connectorConfig.getAPIKey();
        this.workspace = connectorConfig.getWorkspace();
        this.stream = connectorConfig.getStream();
        this.dataFormat = connectorConfig.getDataformat();
        this.createStream = connectorConfig.getCreateStream();

        this.streamUrl = address + "/" + workspace + "/api/v1beta1/streams/";
        this.ingestUrl = address + "/" + workspace + "/api/v1beta1/streams/" + stream + "/ingest";
        this.inferUrl = address + "/" + workspace + "/api/v1beta1/source/infer";

        if (this.dataFormat.equals("raw")) {
            this.contentType = RAW;
        } else {
            this.contentType = JSON;
        }

        this.streamCreated = false;
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        String bodyString = "";
        for (SinkRecord record : records) {
            if (!streamCreated && this.createStream) {
                createStream(record.value().toString());
                streamCreated = true; // only create once
            }
            bodyString = bodyString + record.value() + "\n";
        }

        RequestBody body = RequestBody.create(bodyString, contentType);
        Request request = new Request.Builder()
                .url(ingestUrl)
                .addHeader("X-API-KEY", this.apiKey)
                .post(body)
                .build();

        Response response;
        try {
            response = client.newCall(request).execute();
            if (!response.isSuccessful()) {
                System.out.println("ingest failed " + response.body().string());
            }
        } catch (IOException e) {
            System.out.println("ingest to post " + e.getMessage());
        }
    }

    @Override
    public void stop() {
        System.out.println("sink task stopped");
    }

    private String getCreateRowPayload(String stream) {
        JSONObject payload = new JSONObject();
        payload.put("name", stream);

        JSONObject col = new JSONObject();
        col.put("name", "raw");
        col.put("type", "string");

        JSONArray cols = new JSONArray();
        cols.put(col);
        payload.put("columns", cols);

        return payload.toString();
    }

    private String getCreateJSONPayload(String stream, String event) {
        JSONArray inferredColums = this.infer(event);
        System.out.println("inferred colume : " + inferredColums);

        JSONObject payload = new JSONObject();
        payload.put("name", stream);
        payload.put("columns", inferredColums);
        return payload.toString();
    }

    private void createStream(String event) {
        // TODO check if the stream already exist
        if (this.dataFormat.equals("raw")) {
            this.createPayload = getCreateRowPayload(this.stream);
        } else {
            this.createPayload = getCreateJSONPayload(this.stream, event);
        }

        RequestBody body = RequestBody.create(this.createPayload, JSON);
        Request request = new Request.Builder()
                .url(streamUrl)
                .addHeader("X-API-KEY", this.apiKey)
                .post(body)
                .build();

        Response response;
        try {
            response = client.newCall(request).execute();
            if (response.isSuccessful()) {
                System.out.println("create stream success " + response.body().string());
            } else {
                System.out.println("create stream failed " + response.body().string());
            }
        } catch (IOException e) {
            System.out.println("create stream failed " + e.getMessage());
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

        Response response;
        try {
            response = client.newCall(request).execute();
            if (response.isSuccessful()) {
                String resp = response.body().string(); // can only be called once
                System.out.println("infer stream success " + resp);
                JSONObject respObj = new JSONObject(resp);
                return respObj.getJSONArray("inferred_columns");
            } else {
                System.out.println("infer stream failed " + response.body().string());
            }
        } catch (IOException e) {
            System.out.println("infer stream failed " + e.getMessage());
        }
        return new JSONArray();
    }

}
