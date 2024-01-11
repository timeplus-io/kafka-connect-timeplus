package com.timeplus.kafkaconnect;

import okhttp3.Call;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class TimeplusSinkTaskTest {

    @Test
    public void testCreateStream() throws Exception {
        // Setup test parameters
        String workspace = "my_workspace";
        String apiKey = "my_api_key";
        String stream = "my_stream";
        String event = "{\"id\": 123, \"name\": \"test\"}";
        //String createRawPayload = "{\"name\":\"my_stream\",\"columns\":[{\"name\":\"raw\",\"type\":\"string\"}]}";
        String createJSONPayload = "{\"name\":\"my_stream\",\"columns\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"name\",\"type\":\"string\"}]}";

        // Setup mock responses
        Response successCreateStreamResponse = new Response.Builder()
                .request(new Request.Builder().url("https://example.com").build())
                .protocol(Protocol.HTTP_1_1)
                .code(200)
                .message("")
                .body(ResponseBody.create("{\"status\": \"OK\"}", MediaType.parse("application/json")))
                .build();

        Response successInferResponse = new Response.Builder()
                .request(new Request.Builder().url("https://example.com").build())
                .protocol(Protocol.HTTP_1_1)
                .code(200)
                .message("")
                .body(ResponseBody.create("{\"inferred_columns\": []}", MediaType.parse("application/json")))
                .build();

        Call creatCall = mock(Call.class);
        when(creatCall.execute()).thenReturn(successCreateStreamResponse);

        Call inferCall = mock(Call.class);
        when(inferCall.execute()).thenReturn(successInferResponse);

        // Setup test object
        OkHttpClient mockClient = mock(OkHttpClient.class);
        when(mockClient.newCall(any(Request.class))).thenReturn(inferCall, creatCall);

        // Setup test object
        TimeplusSinkTask task = new TimeplusSinkTask();
        task.apiKey = apiKey;
        task.address = "https://example.com";
        task.workspace = workspace;
        task.stream = stream;
        task.dataFormat = "json";
        task.client = mockClient;
        task.streamUrl = task.address + "/" + task.workspace + "/api/v1beta1/streams/";
        task.inferUrl = task.address + "/" + task.workspace + "/api/v1beta1/source/infer";
        task.createPayload = createJSONPayload;

        // Test createStream() with JSON format
        task.createStream(event);

        verify(mockClient, times(2)).newCall(any(Request.class));
        verify(creatCall, times(1)).execute();
        verify(inferCall, times(1)).execute();
    }
}