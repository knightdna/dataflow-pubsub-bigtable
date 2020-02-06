package com.electrolux.ecp.dataflow.bigtable;

import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;

import java.lang.reflect.Field;
import java.text.MessageFormat;
import java.util.Map;

public class EventStore {

    private static final String GOOGLE_APPLICATION_CREDENTIALS = "GOOGLE_APPLICATION_CREDENTIALS";
    private static final String SERVICE_ACCOUNT_KEY_PATH = "/path/to/keyfile.json";

    private static final String PROJECT_ID = "gcp-project-id";
    private static final String PUBSUB_SUBSCRIPTION = "pubsub-subscription-name";
    private static final String INSTANCE_ID = "instance-id";
    private static final String TABLE_ID = "table-name";

    public static void main(String[] args) throws Exception {
        setEnv(GOOGLE_APPLICATION_CREDENTIALS, SERVICE_ACCOUNT_KEY_PATH);

        CloudBigtableTableConfiguration bigtableConfig =
                new CloudBigtableTableConfiguration.Builder()
                        .withProjectId(PROJECT_ID)
                        .withInstanceId(INSTANCE_ID)
                        .withTableId(TABLE_ID)
                        .build();

        String[] projectArgs = {
                "--project=" + PROJECT_ID,
                "--gcpTempLocation=gs://ecp-gcp-poc-events/events"
        };

        PipelineOptions options = PipelineOptionsFactory.fromArgs(projectArgs).create();
        options.setRunner(DataflowRunner.class);

        Pipeline p = Pipeline.create(options);

        p.apply(
                PubsubIO.readStrings().fromSubscription(
                        MessageFormat.format("projects/{0}/subscriptions/{1}",
                                PROJECT_ID, PUBSUB_SUBSCRIPTION)))
                .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))))
                .apply(ParDo.of(new RowGenerator()))
                .apply(CloudBigtableIO.writeToTable(bigtableConfig));

        p.run();
    }

    public static void setEnv(String key, String value) {
        try {
            Map<String, String> env = System.getenv();
            Class<?> cl = env.getClass();
            Field field = cl.getDeclaredField("m");
            field.setAccessible(true);
            Map<String, String> writableEnv = (Map<String, String>) field.get(env);
            writableEnv.put(key, value);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to set environment variable", e);
        }
    }

}

