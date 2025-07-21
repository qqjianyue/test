package com.example;

import com.google.cloud.storage.*;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

// ... existing code ...
public class GcsToGcsV2 {
    private static final Logger LOG = LoggerFactory.getLogger(GcsToGcsV2.class);

    /**
     * Custom PipelineOptions for configuring the job from the command line.
     */
    public interface GcsToGcsOptions extends PipelineOptions {
        @Description("The Cloud Pub/Sub subscription to read from.")
        @Required
        String getPubSubSubscription();
        void setPubSubSubscription(String value);

        @Description("The destination GCS bucket to copy files to (e.g., 'my-dest-bucket').")
        @Required
        String getDestBucket();
        void setDestBucket(String value);
    }

    public static void main(String[] args) {
        // First create the options using the custom interface
        GcsToGcsOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(GcsToGcsOptions.class);

        Pipeline pipeline = Pipeline.create(options); // Use the same options object
//        DataflowPipelineDebugOptions.UnboundedReaderMaxReadTimeFactory f;
        pipeline
                .apply("Read From Pub/Sub", PubsubIO.readMessagesWithAttributesAndMessageIdAndOrderingKey()
                        .withIdAttribute("messageId")
                        .fromSubscription(options.getPubSubSubscription()))
                .apply("Process Messages", ParDo.of(new GcsCopyFn(options.getDestBucket())))
                .apply("Window into", Window.<KV<String, String>>into(FixedWindows.of(Duration.standardSeconds(30L))))
                .apply("Group by key", GroupByKey.create())
                .apply("Final Process", ParDo.of(new DoFn<KV<String, Iterable<String>>, Void>() {
                     @ProcessElement
                     public void processElement(@Element KV<String, Iterable<String>> message, ProcessContext c) {
                         Objects.requireNonNull(message.getValue()).iterator().forEachRemaining(messageId -> {
                             LOG.info("Processed record: {} and messageId: {}", message.getKey(), messageId);
                         });
                     }
                }));


        pipeline.run().waitUntilFinish();
    }

    /**
     * A DoFn for processing Pub/Sub messages, parsing them, and copying GCS objects.
     */
    static class GcsCopyFn extends DoFn<PubsubMessage, KV<String, String>> {
        private final String destBucket;
        private transient Storage storageClient;
        private transient Gson gson;

        // Metrics to count successes and failures across all workers
        private final Counter successCounter = Metrics.counter(GcsCopyFn.class, "gcs-copy-successes");
        private final Counter failureCounter = Metrics.counter(GcsCopyFn.class, "gcs-copy-failures");
        private final Counter malformedMessageCounter = Metrics.counter(GcsCopyFn.class, "malformed-messages");

        private final AtomicLong count = new AtomicLong(0);

        public GcsCopyFn(String destBucket) {
            this.destBucket = destBucket;
        }

        @Setup
        public void setup() {
            // Initialize clients once per DoFn instance, not per element.
            storageClient = StorageOptions.getDefaultInstance().getService();
            gson = new Gson();
        }

        @ProcessElement
        public void processElement(@Element PubsubMessage message, ProcessContext c) {
            String messageId = message.getMessageId();

            LOG.info("Processing message ID: {}", message.getMessageId());
            LOG.info("Processing bucket ID: {}", message.getAttribute("bucketId"));
            LOG.info("Processing object ID: {}", message.getAttribute("objectId"));
            String key = message.getAttribute("bucketId") + "/" + message.getAttribute("objectId");
            String messageData = new String(Objects.requireNonNull(message).getPayload(), StandardCharsets.UTF_8);
            LOG.info("Message body: {}", messageData);

            count.incrementAndGet();

            try {
                Map<String, String> gcsEvent = gson.fromJson(messageData, Map.class);

                String sourceBucket = gcsEvent.get("bucket");
                String filePath = gcsEvent.get("name");

                if (sourceBucket == null || filePath == null) {
                    LOG.warn("Malformed GCS event message, missing 'bucket' or 'name': {}", messageData);
                    malformedMessageCounter.inc();
                    return;
                }

                LOG.info("Attempting to copy gs://{}/{} to gs://{}/{}", sourceBucket, filePath, destBucket, filePath);
                copyFile(sourceBucket, filePath);

                // Acknowledge the message only after successful processing
                c.output(KV.of(key, messageId)); // Required for the ParDo to acknowledge
                LOG.info("Successfully processed and acknowledged message ID: {}", messageId);
                successCounter.inc();

            } catch (JsonSyntaxException e) {
                LOG.error("Failed to parse JSON message: {}", messageData, e);
                malformedMessageCounter.inc();
                // Don't acknowledge the message - let it retry
            } catch (Exception e) {
                LOG.error("Failed to process message and copy file for payload: {}", messageData, e);
                // Don't acknowledge the message - let it retry
                failureCounter.inc();
            }
            finally {
                LOG.info("Processed message count {}", count.get());
            }
        }

        private void copyFile(String sourceBucket, String filePath) {
            BlobId sourceBlobId = BlobId.of(sourceBucket, filePath);
            BlobId destBlobId = BlobId.of(destBucket, filePath);

            Blob sourceBlob = storageClient.get(sourceBlobId);
            Blob destBlob = storageClient.get(destBlobId);

            if (sourceBlob == null) {
                LOG.info("Source file gs://{}/{} does not exist. Skipping copy.", sourceBucket, filePath);
                return;
            }
            // Skip copy if the file exists in the destination and is already the same
            if (destBlob != null && sourceBlob != null &&
                    sourceBlob.getSize() == destBlob.getSize() &&
                    sourceBlob.getMd5().equals(destBlob.getMd5())) {
                LOG.info("File gs://{}/{} already exists in destination with same content. Skipping copy.",
                        sourceBucket, filePath);
                return;
            }

            // Proceed with copy if files differ
            Storage.CopyRequest copyRequest = Storage.CopyRequest.of(sourceBlobId, destBlobId);
            storageClient.copy(copyRequest);

            LOG.info("Successfully copied file from gs://{}/{} to gs://{}/{}",
                    sourceBucket, filePath, destBucket, filePath);
        }

    }
}
