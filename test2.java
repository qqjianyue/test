import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GCSToGCSDynamic {
    private static final Logger LOG = LoggerFactory.getLogger(GCSToGCSDynamic.class);

    public interface GCSToGCSOptions extends PipelineOptions {
        String getPubsubSubscription();
        void setPubsubSubscription(String pubsubSubscription);
    }

    public static void main(String[] args) {
        GCSToGCSOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(GCSToGCSOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        // 1. Read messages from Pub/Sub
        // Each message is expected to be in the format: "gs://source-bucket/source-file,gs://destination-bucket/destination-file"
        PCollection<String> pubsubMessages = pipeline.apply(
                "ReadPubSubMessages",
                PubsubIO.readStrings().fromSubscription(options.getPubsubSubscription()));

        // 2. Process each message to read from source and write to destination
        pubsubMessages.apply(
                "ProcessFiles",
                ParDo.of(new DoFn<String, Void>() {
                    @ProcessElement
                    public void processElement(@Element String message, OutputReceiver<Void> out) {
                        // Split the message into source and destination file paths
                        String[] paths = message.split(",");
                        if (paths.length != 2) {
                            LOG.error("Invalid message format: {}", message);
                            return;
                        }
                        String sourcePath = paths[0];
                        String destinationPath = paths[1];

                        try {
                            // Read from source GCS file
                            PCollection<String> lines = pipeline.apply("ReadFromGCS", TextIO.read().from(sourcePath));

                            //Transform the data (example: convert to uppercase)
                            PCollection<String> upperCaseLines = lines.apply("UpperCase", ParDo.of(new DoFn<String, String>() {
                                @ProcessElement
                                public void processElement(@Element String line, OutputReceiver<String> out) {
                                    out.output(line.toUpperCase());
                                }
                            }));

                            // Write to destination GCS file
                            upperCaseLines.apply("WriteToGCS", TextIO.write().to(destinationPath).withoutSharding());

                            pipeline.run().waitUntilFinish();

                        } catch (Exception e) {
                            LOG.error("Error processing files: source={} destination={}", sourcePath, destinationPath, e);
                        }
                    }
                }));

        pipeline.run().waitUntilFinish();
    }
}
