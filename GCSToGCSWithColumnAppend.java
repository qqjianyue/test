import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GCSToGCSWithColumnAppend {
    private static final Logger LOG = LoggerFactory.getLogger(GCSToGCSWithColumnAppend.class);

    public interface GCSToGCSOptions extends PipelineOptions {
        String getPubsubSubscription();
        void setPubsubSubscription(String pubsubSubscription);

        String getDestinationBucket();
        void setDestinationBucket(String destinationBucket);
    }

    public static void main(String[] args) {
        GCSToGCSOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(GCSToGCSOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        // Read messages from Pub/Sub
        PCollection<String> pubsubMessages = pipeline.apply(
                "ReadPubSubMessages",
                PubsubIO.readStrings().fromSubscription(options.getPubsubSubscription()));

        // Extract GCS file path from Pub/Sub message and read the file
        PCollection<String> fileContents = pubsubMessages.apply(
                "ExtractFilePathAndReadFile",
                ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(@Element String message, OutputReceiver<String> out) {
                        // Assuming the message is the GCS file path
                        String filePath = message;
                        try {
                            String fileContent = String.join("\n", TextIO.read().from(filePath).expand());
                            out.output(fileContent);
                        } catch (Exception e) {
                            LOG.error("Error reading file: {}", filePath, e);
                        }
                    }
                }));

        // Append a column to each line of the file
        PCollection<String> modifiedContents = fileContents.apply(
                "AppendColumn",
                ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(@Element String content, OutputReceiver<String> out) {
                        String[] lines = content.split("\n");
                        for (String line : lines) {
                            out.output(line + ",new_column_value");
                        }
                    }
                }));

        // Write the modified content to the destination bucket
        modifiedContents.apply(
                "WriteToGCS",
                TextIO.write().to(options.getDestinationBucket() + "/output").withSuffix(".csv").withoutSharding());

        pipeline.run().waitUntilFinish();
    }
}
