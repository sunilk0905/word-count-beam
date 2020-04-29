package org.apache.beam.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

public class PubSubPipeline {
    public interface PubSubOptions extends GcpOptions{
        @Description("Topic to be read from")
        @Validation.Required
        String getTopic();
        void setTopic(String topic);
    }
    public static void main(String[] args) {
        PubSubOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PubSubOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(PubsubIO.readStrings().fromTopic(options.getTopic()))
        .apply("Write to Console", ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(@Element String row, OutputReceiver<String> out){
                System.out.println(row);
                out.output(row);
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}
