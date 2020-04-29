package org.apache.beam.examples;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;

import java.util.ArrayList;
import java.util.Arrays;

public class EmployeeAttendance {

    static class Employee extends DoFn<String, KV<String, Integer>>{

        @ProcessElement
        public void processElement(@Element String row, OutputReceiver<KV<String, Integer>> out ){
            String[] columns = row.split(",");
            if(columns[3].equals("Accounts")){
                out.output(KV.of(columns[1], 1));
            }
        }
    }

    static class Counting extends DoFn<KV<String,Integer>, String>{
        @ProcessElement
        public void processElement(ProcessContext c){
            KV<String,Integer> element = c.element();
            String key = element.getKey();
            Integer value = element.getValue();
            c.output(key + ", " + value);
        }
    }
    public interface CustomGCPOptions extends PipelineOptions {

        @Description("Path of the input file including its filename prefix.")
        @Required
        String getInput();
        void setInput(String value);

        @Description("Path of the output file including its filename prefix.")
        @Required
        String getOutput();
        void setOutput(String value);
    }

    public static void main(String[] args) {

        CustomGCPOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(CustomGCPOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("Read data from txt file", TextIO.read().from(options.getInput()))
                .apply("Split columns", ParDo.of(new Employee()))
//                .apply("Group By", GroupByKey.<String,String>create())
//                .apply("Counting", ParDo.of(new Counting()))
//                Above 2 transaforms can be acheived by Combine.perKey
                .apply("Counting", Combine.perKey(Sum.ofIntegers()))
                .apply("Transform tp Pcollection", ParDo.of(new Counting()))
                .apply("Write data to file", TextIO.write().to(options.getOutput()).withNumShards(1));
        pipeline.run().waitUntilFinish();
    }

}
