package org.apache.beam.examples;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.examples.common.ExampleBigQueryTableOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.util.Arrays;

public class BigQueryPipeline {

    static class Employee extends DoFn<String, KV<String, Integer>> {

        @ProcessElement
        public void processElement(@Element String row, OutputReceiver<KV<String, Integer>> out ){
            String[] columns = row.split(",");
            if(columns[3].equals("Accounts")){
                out.output(KV.of(columns[1], 1));
            }
        }
    }

    static class FormatRow extends DoFn<KV<String,Integer>, String>{
        @ProcessElement
        public void processElement(ProcessContext c){
            KV<String,Integer> element = c.element();
            String key = element.getKey();
            Integer value = element.getValue();
            c.output(key + ", " + value);
        }
    }
    public interface CustomGCPOptions extends GcpOptions {

        @Description("Path of the input file including its filename prefix.")
        @Validation.Required
        String getInput();
        void setInput(String value);

        @Description("Path of the output file including its filename prefix.")
        @Validation.Required
        String getOutput();
        void setOutput(String value);

        @Description("BigQuery dataset name")
        @Default.String("beam_examples")
        String getBigQueryDataset();

        void setBigQueryDataset(String dataset);

        @Description("BigQuery dataset name")
        @Default.String("default")
        String getBigQueryProject();

        void setBigQueryProject(String dataset);

        @Description("BigQuery table name")
        @Default.InstanceFactory(ExampleBigQueryTableOptions.BigQueryTableFactory.class)
        String getBigQueryTable();

        void setBigQueryTable(String table);

        @Description("BigQuery table schema")
        TableSchema getBigQuerySchema();

        void setBigQuerySchema(TableSchema schema);

        /** Returns the job name as the default BigQuery table name. */
        class BigQueryTableFactory implements DefaultValueFactory<String> {
            @Override
            public String create(PipelineOptions options) {
                return options.getJobName().replace('-', '_');
            }
        }
    }
    static TableReference getReference(String projectId, String datasetId, String tableId){
        return new TableReference()
                .setProjectId(projectId)
                .setDatasetId(datasetId)
                .setTableId(tableId);

    }
    static TableSchema getTableSchema(){
        return new TableSchema()
                .setFields(
                        Arrays.asList(new TableFieldSchema()
                                        .setName("employee_name")
                                        .setType("STRING")
                                        .setMode("REQUIRED"),
                                new TableFieldSchema()
                                        .setName("attendance")
                                        .setMode("Required")
                                        .setType("INTEGER"))
                );
    }
    public static void main(String[] args) {

        CustomGCPOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(CustomGCPOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        System.out.println("Big Query table : "+options.getBigQueryTable());
        pipeline.apply("Read data from txt file", TextIO.read().from(options.getInput()))
                .apply("Split columns", ParDo.of(new EmployeeAttendance.Employee()))
                .apply("Counting", Combine.perKey(Sum.ofIntegers()))
                .apply("Transform to TableRow", MapElements.into(TypeDescriptor.of(TableRow.class))
                .via(
                        (KV<String,Integer> element) ->
                                new TableRow()
                        .set("employee_name", element.getKey())
                        .set("attendance", element.getValue())
                ))
                .apply("Write data to BigQuery", BigQueryIO.writeTableRows()
                .to(getReference(options.getBigQueryProject(), options.getBigQueryDataset(), options.getBigQueryTable()))
                .withSchema(getTableSchema())
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                );
        pipeline.run().waitUntilFinish();
    }
}
