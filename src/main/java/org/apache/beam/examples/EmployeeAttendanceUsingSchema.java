package org.apache.beam.examples;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

import java.io.Serializable;
import java.util.List;

public class EmployeeAttendanceUsingSchema {

    @DefaultCoder(AvroCoder.class)
    private static class Employee {
        String id;
        String name;
        String rating;
        String dept;
        String date;

        public Employee(String id, String name, String rating, String dept, String date) {
            this.id = id;
            this.name = name;
            this.rating = rating;
            this.dept = dept;
            this.date = date;
        }

        @Override
        public String toString() {
            return "EmployeeData{" +
                    ", name='" + name + '\'' +
                    ", rating='" + rating + '\'' +
                    ", dept='" + dept + '\'' +
                    ", date='" + date + '\'' +
                    '}';
        }
    }

//    @DefaultCoder(AvroCoder.class)
    static class EmployeeLocation implements Serializable{
        String id;
        Long phNumber;
        String location;

        public EmployeeLocation(String id, Long phNumber, String location) {
            this.id = id;
            this.phNumber = phNumber;
            this.location = location;
        }
    }
    static class TransformToLocationBean extends DoFn<String, EmployeeLocation>{
        @ProcessElement
        public void process(@Element String row, OutputReceiver<EmployeeLocation> out){
            String[] columns = row.split(",");
            try {
                out.output(new EmployeeLocation(columns[0], Long.parseLong(columns[1]), columns[2]));
            }catch(NumberFormatException e){
                System.out.println("NumberFormatException for Record : " + row);
            }
        }
    }


    static class TransformToBean extends DoFn<String, Employee>{
        Counter counter = Metrics.counter("org.apache.beam.examples.EmployeeAttendanceUsingSchema.TransformToBean", "counter");
        @ProcessElement
        public void process(@Element String row, OutputReceiver<Employee> out){
            String[] columns = row.split(",");
            out.output(new Employee(columns[0],columns[1], columns[2], columns[3], columns[4]));
            counter.inc();
        }
    }
    public interface CustomOptions extends GcpOptions{
        @Description("Big Query Dataset")
        @Validation.Required
        public String getBigQueryDataset();
        public void setBigQueryDataset(String dataset);

        @Description("Table Name")
        @Validation.Required
        public String getBigQueryTable();
        public void setBigQueryTable(String tableName);

    }
    static TableReference getTable(CustomOptions options){
        return new TableReference()
                .setProjectId(options.getProject())
                .setDatasetId(options.getBigQueryDataset())
                .setTableId(options.getBigQueryTable());
    }
    static TableSchema getSchema(){
        return new TableSchema()
                .setFields(ImmutableList.of(
                        new TableFieldSchema()
                                .setName("id")
                                .setType("STRING"),
                        new TableFieldSchema()
                                .setName("name")
                                .setType("STRING"),
                        new TableFieldSchema()
                                .setName("rating")
                                .setType("STRING"),
                        new TableFieldSchema()
                                .setName("dept")
                                .setType("STRING"),
                        new TableFieldSchema()
                                .setName("date")
                                .setType("STRING")
                ));
    }
    static TableReference getLocationTable(){
        return new TableReference()
                .setProjectId("research-poc-274116")
                .setDatasetId("poc_dataset")
                .setTableId("employee_location");
    }
    static TableSchema getLocationSchema(){
        return new TableSchema()
                .setFields(ImmutableList.of(
                        new TableFieldSchema()
                                .setName("id")
                                .setType("STRING"),
                        new TableFieldSchema()
                                .setName("phone")
                                .setType("NUMERIC"),
                        new TableFieldSchema()
                                .setName("location")
                                .setType("STRING")
                ));
    }
    public static void main(String[] args) {
        CustomOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(CustomOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        Schema appSchema = Schema.builder()
                .addStringField("id")
                .addStringField("name")
                .addStringField("rating")
                .addStringField("dept")
                .addStringField("date")
                .build();

        PCollection<Employee> employees = pipeline
                .apply("Read from File", TextIO.read().from("D:\\TouchMeNot\\word-count-beam\\src\\main\\resources\\data_file.txt"))
                .apply("Transform to Beam" , ParDo.of(new TransformToBean()));
        employees.setSchema(appSchema, TypeDescriptor.of(Employee.class), employee -> {
            return Row.withSchema(appSchema)
                    .addValues(
                            employee.id,
                            employee.name,
                            employee.rating,
                            employee.dept,
                            employee.date
                    ).build();
        }, row -> {
            return new Employee(
                    row.getString("id"),
                    row.getString("name"),
                    row.getString("rating"),
                    row.getString("dept"),
                    row.getString("date"));
        });
        employees.apply("Select names", Select.fieldNames("id", "name"))
                .apply("Print Names", ParDo.of(new DoFn<Row, Row>(){
                    @ProcessElement
                    public void process(@Element Row row, OutputReceiver<Row> out){
                        System.out.println(row.getString("id") + ", " + row.getString("name"));
                        out.output(row);
                    }
                }));
        employees.apply("FormatFn", MapElements
                    .into(TypeDescriptor.of(TableRow.class))
                    .via((Employee e) -> new TableRow()
                        .set("id", e.id)
                        .set("name", e.name)
                        .set("rating", e.rating)
                        .set("dept", e.dept)
                        .set("date", e.date)))
                .apply("Insert To BigQuery", BigQueryIO.writeTableRows()
                .to(getTable(options))
                .withSchema(getSchema())
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));


        PCollection<EmployeeLocation> locations = pipeline.apply("Read location file", TextIO.read().from("D:\\TouchMeNot\\word-count-beam\\src\\main\\resources\\location_file.txt"))
                .apply("Transform to bean", ParDo.of(new TransformToLocationBean()));
        locations.apply("FormatFn",
                MapElements
                        .into(TypeDescriptor.of(TableRow.class))
                        .via(
                            (EmployeeLocation e) -> new TableRow()
                            .set("id", e.id)
                            .set("phone", e.phNumber)
                            .set("location", e.location)))
                .apply("Insert To Big Query",
                        BigQueryIO.writeTableRows()
                .withSchema(getLocationSchema())
                .to(getLocationTable())
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));


        pipeline.run();

    }
}
