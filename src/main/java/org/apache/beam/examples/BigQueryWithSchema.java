package org.apache.beam.examples;

import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.io.Serializable;
import java.util.List;

public class BigQueryWithSchema {

    static class Employee implements Serializable {
        final String id;
        final String name;
        final String rating;
        final String dept;
        final String date;

        public Employee(String id, String name, String rating, String dept, String date) {
            this.id = id;
            this.name = name;
            this.rating = rating;
            this.dept = dept;
            this.date = date;
        }
    }

    public interface CustomOptions extends GcpOptions {
        @Description("Big Query Dataset")
        @Validation.Required
        public String getBigQueryDataset();
        public void setBigQueryDataset(String dataset);

        @Description("Table Name")
        @Validation.Required
        public String getBigQueryTable();
        public void setBigQueryTable(String tableName);

    }

    public static void main(String[] args) throws CannotProvideCoderException {
        CustomOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(CustomOptions.class);
        Pipeline pipeline = Pipeline.create(options);


        Schema employeeSchema = Schema.builder()
                .addStringField("id")
                .addStringField("name")
                .addStringField("rating")
                .addStringField("dept")
                .addStringField("date")
                .build();
        CoderRegistry coderRegistry = pipeline.getCoderRegistry();
        coderRegistry.registerCoderForClass(Employee.class, AvroCoder.of(Employee.class));
        PCollection<Employee> employees = pipeline.apply("Read From Big Query",
                BigQueryIO.read(
                        (SchemaAndRecord t) -> {
                            GenericRecord r = t.getRecord();
                            return new Employee(
                                    String.valueOf(r.get("id")),
                                    String.valueOf(r.get("name")),
                                    String.valueOf(r.get("rating")),
                                    String.valueOf(r.get("dept")),
                                    String.valueOf(r.get("date")));
                        }
                ).from(new TableReference()
                        .setTableId(options.getBigQueryTable())
                        .setDatasetId(options.getBigQueryDataset())
                        .setProjectId(options.getProject()))
                .withCoder(SerializableCoder.of(Employee.class))
        );
//        System.out.println(pipeline.getCoderRegistry().getCoder(Employee.class).toString());
//        System.out.println(employees.getCoder().toString());
        employees.setSchema(employeeSchema,
                TypeDescriptor.of(Employee.class),
                employee -> {
                    return Row.withSchema(employeeSchema)
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
                employees.apply("Filter Data", Select.fieldNames("id", "name"))
                .apply("Print Data", ParDo.of(new DoFn<Row, Row>() {
                    @ProcessElement
                    public void process(ProcessContext c){
                        Row e = c.element();
                        System.out.println(e.getString("id") + " : " + e.getString("name"));
                        c.output(e);
                    }
                }));

                pipeline.run();
    }
}
