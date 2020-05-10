package org.apache.beam.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class JDBCPipeline {
    public interface JDBCOptions extends PipelineOptions{
        @Description("Driver Class for SQL Server")
        @Validation.Required
        String getDriverClass();
        void setDriverClass(String driverClass);

        @Description("SQL Connection String")
        @Validation.Required
        String getConnectionString();
        void setConnectionString(String connectionString);

        @Description("SQL Username")
        @Validation.Required
        ValueProvider<String> getUsername();
        void setUsername(ValueProvider<String> username);

        @Description("SQL Password")
        @Validation.Required
        ValueProvider<String> getPassword();
        void setPassword(ValueProvider<String> password);

        @Description("Input flat file")
        @Validation.Required
        String getInput();
        void setInput(String input);
    }

    private static class ExtractFn extends DoFn<String, KV<String,Integer>>{
        @ProcessElement
        public void process(@Element String row, OutputReceiver<KV<String,Integer>> out){
            String[] columns = row.split(",");
            out.output(KV.of(columns[0], 1));
        }
    }
    public static void main(String[] args) {
        JDBCOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(JDBCOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("Read from file", TextIO.read().from(options.getInput()))
                .apply("Extract Ids", ParDo.of(new ExtractFn()))
                .apply("Combine per key", Combine.perKey(Sum.ofIntegers()))
                .apply("write to SQL Server", JdbcIO.<KV<String,Integer>>write().withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                                .create(options.getDriverClass(), options.getConnectionString())
                                .withUsername(options.getUsername())
                                .withPassword(options.getPassword()))
                        .withStatement("INSERT INTO employee_attendance(employee_id,attendance) VALUES(?,?)")
//                        .withStatement("INSERT INTO employee_attendance(employee_id,attendance) VALUES(?,?) " +
//                                "ON CONFLICT ON CONSTRAINT employee_attendance_pkey DO UPDATE SET attendance = EXCLUDED.attendance")
                        .withBatchSize(10)
                        .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<KV<String, Integer>>(){
                            @Override
                            public void setParameters(KV<String, Integer> element, PreparedStatement preparedStatement) throws SQLException {
                                    preparedStatement.setString(1, element.getKey());
                                    preparedStatement.setInt(2, element.getValue());
                            }
                        })
                        .withResults()
                        .withRetryStrategy(new JdbcIO.RetryStrategy(){
                            @Override
                            public boolean apply(SQLException sqlException) {
                                return false;
                            }
                        })

                );
        pipeline.run();
    }
}
