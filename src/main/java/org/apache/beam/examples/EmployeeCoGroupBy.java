package org.apache.beam.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

import java.io.Serializable;
import java.util.Iterator;

public class EmployeeCoGroupBy {

    private static class EmployeeData implements Serializable {
        String name;
        String rating;
        String dept;
        String date;

        public EmployeeData(String name, String rating, String dept, String date) {
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

    private static class EmployeeLocation implements Serializable{
        String phNo;
        String location;

        public EmployeeLocation(String phNo, String location) {
            this.phNo = phNo;
            this.location = location;
        }

        @Override
        public String toString() {
            return "EmployeeLocation{" +
                    "phNo='" + phNo + '\'' +
                    ", location='" + location + '\'' +
                    '}';
        }
    }

    static class EmployeeDataTransform extends DoFn<String, KV<String, EmployeeData>>{
        @ProcessElement
        public void processElement(@Element String row, OutputReceiver<KV<String, EmployeeData>> out){
            String[] columns = row.split(",");
            out.output(KV.of(columns[0], new EmployeeData(columns[1], columns[2], columns[3], columns[4])));
        }
    }

    static class EmployeeLocationTransform extends DoFn<String, KV<String, EmployeeLocation>>{
        @ProcessElement
        public void processElement(@Element String row, OutputReceiver<KV<String, EmployeeLocation>> out){
            String[] columns = row.split(",");
            out.output(KV.of(columns[0], new EmployeeLocation(columns[1], columns[2])));
        }
    }
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<KV<String,EmployeeData>> dataFileCollection = pipeline.apply("read data file", TextIO.read().from("D:\\TouchMeNot\\word-count-beam\\src\\main\\resources\\data_file.txt"))
                .apply("Key Value Pair Transaform", ParDo.of(new EmployeeDataTransform()));
        PCollection<KV<String,EmployeeLocation>> locationFileCollection = pipeline.apply("read location file", TextIO.read().from("D:\\TouchMeNot\\word-count-beam\\src\\main\\resources\\location_file.txt"))
                .apply("Key Value Pair Transaform", ParDo.of(new EmployeeLocationTransform()));

        final TupleTag<EmployeeData> employeeDataTupleTag = new TupleTag<>();
        final TupleTag<EmployeeLocation> employeeLocationTupleTag = new TupleTag<>();

        PCollection<KV<String, CoGbkResult>> result = KeyedPCollectionTuple.of(employeeDataTupleTag,dataFileCollection)
                .and(employeeLocationTupleTag, locationFileCollection)
                .apply(CoGroupByKey.create());

        result.apply("Display Records", ParDo.of(new DoFn<KV<String, CoGbkResult>, String>(){
            @ProcessElement
            public void processElement(ProcessContext c){
                KV<String, CoGbkResult> e = c.element();
                Iterable<EmployeeData> employeeData = e.getValue().getAll(employeeDataTupleTag);
                Iterable<EmployeeLocation> employeeLocation = e.getValue().getAll(employeeLocationTupleTag);
                String formattedResult =
                        EmployeeCoGroupBy.formatCoGbkResults(e.getKey(), employeeData, employeeLocation);
                c.output(formattedResult);
            }
        }))
                .apply("write to file", TextIO.write().to("EmployeeGroupBy").withNumShards(1));

        pipeline.run();


    }

    private static String formatCoGbkResults(String key, Iterable<EmployeeData> employeeData, Iterable<EmployeeLocation> employeeLocation) {
        String result = key + ", [";
        Iterator<EmployeeData> dataIterator = employeeData.iterator();
        while(dataIterator.hasNext()){
            result += dataIterator.next().toString();
        }
        result += "], [";
        Iterator<EmployeeLocation> locationIterator = employeeLocation.iterator();
        while(locationIterator.hasNext()){
            result += locationIterator.next().toString();
        }
        result += "]";
        return result;

    }
}
