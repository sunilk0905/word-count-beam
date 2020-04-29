package org.apache.beam.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CrediCardDefaulters {

    private static final Logger LOG = LoggerFactory.getLogger(CrediCardDefaulters.class);

    static class DefaulterPoints extends DoFn<String, KV<String, Integer>>{
        @ProcessElement
        public void processElement(@Element String row, OutputReceiver<KV<String, Integer>> out){
            try {
                String[] columns = row.split(",");
//            Customer_id, First_name,Last_name,Relationship_no.,Card_type,Max_credit_limit,Total_Spent,Cash_withdrawn,Cleared_amount,Last_date
                String key = columns[0] + ", " + columns[1] + " " + columns[2];
                int points = 0;
                int totalSpent = Integer.parseInt(columns[6]);
                int cleardAmount = Integer.parseInt(columns[7]);
                int maxCredit = Integer.parseInt(columns[5]);

                if ((totalSpent != 0) && ((cleardAmount * 100) / totalSpent) < 70) {
                    points += 1;
                }
                if (maxCredit == totalSpent && totalSpent != cleardAmount) {
                    points += 1;
                }
                if (points == 2)
                    points += 1;

                out.output(KV.of(key, points));
            }catch(Exception e){
                LOG.error("Error at DafaulterPoints processing record : "+ row, e);
            }


        }
    }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("Read File", TextIO.read().from("D:\\TouchMeNot\\word-count-beam\\src\\main\\resources\\credit_card.txt"))
                .apply("evaluate Defaulter points", ParDo.of(new DefaulterPoints()))
                .apply("Combine By Key", Combine.perKey(Sum.ofIntegers()))
                .apply("Filter", ParDo.of(new DoFn<KV<String, Integer>, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c){
                        KV<String,Integer> element = c.element();
                        if(element.getValue() > 0){
                            c.output(element.getKey() + " : " + element.getValue() + " Defaulter Points");
                        }
                    }
                }))
                .apply("Write To File", TextIO.write().to("CreaditCardDefaulters").withNumShards(1));
        pipeline.run();
    }
}
