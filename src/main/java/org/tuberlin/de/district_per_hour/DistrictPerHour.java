package org.tuberlin.de.district_per_hour;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.tuberlin.de.read_data.Pickup;

@SuppressWarnings("serial")
public class DistrictPerHour {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> textInput = env.readTextFile(args[0]);
        DataSet<Pickup> taxidriveDataSet = textInput.flatMap(new FlatMapFunction<String, Pickup>() {

            @Override
            public void flatMap(String value, Collector<Pickup> collector) throws Exception {
                String[] splittedText = value.split(",");

                DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
                DateTime dateTime = formatter.parseDateTime(splittedText[2]);
                Pickup pickup = new Pickup.Builder()
                        .setDate(dateTime.toString("yyyy-MM-dd"))
                        .setHour(dateTime.toString("HH"))
                        .setDistrict("NYC")
                        .build();

                collector.collect(pickup);
            }
        });

        DataSet<Pickup> reducedDataSet = taxidriveDataSet.groupBy("date", "hour").reduce((t1, t2) -> {
                    int sum = t1.getCount() + t2.getCount();
                    return new Pickup.Builder()
                        .setDate(t1.getDate())
                        .setHour(t1.getHour())
                        .setDistrict(t1.getDistrict())
                        .setCount(sum)
                        .build();
                });

        reducedDataSet.print();
    }
}
