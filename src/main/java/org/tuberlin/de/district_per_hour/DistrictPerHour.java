package org.tuberlin.de.district_per_hour;

import com.google.gson.Gson;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.tuberlin.de.geodata.MapCoordToDistrict;
import org.tuberlin.de.read_data.Pickup;
import org.tuberlin.de.read_data.Taxidrive;

@SuppressWarnings("serial")
public class DistrictPerHour {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final ParameterTool params = ParameterTool.fromArgs(args);
//        final String inputFilepath = params.get("input", "data/bigger.csv");
//        final String districtsCsvFilepath = params.get("district", "data/geodata/ny_districts.csv");
//        final String dataWithDistrictsFilepath = params.get("inputwithdistrict", "data/testDataWithDistricts");

        final String inputFilepath = params.get("input", "hdfs:///TaxiData/sorted_data.csv");
        final String districtsCsvFilepath = params.get("district", "hdfs:///data/ny_districts.csv");
        final String dataWithDistrictsFilepath = params.get("inputwithdistrict", "hdfs:///data/sorted_data_with_districts");

        MapCoordToDistrict.main(new String[]{"--input", inputFilepath, "--district", districtsCsvFilepath, "--inputwithdistrict", dataWithDistrictsFilepath});

        DataSet<String> textInput = env.readTextFile(dataWithDistrictsFilepath);
        DataSet<Pickup> pickupDataset = textInput.flatMap(new FlatMapFunction<String, Pickup>() {

            @Override
            public void flatMap(String value, Collector<Pickup> collector) throws Exception {
                Taxidrive taxidrive = new Gson().fromJson(value, Taxidrive.class);
                if (taxidrive.getPickupNeighborhood() != null && taxidrive.getPickupBorough() != null) {
                    DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
                    DateTime dateTime = formatter.parseDateTime(taxidrive.getPickup_datetime());
                    Pickup pickup = new Pickup.Builder()
                            .setDate(dateTime.toString("yyyy-MM-dd"))
                            .setHour(dateTime.toString("HH"))
                            .setNeighborhood(taxidrive.getPickupNeighborhood())
                            .setBorough(taxidrive.getPickupBorough())
                            .build();
                    collector.collect(pickup);
                }
            }

        });

        DataSet<Pickup> reducedDataSet = pickupDataset.groupBy("borough", "date", "hour").reduce((t1, t2) -> {
            int sum = t1.getCount() + t2.getCount();
            return new Pickup.Builder()
                    .setDate(t1.getDate())
                    .setHour(t1.getHour())
                    .setBorough(t1.getBorough())
                    .setNeighborhood(t1.getNeighborhood())
                    .setCount(sum)
                    .build();
        });

        reducedDataSet.print();
    }
}
