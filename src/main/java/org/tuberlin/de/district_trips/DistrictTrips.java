package org.tuberlin.de.district_trips;

import com.google.gson.Gson;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.tuberlin.de.geodata.MapCoordToDistrict;
import org.tuberlin.de.read_data.Pickup;
import org.tuberlin.de.read_data.Taxidrive;

@SuppressWarnings("serial")
public class DistrictTrips {
    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String inputFilepath = "data/testData.csv";
        final String dataWithDistrictsFilepath = "data/testDataWithDistricts";
        if (args.length > 0) {
            inputFilepath = args[0];
        }

        MapCoordToDistrict.main(new String[]{inputFilepath, dataWithDistrictsFilepath});

        DataSet<String> textInput = env.readTextFile(dataWithDistrictsFilepath);
        DataSet<Pickup> taxidriveDataSet = textInput.flatMap(new FlatMapFunction<String, Pickup>() {

            @Override
            public void flatMap(String value, Collector<Pickup> collector) throws Exception {
                Taxidrive taxidrive = new Gson().fromJson(value, Taxidrive.class);
                if (taxidrive.getPickupDistrict() != null) {
                    Pickup pickup = new Pickup.Builder()
                            .setDistrict(taxidrive.getPickupDistrict())
                            .build();
                    collector.collect(pickup);
                }
            }

        });

        DataSet<Pickup> reducedDataSet = taxidriveDataSet.groupBy("district").reduce((t1, t2) -> {
            int sum = t1.getCount() + t2.getCount();
            return new Pickup.Builder()
                    .setDistrict(t1.getDistrict())
                    .setCount(sum)
                    .build();
        });

        reducedDataSet.print();

    }
}
