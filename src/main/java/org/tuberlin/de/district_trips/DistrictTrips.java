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
        String districtsCsvFilepath = "data/geodata/ny_districts.csv";
        final String dataWithDistrictsFilepath = "data/testDataWithDistricts";
        if (args.length > 0) {
            inputFilepath = args[0];
        }

        MapCoordToDistrict.main(new String[]{inputFilepath, districtsCsvFilepath});

        DataSet<String> textInput = env.readTextFile(dataWithDistrictsFilepath);
        DataSet<Pickup> pickupDataset = textInput.flatMap(new FlatMapFunction<String, Pickup>() {

            @Override
            public void flatMap(String value, Collector<Pickup> collector) throws Exception {
                Taxidrive taxidrive = new Gson().fromJson(value, Taxidrive.class);
                if (taxidrive.getPickupNeighborhood() != null && taxidrive.getPickupBorough() != null) {
                    Pickup pickup = new Pickup.Builder()
                            .setNeighborhood(taxidrive.getPickupNeighborhood())
                            .setBorough(taxidrive.getPickupBorough())
                            .build();
                    collector.collect(pickup);
                }
            }

        });


        DataSet<Pickup> dsGroupedByNeighborhood = pickupDataset.groupBy("neighborhood").reduce((t1, t2) -> {
            int sum = t1.getCount() + t2.getCount();
            return new Pickup.Builder()
                    .setNeighborhood(t1.getNeighborhood())
                    .setBorough(t1.getBorough())
                    .setCount(sum)
                    .build();
        });
        dsGroupedByNeighborhood.print();

        DataSet<Pickup> dsGroupedByBorough = pickupDataset.groupBy("borough").reduce((t1, t2) -> {
            int sum = t1.getCount() + t2.getCount();
            return new Pickup.Builder()
                    .setNeighborhood(t1.getNeighborhood())
                    .setBorough(t1.getBorough())
                    .setCount(sum)
                    .build();
        });
        dsGroupedByBorough.print();

    }
}
