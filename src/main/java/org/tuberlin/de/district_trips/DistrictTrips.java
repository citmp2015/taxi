package org.tuberlin.de.district_trips;

import com.google.gson.Gson;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;
import org.tuberlin.de.geodata.MapCoordToDistrict;
import org.tuberlin.de.read_data.Pickup;
import org.tuberlin.de.read_data.Taxidrive;

@SuppressWarnings("serial")
public class DistrictTrips {

    private static boolean isLocal = false;

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final ParameterTool params = ParameterTool.fromArgs(args);

        String inputFilepath = params.get("input", "hdfs:///TaxiData/sorted_data.csv");
        String districtsCsvFilepath = params.get("district", "hdfs:///data/ny_districts.csv");
        String neighborhoodResultFilepath = params.get("neighborhoodresult", " hdfs://asok05.cit.tu-berlin.de:54310/results/district_tips_neighborhoods.txt");
        String boroughResultFilepath = params.get("boroughresult", " hdfs://asok05.cit.tu-berlin.de:54310/results/district_tips_boroughs.txt");
        if (isLocal) {
            inputFilepath = "data/sorted_data.csv";
            districtsCsvFilepath = "data/geodata/ny_districts.csv";
            neighborhoodResultFilepath = "data/results/district_tips_neighborhoods.txt";
            boroughResultFilepath = "data/results/district_tips_boroughs.txt";
        }

        DataSet<Taxidrive> taxidrives = MapCoordToDistrict.readData(env, inputFilepath, districtsCsvFilepath);
        DataSet<Pickup> pickupDataset = taxidrives.flatMap(new FlatMapFunction<Taxidrive, Pickup>() {

            @Override
            public void flatMap(Taxidrive taxidrive, Collector<Pickup> collector) throws Exception {
                if (taxidrive.getPickupNeighborhood() != null && !taxidrive.getPickupNeighborhood().equals("") && taxidrive.getPickupBorough() != null && !taxidrive.getPickupBorough().equals("")) {
                    Pickup pickup = new Pickup.Builder()
                            .setNeighborhood(taxidrive.getPickupNeighborhood())
                            .setBorough(taxidrive.getPickupBorough())
                            .build();
                    collector.collect(pickup);
                }
            }

        });


        /*DataSet<Pickup> dsGroupedByNeighborhood = pickupDataset.groupBy("neighborhood").reduce((t1, t2) -> {
            int sum = t1.getCount() + t2.getCount();
            return new Pickup.Builder()
                    .setNeighborhood(t1.getNeighborhood())
                    .setBorough(t1.getBorough())
                    .setCount(sum)
                    .build();
        });
        dsGroupedByNeighborhood.writeAsText(neighborhoodResultFilepath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        dsGroupedByNeighborhood.print();*/

        DataSet<Pickup> dsGroupedByBorough = pickupDataset.groupBy("borough").reduce((t1, t2) -> {
            int sum = t1.getCount() + t2.getCount();
            return new Pickup.Builder()
                    .setNeighborhood(t1.getNeighborhood())
                    .setBorough(t1.getBorough())
                    .setCount(sum)
                    .build();
        });
        dsGroupedByBorough.writeAsText(boroughResultFilepath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        dsGroupedByBorough.print();

    }
}
