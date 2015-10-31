package org.tuberlin.de.district_trips;

import com.vividsolutions.jts.geom.Coordinate;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.tuberlin.de.geodata.District;
import org.tuberlin.de.geodata.MapCoordToDistrict;
import org.tuberlin.de.read_data.Pickup;

import java.util.Collection;

@SuppressWarnings("serial")
public class DistrictTrips {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //load districts
        Collection<District> districtGeometries = MapCoordToDistrict.extractDistrictsFromShapefile("data/manhattan_districts.shp");

        DataSet<String> textInput = env.readTextFile("data/testData.csv");
        DataSet<Pickup> taxidriveDataSet = textInput.flatMap(new FlatMapFunction<String, Pickup>() {

            @Override
            public void flatMap(String value, Collector<Pickup> collector) throws Exception {

                String[] cells = value.split(",");
                String district = null;

                Float pickupLat = Float.parseFloat(cells[7]);
                Float pickupLng = Float.parseFloat(cells[6]);

                Float dropoffLat = Float.parseFloat(cells[9]);
                Float dropoffLng = Float.parseFloat(cells[8]);

                //System.out.print(pickupLat + ", " + pickupLng + " -> ");

                for(District feat : districtGeometries) {

                    // pickup location
                    if (MapCoordToDistrict.coordinateInRegion(feat.geometry, new Coordinate(pickupLat, pickupLng))) {
                        district = feat.district;
                        break;
                    }
                }

                for(District feat : districtGeometries) {
                    // dropoff location
                    if(MapCoordToDistrict.coordinateInRegion(feat.geometry, new Coordinate(dropoffLat, dropoffLng))){

                        break;
                    }
                }

                if (district != null) {
                    collector.collect(new Pickup.Builder()
                            .setDistrict(district)
                            .build());
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
