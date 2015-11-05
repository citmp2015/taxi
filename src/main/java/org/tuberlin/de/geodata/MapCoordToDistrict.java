package org.tuberlin.de.geodata;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.tuberlin.de.read_data.Taxidrive;

import java.util.Collection;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * maps longitude, latitude coordinates from taxi trips to the appropriate neighborhood of manhattan.
 */

public class MapCoordToDistrict {

    public static void main(String[] args) throws Exception {
        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //ShapeToTextConverter.convertShape("data/manhattan_districts.shp", "data/districts");

        String taxiDatasetPath = args[0]; // local :"data/testData.csv"
        String districtsPath = args[1]; //local : "data/districts"
        // load taxi data from csv-file and map districts
        DataSet<Taxidrive> taxidrives = readData(env, taxiDatasetPath, districtsPath);

        //taxidrives.writeAsText("data/testDataWithDistricts");
        taxidrives.print();

        // execute program
        //env.execute("Flink Java API Skeleton");
    }

    public static DataSet<Taxidrive> readData(ExecutionEnvironment env, String taxiDatasetPath, String districtsPath) throws Exception {
        // set up the execution environment
        // load taxi data from csv-file
        DataSet<Taxidrive> taxidrives = env.readCsvFile(taxiDatasetPath)
                .pojoType(Taxidrive.class,
                        "taxiID",
                        "licenseID",
                        "pickup_datetime",
                        "dropoff_datetime",
                        "trip_time_in_secs",
                        "trip_distance",
                        "pickup_longitude",
                        "pickup_latitude",
                        "dropoff_longitude",
                        "dropoff_latitude",
                        "payment_type",
                        "fare_amount",
                        "surcharge",
                        "mta_tax",
                        "tip_amount",
                        "tolls_amount",
                        "total_amount");

        //load districts
        DataSet<String> districtGeometriesAsText = env.readTextFile(districtsPath);

        DataSet<District> districtGeometries = districtGeometriesAsText.map(new MapFunction<String, District>() {
            @Override
            public District map(String s) throws Exception {
                String sep[] = s.split(";");
                if (sep.length % 2 != 0) {
                    throw new Exception("invalid geometry");
                }

                District d = new District();
                d.borough = sep[0];
                d.neighborhood = sep[1];

                int countOfCoords = (sep.length - 2) / 2;
                Coordinate[] geometry = new Coordinate[countOfCoords];
                int geometryIndex = 0;

                for (int i = 2; i < sep.length; i += 2) {
                    Coordinate c = new Coordinate();
                    c.x = Double.parseDouble(sep[i]);
                    c.y = Double.parseDouble(sep[i + 1]);
                    geometry[geometryIndex] = c;
                    geometryIndex += 1;
                }
                d.geometry = geometry;
                return d;
            }
        });
        // map pickup/dropoff-coordinates to districts
        // neighborhood dataset is used as broadcast variable (https://cwiki.apache.org/confluence/display/FLINK/Variables+Closures+vs.+Broadcast+Variables)
        taxidrives = taxidrives.map(new DistrictMapper())
                .withBroadcastSet(districtGeometries, "districtGeometries");

        return taxidrives;
    }


    public static boolean coordinateInRegion(Coordinate[] region, Coordinate coord) {
        // adapt the code from http://stackoverflow.com/questions/12083093/how-to-define-if-a-determinate-point-is-inside-a-region-lat-long
        int i, j;
        boolean isInside = false;
        //create an array of coordinates from the region boundary list
        int sides = region.length;
        for (i = 0, j = sides - 1; i < sides; j = i++) {
            //verifying if your coordinate is inside your region
            if (
                    (
                            (
                                    (region[i].x <= coord.x) && (coord.x < region[j].x)
                            ) || (
                                    (region[j].x) <= coord.x) && (coord.x < region[i].x)
                    )
                            &&
                            (coord.y < (region[j].y - region[i].y) * (coord.y - region[i].x) / (region[j].x - region[i].x) + region[i].y)
                    ) {
                isInside = !isInside;
            }
        }
        return isInside;
    }

    public static class DistrictMapper extends RichMapFunction<Taxidrive, Taxidrive> {

        private Collection<District> districtGeometries;

        @Override
        public void open(Configuration parameters) {
            this.districtGeometries = getRuntimeContext().getBroadcastVariable("districtGeometries");
        }


        @Override
        public Taxidrive map(Taxidrive taxidrive) throws Exception {

            for (District feat : districtGeometries) {
                Coordinate c = new Coordinate();
                c.x = taxidrive.dropoff_longitude;
                c.y = taxidrive.dropoff_latitude;
                // pickup location
                if (coordinateInRegion(feat.geometry, c)) {
                    taxidrive.setPickupNeighborhood(feat.neighborhood);
                    taxidrive.setPickupBorough(feat.borough);
                    break;
                }
            }

            for (District feat : districtGeometries) {
                // dropoff location
                Coordinate c = new Coordinate();
                c.x = taxidrive.dropoff_longitude;
                c.y = taxidrive.dropoff_latitude;

                if (coordinateInRegion(feat.geometry, c)) {
                    taxidrive.setDropoffNeighborhood(feat.neighborhood);
                    taxidrive.setDropoffBorough(feat.borough);
                    break;
                }
            }
            return taxidrive;
        }
    }
}


