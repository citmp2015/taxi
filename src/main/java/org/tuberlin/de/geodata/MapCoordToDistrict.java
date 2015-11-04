package org.tuberlin.de.geodata;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.tuberlin.de.read_data.Taxidrive;

import java.io.*;
import java.net.MalformedURLException;
import java.util.*;

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
 * Skeleton for a Flink Job.
 *
 * For a full example of a Flink Job, see the WordCountJob.java file in the
 * same package/directory or have a look at the website.
 *
 * You can also generate a .jar file that you can submit on your Flink
 * cluster.
 * Just type
 * 		mvn clean package
 * in the projects root directory.
 * You will find the jar in
 * 		target/flink-quickstart-0.1-SNAPSHOT-Sample.jar
 *
 */

public class MapCoordToDistrict {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Taxidrive> taxidriveDataSet = readData(env, "data/testData.csv");

		taxidriveDataSet.writeAsText("data/testDataWithDistricts");
		taxidriveDataSet.print();
		// execute program
		env.execute("Flink Java API Skeleton");

	}

	public static DataSet<Taxidrive> readData(ExecutionEnvironment env, String dataPath) throws Exception {
		// set up the execution environment

		// load taxi data from csv-file
		DataSet<Taxidrive> taxidrives = env.readCsvFile(dataPath)
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
		//Collection<District> col = extractDistrictsFromShapefile("data/manhattan_districts.shp");
		//DataSet<District> districtGeometries = env.fromCollection(col);

		// map pickup/dropoff-coordinates to districts
		// district dataset is used as broadcast variable (https://cwiki.apache.org/confluence/display/FLINK/Variables+Closures+vs.+Broadcast+Variables)
		//taxidrives = taxidrives.map(new DistrictMapper())
				//.withBroadcastSet(districtGeometries, "districtGeometries");


	  	return taxidrives;
	}


	/**
	 *
	 * @param path to shapefile (extension .shp)
	 * @return a collection of districts containing geometries and district names
	 */

	private static Collection<District> extractDistrictsFromShapefile(String path) {
		File file = new File(path);
		Map<String, Object> map = new HashMap<String, Object>();
		FeatureSource<SimpleFeatureType, SimpleFeature> source = null;
		DataStore dataStore = null;
		FeatureIterator<SimpleFeature> features = null;
		try {

			map.put("url", file.toURI().toURL());

			dataStore = DataStoreFinder.getDataStore(map);

			String typeName = dataStore.getTypeNames()[0];

			source = dataStore
					.getFeatureSource(typeName);


			FeatureCollection<SimpleFeatureType, SimpleFeature> collection = null;
			collection = source.getFeatures();



		HashSet<District> feats = new HashSet<District>();
		features = collection.features();
			while (features.hasNext()) {
				SimpleFeature feature = features.next();
				District d =  new District();
				d.district = (String) feature.getAttribute("label");
				d.geometry = ((Geometry) feature.getDefaultGeometry()).getCoordinates();
				feats.add(d);
			}
			return feats;

		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			features.close();
			dataStore.dispose();
		}


		return null;
	}



  	public static class DistrictMapper extends RichMapFunction<Taxidrive, Taxidrive> {

		private Collection<District> districtGeometries;

		@Override
		public void open(Configuration parameters) {
			this.districtGeometries = getRuntimeContext().getBroadcastVariable("districtGeometries");
		}


		@Override
		public Taxidrive map(Taxidrive taxidrive) throws Exception {

			for(District feat : districtGeometries) {

				// pickup location
				if (coordinateInRegion(feat.geometry, new Coordinate(taxidrive.dropoff_longitude, taxidrive.dropoff_latitude))) {
					taxidrive.setPickupDistrict(feat.district);
					break;
				}
			}

			for(District feat : districtGeometries) {
				// dropoff location
				if(coordinateInRegion(feat.geometry, new Coordinate(taxidrive.dropoff_longitude, taxidrive.dropoff_latitude))){
					taxidrive.setDropoffDistrict(feat.district);
					break;
				}
			}
			return taxidrive;
		}
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
}


