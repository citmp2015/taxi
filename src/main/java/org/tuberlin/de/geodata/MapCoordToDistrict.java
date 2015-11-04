package org.tuberlin.de.geodata;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.flink.api.common.functions.MapFunction;
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
 * maps longitude, latitude coordinates from taxi trips to the appropriate district of manhattan.
 *
 */

public class MapCoordToDistrict {

	public static void main(String[] args) throws Exception {
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// load taxi data from csv-file
		DataSet<Taxidrive> taxidrives = env.readCsvFile("data/testData.csv")
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

		//DataSet<District> districtGeometries = env.fromCollection(col); //env.readCsvFile("data/districtsAsText").pojoType(District.class, "district","geometry");
		DataSet<String> districtGeometriesAsText = env.readTextFile("data/districtsAsText");

		DataSet<District> districtGeometries = districtGeometriesAsText.map(new MapFunction<String, District>() {
			@Override
			public District map(String s) throws Exception {
				String sep[] = s.split(",");
				District d = new District();
				d.district = sep[0];

				int countOfCoords = (sep.length - 1) / 2;
				org.tuberlin.de.geodata.Coordinate[] geometry = new org.tuberlin.de.geodata.Coordinate[countOfCoords];
				int geometryIndex = 0;

				for (int i = 1; i < sep.length; i += 2) {
					org.tuberlin.de.geodata.Coordinate c = new org.tuberlin.de.geodata.Coordinate();
					c.x = Double.parseDouble(sep[i]);
					c.y = Double.parseDouble(sep[i + 1]);
					geometry[geometryIndex] = c;
					geometryIndex += 1;
				}
				d.geometry = geometry;
				System.out.println(d.toString());
				return d;
			}
		});

		//env.fromCollection(col);

		//districtGeometries.writeAsText("data/districtsAsText");
		//districtGeometries.print();
		// map pickup/dropoff-coordinates to districts
		// district dataset is used as broadcast variable (https://cwiki.apache.org/confluence/display/FLINK/Variables+Closures+vs.+Broadcast+Variables)
		taxidrives = taxidrives.map(new DistrictMapper())
				.withBroadcastSet(districtGeometries, "districtGeometries");

		//taxidrives.writeAsText("data/testDataWithDistricts");
		taxidrives.print();
		// execute program
		//env.execute("Flink Java API Skeleton");
	}


	/**
	 *
	 * @param path to shapefile (extension .shp)
	 * @return a collection of districts containing geometries and district names
	 */

	public static Collection<District> extractDistrictsFromShapefile(String path) {
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
				d.geometry = convertGeometry(((Geometry) feature.getDefaultGeometry()).getCoordinates());
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

	private static org.tuberlin.de.geodata.Coordinate[] convertGeometry(Coordinate[] coordinates) {
		org.tuberlin.de.geodata.Coordinate[] converted = new org.tuberlin.de.geodata.Coordinate[coordinates.length];

		for (int i = 0; i < coordinates.length; i++) {
			Coordinate c = coordinates[i];
			org.tuberlin.de.geodata.Coordinate conv = new org.tuberlin.de.geodata.Coordinate();
			conv.x = c.x;
			conv.y = c.y;
			converted[i] = conv;
		}

		return converted;
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


	public static boolean coordinateInRegion(org.tuberlin.de.geodata.Coordinate[] region, Coordinate coord) {
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


