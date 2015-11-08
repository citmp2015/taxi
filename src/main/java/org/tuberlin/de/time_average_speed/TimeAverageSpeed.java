package org.tuberlin.de.time_average_speed;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.tuberlin.de.geodata.MapCoordToDistrict;
import org.tuberlin.de.read_data.Taxidrive;

public class TimeAverageSpeed {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		String inputPath = "data/testData.csv";
		String districtsPath = "data/geodata/ny_districts.csv";

		if (args.length > 0) {
			inputPath = args[0];
		}

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Taxidrive> taxidrives = MapCoordToDistrict.readData(env, inputPath, districtsPath);

		DataSet<Taxidrive> filteredTripData = taxidrives.filter(new FilterFunction<Taxidrive>() {

			@Override
			public boolean filter(Taxidrive trip) throws Exception {
				return trip.getTrip_distance() > 0.0 && trip.getTrip_time_in_secs() > 0;
			}
		});

		DataSet<TaxiTrip> tripData = filteredTripData.flatMap(new FlatMapFunction<Taxidrive, TaxiTrip>() {

			@Override
			public void flatMap(Taxidrive taxidrive, Collector<TaxiTrip> collector) throws Exception {
				DateFormat parseFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
				DateFormat outputFormat = new SimpleDateFormat("HH");

				String pickupTime = outputFormat.format(parseFormat.parse(taxidrive.getPickup_datetime()))
						.concat(":00");
				double duration = (taxidrive.getTrip_time_in_secs() / 60.0) / 60.0;
				double distance = taxidrive.getTrip_distance();
				double speed = distance / duration;
				double roundedSpeed = Math.round(speed * 100.0) / 100.0;

				TaxiTrip trip = new TaxiTrip();
				trip.setTime(pickupTime);
				trip.setSpeed(roundedSpeed);
				collector.collect(trip);
			}
		});

		DataSet<TaxiTrip> reducedData = tripData.groupBy("time").reduce((t1, t2) -> {
			int sum = t1.getCount() + t2.getCount();
			double sumSpeed = t1.getSpeed() + t2.getSpeed();
			double roundedSumSpeed = Math.round(sumSpeed * 100.0) / 100.0;
			double average = sumSpeed / sum;
			double roundedAverage = Math.round(average * 100.0) / 100.0;
			TaxiTrip trip = new TaxiTrip();
			trip.setTime(t1.getTime());
			trip.setCount(sum);
			trip.setSpeed(roundedSumSpeed);
			trip.setAverageSpeed(roundedAverage);
			return trip;
		});

		createRanking(reducedData);
		env.execute("Time AverageSpeed");

	}

	@SuppressWarnings("serial")
	private static void createRanking(DataSet<TaxiTrip> data) throws Exception {
		DataSet<Tuple2<String, Double>> tupleData = data.map(new MapFunction<TaxiTrip, Tuple2<String, Double>>() {

			@Override
			public Tuple2<String, Double> map(TaxiTrip trip) throws Exception {
				return new Tuple2<String, Double>(trip.getTime(), trip.getAverageSpeed());
			}
		});

		tupleData.sortPartition(1, Order.DESCENDING).setParallelism(1)
				.writeAsCsv("result/time_averagespeed_result.csv");
	}

}
