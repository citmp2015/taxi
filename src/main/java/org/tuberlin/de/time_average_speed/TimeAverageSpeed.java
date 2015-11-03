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

public class TimeAverageSpeed {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<String> data = env.readTextFile("data/testData.csv");

		DataSet<TaxiTrip> tripData = data.flatMap(new FlatMapFunction<String, TaxiTrip>() {

			@Override
			public void flatMap(String value, Collector<TaxiTrip> collector) throws Exception {
				String[] split = value.split(",");

				DateFormat parseFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
				DateFormat outputFormat = new SimpleDateFormat("HH:mm");

				String pickupTime = outputFormat.format(parseFormat.parse(split[2]));
				double duration = (Double.parseDouble(split[4]) / 60) / 60;
				double distance = Double.parseDouble(split[5]);
				double speed = distance / duration;
				double roundedSpeed = Math.round(speed * 100.0) / 100.0;

				TaxiTrip trip = new TaxiTrip();
				trip.setTime(pickupTime);
				trip.setSpeed(roundedSpeed);
				collector.collect(trip);
			}
		});

		DataSet<TaxiTrip> filteredTripData = tripData.filter(new FilterFunction<TaxiTrip>() {

			@Override
			public boolean filter(TaxiTrip trip) throws Exception {
				return !Double.isNaN(trip.getSpeed());
			}
		});

		DataSet<TaxiTrip> reducedData = filteredTripData.groupBy("time").reduce((t1, t2) -> {
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

		showRanking(reducedData);
		//reducedData.print();

	}
	
	@SuppressWarnings("serial")
	private static void showRanking(DataSet<TaxiTrip> data) throws Exception{
		DataSet<Tuple2<String, Double>> tupleData=data.map(new MapFunction<TaxiTrip, Tuple2<String, Double>>() {

			@Override
			public Tuple2<String, Double> map(TaxiTrip trip) throws Exception {
				return new Tuple2<String, Double>(trip.getTime(),trip.getAverageSpeed());
			}
			});

		tupleData.sortPartition(1, Order.DESCENDING).first(10).print();
	}

}
