package org.tuberlin.de.district_tip;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.tuberlin.de.geodata.MapCoordToDistrict;
import org.tuberlin.de.read_data.Taxidrive;

public class DistrictTip {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		String inputPath = "data/testData.csv";
		String outputPath = "result/district_tip_result.csv";
		String districtsPath = "data/geodata/ny_districts.csv";

		if (args.length > 0) {
			inputPath = args[0];
			outputPath = args[1];
			districtsPath = args[2];
		}

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Taxidrive> taxidrives = MapCoordToDistrict.readData(env, inputPath, districtsPath);

		DataSet<TaxiTip> tripData = taxidrives.flatMap(new FlatMapFunction<Taxidrive, TaxiTip>() {

			@Override
			public void flatMap(Taxidrive drive, Collector<TaxiTip> collector) throws Exception {
				TaxiTip trip = new TaxiTip();

				if ("".equals(drive.getPickupNeighborhood())) {
					return;
				}

				trip.setDistrict(drive.getPickupNeighborhood());
				trip.setTip(drive.getTip_amount());
				collector.collect(trip);
			}

		});

		DataSet<TaxiTip> reducedData = tripData.groupBy("district").reduce((t1, t2) -> {
			int sum = t1.getCount() + t2.getCount();
			double sumValue = t1.getTip() + t2.getTip();
			double average = sumValue / sum;
			double roundedAverage = Math.round(average * 100.0) / 100.0;
			TaxiTip trip = new TaxiTip();
			trip.setDistrict(t1.getDistrict());
			trip.setCount(sum);
			trip.setTip(sumValue);
			trip.setAverageTip(roundedAverage);
			return trip;
		});

		createRanking(reducedData).writeAsCsv(outputPath);
		env.execute("District Tip");

	}

	@SuppressWarnings("serial")
	private static DataSet<Tuple2<String, Double>> createRanking(DataSet<TaxiTip> data) throws Exception {
		DataSet<Tuple2<String, Double>> tupleData = data.map(new MapFunction<TaxiTip, Tuple2<String, Double>>() {

			@Override
			public Tuple2<String, Double> map(TaxiTip trip) throws Exception {
				return new Tuple2<String, Double>(trip.getDistrict(), trip.getAverageTip());
			}
		});

		return tupleData.sortPartition(1, Order.DESCENDING).setParallelism(1);
	}

}
