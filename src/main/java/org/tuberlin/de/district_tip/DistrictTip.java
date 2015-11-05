package org.tuberlin.de.district_tip;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

public class DistrictTip {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<String> data = env.readTextFile("data/district_tip_testdata.txt");
		
		DataSet<TaxiTrip> tripData = data.flatMap(new FlatMapFunction<String, TaxiTrip>() {

			@Override
			public void flatMap(String value, Collector<TaxiTrip> collector) throws Exception {
				String[] split = value.split(",");
				TaxiTrip trip = new TaxiTrip();
				trip.setDistrict(split[17]);
				trip.setTip(Double.parseDouble(split[14]));
				collector.collect(trip);
			}
		});

		DataSet<TaxiTrip> reducedData = tripData.groupBy("district").reduce((t1, t2) -> {
			int sum = t1.getCount() + t2.getCount();
			double sumValue = t1.getTip() + t2.getTip();
			double average=sumValue / sum;
			double roundedAverage=Math.round(average*100.0)/100.0;
			TaxiTrip trip = new TaxiTrip();
			trip.setDistrict(t1.getDistrict());
			trip.setCount(sum);
			trip.setTip(sumValue);
			trip.setAverageTip(roundedAverage);
			return trip;
		});
		
		reducedData.print();

	}
	
}
