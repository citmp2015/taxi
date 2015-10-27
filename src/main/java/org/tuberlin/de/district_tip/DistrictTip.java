package org.tuberlin.de.district_tip;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple3;

public class DistrictTip {

	public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<String> data = env.readTextFile("data/tip.txt");

		DataSet<Tuple3<String, Double, Integer>> trips = data
				.map(new MapFunction<String, Tuple3<String, Double, Integer>>() {
					public Tuple3<String, Double, Integer> map(String value) {
						String[] split = value.split(",");
						String district = split[17];
						Double tip = Double.parseDouble(split[14]);
						
						return new Tuple3<String, Double, Integer>(district, tip, 1);
					}
				});
		
		DataSet<Tuple3<String, Double, Integer>> aggregatedData = 
				trips.groupBy(0)
				.aggregate(Aggregations.SUM, 1)
				.and(Aggregations.SUM, 2);

		//fares.print();
		//aggregatedData.print();

	}

}
