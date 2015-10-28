package org.tuberlin.de.clock_average_speed;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;


public class TimeAverageSpeed {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<String> data = env.readTextFile("data/district_tip_data.txt");
		
		DataSet<Tuple3<String, Double, Integer>> trips = data
				.map(new MapFunction<String, Tuple3<String, Double, Integer>>() {
					public Tuple3<String, Double, Integer> map(String value) throws ParseException {
						String[] split = value.split(",");
						
						DateFormat parseFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
						DateFormat outputFormat = new SimpleDateFormat("HH:mm");
						String pickupTime=outputFormat.format(parseFormat.parse(split[2]));
						Double duration = (Double.parseDouble(split[4])/60)/60;
						Double distance= Double.parseDouble(split[5]);

						return new Tuple3<String, Double,Integer>(pickupTime,distance/duration,1 );
					}
				});
		
		DataSet<Tuple3<String, Double, Integer>> aggregatedData = trips.groupBy(0).aggregate(Aggregations.SUM, 1)
				.and(Aggregations.SUM, 2);

		DataSet<Tuple2<String, Double>> result = aggregatedData.flatMap(new AverageCalculation());

		result.sortPartition(1, Order.DESCENDING).print();
		

	}
	
	@SuppressWarnings("serial")
	public static class AverageCalculation
			implements FlatMapFunction<Tuple3<String, Double, Integer>, Tuple2<String, Double>> {

		@Override
		public void flatMap(Tuple3<String, Double, Integer> in, Collector<Tuple2<String, Double>> out)
				throws Exception {
			out.collect(new Tuple2<String, Double>(in.f0, in.f1 / in.f2));

		}

	}

}
