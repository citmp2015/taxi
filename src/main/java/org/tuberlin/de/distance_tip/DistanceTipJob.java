package org.tuberlin.de.distance_tip;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.tuberlin.de.read_data.Job;
import org.tuberlin.de.read_data.Taxidrive;

import java.util.DoubleSummaryStatistics;


public class DistanceTipJob {

    public class tip_distance extends Tuple3<Double, Double, Double> {
        public Double getTip() { return this.f0; }
        public Double getDistance() { return this.f1; }
        public Double getTipPerMile() { return this.f2; }
    }

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> textInput = env.readTextFile("data/sorted_data.csv");
        DataSet<Taxidrive> taxidriveDataSet = textInput.flatMap(new Job.TaxidriveReader());

        DataSet tmp = taxidriveDataSet.flatMap(new FlatMapFunction<Taxidrive, Tuple3<Double, Double, Double>>() {
            @Override
            public void flatMap(Taxidrive taxidrive, Collector<Tuple3<Double, Double, Double>> collector) throws Exception {
                if (taxidrive.getTrip_distance() > 0d)
                    collector.collect(new Tuple3<>(taxidrive.getTip_amount(), taxidrive.getTrip_distance(), taxidrive.getTip_amount()/taxidrive.getTrip_distance()));
            }
        });
        tmp.writeAsText("data/distance_vs_tip.result");
        env.execute("Distance vs Tip");
    }
}
