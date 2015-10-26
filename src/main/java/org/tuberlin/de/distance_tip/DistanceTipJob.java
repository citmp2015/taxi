package org.tuberlin.de.distance_tip;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.tuberlin.de.read_data.Job;
import org.tuberlin.de.read_data.Taxidrive;


public class DistanceTipJob {

    public static class TipAndDistance {
        double tip;
        double distance;
        int roundedDistance; // rounded down, used as groupkey
        int tripAmount;

        public TipAndDistance(double tip, double distance) {
            this(tip, distance, 1, (int) distance);
        }

        // Only for Aggregation
        public TipAndDistance(double tip, double distance, int tripAmount, int groupKey) {
            this.tip = tip;
            this.distance = distance;
            this.tripAmount = tripAmount;
            roundedDistance = groupKey;
        }

        @Override
        public String toString() {
            // GroupKey, average tip per mile, sum of tips, sum of distance, amount of trips, avg tip per trip (everything per group)
            return roundedDistance + ", " + (tip/distance) + ", " + tip + ", " + distance +  ", " + tripAmount + ", " + (tip/tripAmount);
        }
    }

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> textInput = env.readTextFile("data/sorted_data.csv");
        DataSet<Taxidrive> taxidriveDataSet = textInput.flatMap(new Job.TaxidriveReader());

        // What kind of data do I want to output?
        // Remove invalid trips -> filter trips with distance 0

        // Multiple possible diagrams:
        //  1. amount of trips for distance, (ex: amount of trips that had a distance between 2 and 3 miles)
        //  2.
        //      x axis -> trip distance integer, from 0 to max distance
        //      y axis -> average tip/distance -> double
        // ...
        //

        DataSet<TipAndDistance> filteredAndConverted = taxidriveDataSet.filter(taxidrive -> taxidrive.getTrip_distance() > 0d).flatMap(new FlatMapFunction<Taxidrive, TipAndDistance>() {
            @Override
            public void flatMap(Taxidrive taxidrive, Collector<TipAndDistance> collector) throws Exception {
                if (taxidrive.getTrip_distance() > 0d)
                    collector.collect(new TipAndDistance(taxidrive.getTip_amount(), taxidrive.getTrip_distance()));
            }
        });

        DataSet<TipAndDistance> groupedReducedData = filteredAndConverted.groupBy(tipAndDistance -> tipAndDistance.roundedDistance)
                .reduce((t1, t2) -> new TipAndDistance(t1.tip + t2.tip, t1.distance + t2.distance, t1.tripAmount + t2.tripAmount, t1.roundedDistance));
        groupedReducedData.writeAsText("result/distance_vs_tip.result");
        env.execute("Distance vs Tip");
    }
}
