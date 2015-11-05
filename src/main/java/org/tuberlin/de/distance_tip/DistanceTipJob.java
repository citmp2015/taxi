package org.tuberlin.de.distance_tip;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.tuberlin.de.read_data.Job;
import org.tuberlin.de.read_data.Taxidrive;

public class DistanceTipJob {
    private boolean isLocal = false;

    public DistanceTipJob() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> textInput;
        if (isLocal) textInput = env.readTextFile("data/sorted_data.csv");
        else textInput = env.readTextFile("hdfs:///TaxiData/sorted_data.csv");
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

        DataSet<TipAndDistance> filteredAndConverted = taxidriveDataSet
                .filter(taxidrive -> taxidrive.getTrip_distance() > 0d)
                        // TODO why cant I use a lambda here?
                        // the following does not work:
                        //.flatMap((taxidrive, collector) -> collector.collect(new TipAndDistance(taxidrive.getTip_amount(), taxidrive.getTrip_distance())))
                .flatMap(new FlatMapFunction<Taxidrive, TipAndDistance>() {
                    @Override
                    public void flatMap(Taxidrive taxidrive, Collector<TipAndDistance> collector) throws Exception {
                        collector.collect(new TipAndDistance(taxidrive.getTip_amount(), taxidrive.getTrip_distance()));
                    }
                });

        DataSet<TipAndDistance> groupedReducedData = filteredAndConverted
                .groupBy(tipAndDistance -> tipAndDistance.roundedDistance)
                .reduce((t1, t2) -> new TipAndDistance(t1.tip + t2.tip, t1.distance + t2.distance, t1.tripAmount + t2.tripAmount, t1.roundedDistance));

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ss");
        String currentTime = LocalDateTime.now().format(formatter);

        String resultFile = "results/distance_vs_tip" + currentTime + ".csv";
        if (!isLocal) resultFile = "hdfs:///" + resultFile;

        groupedReducedData.writeAsText(resultFile, FileSystem.WriteMode.OVERWRITE).setParallelism(1); // (writes to single file, while only reducing parallelism of the io task)

        env.execute("Distance vs Tip");
    }

    public static void main(String[] args) throws Exception {
        new DistanceTipJob();
    }
}
