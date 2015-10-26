package org.tuberlin.de.best_taxidriver;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import org.apache.flink.api.java.tuple.Tuple11;
import org.tuberlin.de.read_data.Taxidrive;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;


/**
 * Created by gerrit on 23.10.15.
 */
public final class BestTaxidriver {

    public static void main(String[] args) throws Exception {
        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // load taxi data from csv-file
        DataSet<Taxidrive> taxidrives = env.readCsvFile("data/sorted_data.csv")
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

        DataSet<DriverStat> stats  = createDriverHighscores(taxidrives);

        DataSet<Tuple11<String, Integer, Double, Double, Double, Double, Integer, Double, Double, Double, Double>> statsAsTuple =
                stats.map(new MapFunction<DriverStat, Tuple11<String, Integer, Double, Double, Double, Double, Integer, Double, Double, Double, Double>>() {
                    @Override
                    public Tuple11<String, Integer, Double, Double, Double, Double, Integer, Double, Double, Double, Double> map(DriverStat stat) throws Exception {
                        return stat.getStatAsTuple();
                    }
                });

        /*
        // sort by average total amount
        statsAsTuple.sortPartition(10, Order.DESCENDING).first(10).print();
        // sort by average trip distance
        statsAsTuple.sortPartition(8, Order.DESCENDING).first(10).print();
        // sort by average trip time (fastest)
        statsAsTuple.sortPartition(7, Order.DESCENDING).first(10).print();
        // driver with most trips
        */
        statsAsTuple.sortPartition(10, Order.DESCENDING)
                .setParallelism(1)
                .writeAsCsv("data/driverstats.csv");

        // execute program
        env.execute("Driver Highscore");

        //addHeader("data/driverstats.csv",DriverStat.getCSVHeader());
    }

    private static void addHeader(String filepath, String csvHeader) {
        RandomAccessFile f = null;
        try {
            f = new RandomAccessFile(new File(filepath), "rw");
            f.seek(0); // to the beginning
            f.write(csvHeader.getBytes());
            f.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    private static DataSet<DriverStat> createDriverHighscores(DataSet<Taxidrive> taxidrives) throws Exception {
        return taxidrives.filter(new FilterFunction<Taxidrive>() {
            @Override
            public boolean filter(Taxidrive taxidrive) throws Exception {
                if (taxidrive.getTrip_time_in_secs() > 0) {
                    return true;
                }
                return false;
            }
        }).map(new DriveToDriverStatMapper())
                .groupBy(stat -> stat.licenseID)

                .reduce(new StatReducer());
    }

    public static final class DriveToDriverStatMapper implements MapFunction<Taxidrive, DriverStat>{

        @Override
        public DriverStat map(Taxidrive taxidrive) throws Exception {
            DriverStat stat = new DriverStat();
            stat.countOfTrips = 1;
            stat.licenseID = taxidrive.licenseID;
            stat.trip_distanceSum = taxidrive.getTrip_distance();
            stat.tip_amountSum = taxidrive.getTip_amount();
            stat.total_amountSum = taxidrive.getTotal_amount();
            stat.tolls_amountSum = taxidrive.getTolls_amount();
            stat.tripTimeSum_inSecs = taxidrive.trip_time_in_secs;
            return stat;
        }
    }


    public static final class StatReducer implements ReduceFunction<DriverStat>{
        @Override
        public DriverStat reduce(DriverStat d1, DriverStat d2) throws Exception {
            d1.tip_amountSum += d2.tip_amountSum;
            d1.countOfTrips += d2.countOfTrips;
            d1.tolls_amountSum += d2.tolls_amountSum;
            d1.tip_amountSum += d2.tip_amountSum;
            d1.total_amountSum += d2.total_amountSum;
            d1.tripTimeSum_inSecs += d2.tripTimeSum_inSecs;
            return d1;
        }
    }


}
